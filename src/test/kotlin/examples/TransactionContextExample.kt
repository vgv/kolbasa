package examples

import kolbasa.consumer.connection.ConnectionAwareDatabaseConsumer
import kolbasa.utils.JdbcHelpers.useConnection
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.producer.connection.ConnectionAwareDatabaseProducer
import kolbasa.queue.PredefinedDataTypes
import kolbasa.queue.Queue
import kolbasa.schema.SchemaHelpers
import java.sql.Statement

fun main() {
    // Define queue with name `test_queue` and varchar type as data storage in PostgreSQL table
    val queue = Queue.of("test_queue", PredefinedDataTypes.String)

    // Valid datasource from DI, static factory etc.
    val dataSource = ExamplesDataSourceProvider.getDataSource()

    // Update PostgreSQL schema
    // We need to create (or update) the queue table before the first use, since the table schema can be changed - for
    // example, new meta fields were added or other internal schema changes occurred. This is a convenient method that allows
    // you not to think about whether this queue has been used before or this is the first time and simply brings its state
    // in the database to the current one.
    // Of course, in a real application this should be done once at the start of the service, and not before each send/receive.
    // A good analogy is updating the business tables schema before the start of the service using migration or other
    // methods - this should be done once at the start of the service, and not before each SQL query from these tables.
    SchemaHelpers.createOrUpdateQueues(dataSource, queue)

    // First, let's create a business table to emulate a real application
    dataSource.useStatement { statement: Statement ->
        val sql = """
            create table customer(
                 id int not null primary key,
                 email text not null unique,
                 name text not null,
                 additional_info text
            )""".trimIndent()

        statement.execute(sql)
    }

    // -------------------------------------------------------------------------------------------
    // Create producer and send simple message using the same transaction with business query
    val producer = ConnectionAwareDatabaseProducer()
    dataSource.useConnection { connection ->
        // Execute business query - insert new customer to the business table
        val businessQuery = "insert into customer(id, email, name) values(1, 'john.doe@example.com', 'John Doe')"
        connection.useStatement { it.execute(businessQuery) }

        // Send a message to the queue in the same transaction with the business query above.
        // If this transaction fails, the new customer won't be inserted and the message will not be sent to the queue.
        // Please note that the ConnectionAware* methods take the connection as the first argument. This is different from
        // the regular Producer/Consumer
        producer.send(connection, queue, "User 'John Doe' was created")

        println("The user has been inserted and the message has been sent to the queue")
    }

    // -------------------------------------------------------------------------------------------
    // Create consumer and try to read message from the queue, process it and delete
    val consumer = ConnectionAwareDatabaseConsumer()
    dataSource.useConnection { connection ->
        // Receive the message from the queue using the same connection (and transaction) as the business query
        // Please note that the ConnectionAware* methods take the connection as the first argument. This is different from
        // the regular Producer/Consumer
        consumer.receive(connection, queue)?.let { message ->
            // the message from the queue was received, we can emulate the heavy calculation and update the business table
            val heavyData = "Large and heavy calculated data" // in the real application this will be more complex of course
            val businessQuery = "update customer set additional_info = '$heavyData' where id = 1"
            connection.useStatement { it.execute(businessQuery) }

            // If everything went well, remove the message from the queue.
            // If the transaction fails, the message will not be removed from the queue and will be processed again later.
            // Please note that the ConnectionAware* methods take the connection as the first argument. This is different from
            // the regular Producer/Consumer
            consumer.delete(connection, queue, message)

            println("The message has been received, the user has been updated and the message has been deleted from the queue")
        }
    }
}
