package examples;

import kolbasa.consumer.datasource.DatabaseConsumer;
import kolbasa.queue.PredefinedDataTypes;
import kolbasa.queue.Queue;
import kolbasa.queue.QueueOptions;
import kolbasa.schema.SchemaHelpers;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Statement;

class SqlPutFunctionExample {

    public static void main(String[] args) throws SQLException {
        // Enable the per-queue SQL "put" function. With this flag on, createOrUpdateQueues generates a
        // typed function q_events_put(...) next to the queue table, so messages can be enqueued straight
        // from SQL — database triggers, stored functions, batch jobs — without a round-trip to the JVM.
        var queue = Queue.builder("events", PredefinedDataTypes.String)
            .options(QueueOptions.builder()
                .enableSqlPutFunction()
                .build())
            .build();

        // Valid datasource from DI, static factory etc.
        var dataSource = ExamplesDataSourceProvider.getDataSource();

        // Update PostgreSQL schema. Besides the queue table (q_events), this also creates the
        // q_events_put(data, delay, attempts, shard, producer, ignore_duplicates, ...) function.
        SchemaHelpers.createOrUpdateQueues(dataSource, queue);

        // -------------------------------------------------------------------------------------------
        // Enqueue straight from SQL. Here we wire an AFTER INSERT trigger on a business table so that
        // every new audit_log row is mirrored into the queue — no application code in the write path.
        try (var connection = dataSource.getConnection(); var st = connection.createStatement()) {
            // A business table whose new rows we want to mirror into the queue.
            st.execute("""
                create table if not exists audit_log(
                    id serial primary key,
                    message text
                )
                """);

            // The trigger function just calls q_events_put(...). `perform` because we ignore the returned id.
            // The body is single-quote delimited (it has no inner quotes), so no PostgreSQL dollar-quoting is needed.
            st.execute("""
                create or replace function audit_enqueue() returns trigger language plpgsql as '
                    begin
                        perform q_events_put(data => new.message);
                        return new;
                    end
                '
                """);

            // EXECUTE PROCEDURE (not FUNCTION) keeps this compatible down to PostgreSQL 10.
            st.execute("drop trigger if exists audit_trg on audit_log");
            st.execute("create trigger audit_trg after insert on audit_log for each row execute procedure audit_enqueue()");
        }

        // A plain INSERT into the business table now enqueues a queue message as a side effect.
        execute(dataSource, "insert into audit_log(message) values ('user #1 signed up')");
        execute(dataSource, "insert into audit_log(message) values ('user #2 signed up')");
        execute(dataSource, "insert into audit_log(message) values ('user #3 signed up')");

        // -------------------------------------------------------------------------------------------
        // Consume what the trigger enqueued — an ordinary kolbasa consumer, nothing special about it.
        var messages = new DatabaseConsumer(dataSource).receive(queue, 10);
        messages.forEach(message -> System.out.println("Consumed message enqueued from SQL: " + message.getData()));
    }

    private static void execute(DataSource dataSource, String sql) throws SQLException {
        try (var connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
