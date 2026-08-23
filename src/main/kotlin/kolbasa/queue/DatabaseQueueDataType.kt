package kolbasa.queue

sealed class DatabaseQueueDataType<Data> {

    data class Json<Data>(
        val serializer: (Data) -> String,
        val deserializer: (String) -> Data
    ) : DatabaseQueueDataType<Data>() {
        override val dbColumnType = "jsonb"
    }

    data class Binary<Data>(
        val serializer: (Data) -> ByteArray,
        val deserializer: (ByteArray) -> Data
    ) : DatabaseQueueDataType<Data>() {
        override val dbColumnType = "bytea"
    }

    data class Text<Data>(
        val serializer: (Data) -> String,
        val deserializer: (String) -> Data
    ) : DatabaseQueueDataType<Data>() {
        override val dbColumnType = "varchar"
    }

    data class Int<Data>(
        val serializer: (Data) -> kotlin.Int,
        val deserializer: (kotlin.Int) -> Data
    ) : DatabaseQueueDataType<Data>() {
        override val dbColumnType = "int"
    }

    data class Long<Data>(
        val serializer: (Data) -> kotlin.Long,
        val deserializer: (kotlin.Long) -> Data
    ) : DatabaseQueueDataType<Data>() {
        override val dbColumnType = "bigint"
    }

    internal abstract val dbColumnType: String

}

/**
 * The ready-made message types of Kolbasa – one of these is what most queues use.
 *
 * A queue has to know how to store your message in a database column and how to read it back. These four types
 * cover the simple cases, where the message already is a value PostgreSQL understands, so nothing is converted:
 * what you send is what is stored, and what is stored is what you receive.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val orders = Queue.of("orders", PredefinedDataTypes.String)
 * val events = Queue.of("events", PredefinedDataTypes.ByteArray)
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var orders = Queue.of("orders", PredefinedDataTypes.String);
 * var events = Queue.of("events", PredefinedDataTypes.ByteArray);
 * ```
 *
 * If your messages are objects, none of these fits: build a [DatabaseQueueDataType] of your own with the two
 * functions that turn your object into a string, or into bytes, and back. That is where a JSON/Protobuf library, or any
 * other serialization you already use, goes.
 *
 * The names here are the names of the types they carry, so they hide the usual `String`, `Int`, `Long` and
 * `ByteArray` inside this object. Written as `PredefinedDataTypes.String` from the outside, which is how they are
 * meant to be used, there is nothing to trip over.
 */
object PredefinedDataTypes {

    private fun <T> identity(x: T): T = x

    /**
     * Messages are byte arrays, stored in a `bytea` column.
     *
     * The most neutral choice: Kolbasa never looks inside the value, so anything you can serialize yourself fits,
     * and the database never tries to interpret it. Pick this one when the payload is binary, or when it is the
     * output of a serializer you already have.
     */
    @JvmField
    val ByteArray = DatabaseQueueDataType.Binary(::identity, ::identity)

    /**
     * Messages are strings, stored in a `varchar` column.
     *
     * Pick this one when the payload is text you built yourself – JSON, XML, CSV, an identifier. It is also the
     * easiest type to work with when you look at the queue table directly in `psql`.
     */
    @JvmField
    val String = DatabaseQueueDataType.Text(::identity, ::identity)

    /**
     * Messages are 32-bit integers, stored in an `int` column.
     *
     * Useful when the message is only a reference – the id of a row in your own tables – and everything else is
     * read from there when the message is processed.
     */
    @JvmField
    val Int = DatabaseQueueDataType.Int(::identity, ::identity)

    /**
     * Messages are 64-bit integers, stored in a `bigint` column.
     *
     * The same idea as [Int], for ids that do not fit into 32 bits.
     */
    @JvmField
    val Long = DatabaseQueueDataType.Long(::identity, ::identity)

}
