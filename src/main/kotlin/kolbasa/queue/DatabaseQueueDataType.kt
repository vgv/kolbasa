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
        override val dbColumnType = "text"
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

object PredefinedDataTypes {

    private fun <T> identity(x: T): T = x

    @JvmStatic
    val ByteArray = DatabaseQueueDataType.Binary(::identity, ::identity)

    @JvmStatic
    val String = DatabaseQueueDataType.Text(::identity, ::identity)

    @JvmStatic
    val Int = DatabaseQueueDataType.Int(::identity, ::identity)

    @JvmStatic
    val Long = DatabaseQueueDataType.Long(::identity, ::identity)

}
