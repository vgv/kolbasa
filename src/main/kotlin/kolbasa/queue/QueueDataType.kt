package kolbasa.queue

sealed class QueueDataType<Data> {

    data class Json<Data>(
        val serializer: (Data) -> String,
        val deserializer: (String) -> Data
    ) : QueueDataType<Data>() {
        override val dbColumnType = "jsonb"
    }

    data class Binary<Data>(
        val serializer: (Data) -> ByteArray,
        val deserializer: (ByteArray) -> Data
    ) : QueueDataType<Data>() {
        override val dbColumnType = "bytea"
    }

    data class Text<Data>(
        val serializer: (Data) -> String,
        val deserializer: (String) -> Data
    ) : QueueDataType<Data>() {
        override val dbColumnType = "text"
    }

    data class Int<Data>(
        val serializer: (Data) -> kotlin.Int,
        val deserializer: (kotlin.Int) -> Data
    ) : QueueDataType<Data>() {
        override val dbColumnType = "int"
    }

    data class Long<Data>(
        val serializer: (Data) -> kotlin.Long,
        val deserializer: (kotlin.Long) -> Data
    ) : QueueDataType<Data>() {
        override val dbColumnType = "bigint"
    }

    internal abstract val dbColumnType: String

}

object PredefinedDataTypes {

    private fun <T> identity(x: T): T = x

    @JvmStatic
    val ByteArray = QueueDataType.Binary(::identity, ::identity)

    @JvmStatic
    val String = QueueDataType.Text(::identity, ::identity)

    @JvmStatic
    val Int = QueueDataType.Int(::identity, ::identity)

    @JvmStatic
    val Long = QueueDataType.Long(::identity, ::identity)

}
