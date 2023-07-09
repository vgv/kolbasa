package kolbasa.queue

sealed class QueueDataType<V> {

    data class Json<V>(
        val serializer: (V) -> String,
        val deserializer: (String) -> V
    ) : QueueDataType<V>() {
        override val dbColumnType = "jsonb"
    }

    data class Binary<V>(
        val serializer: (V) -> ByteArray,
        val deserializer: (ByteArray) -> V
    ) : QueueDataType<V>() {
        override val dbColumnType = "bytea"
    }

    data class Text<V>(
        val serializer: (V) -> String,
        val deserializer: (String) -> V
    ) : QueueDataType<V>() {
        override val dbColumnType = "text"
    }

    data class Int<V>(
        val serializer: (V) -> kotlin.Int,
        val deserializer: (kotlin.Int) -> V
    ) : QueueDataType<V>() {
        override val dbColumnType = "int"
    }

    data class Long<V>(
        val serializer: (V) -> kotlin.Long,
        val deserializer: (kotlin.Long) -> V
    ) : QueueDataType<V>() {
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
