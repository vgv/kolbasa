package kolbasa.producer

data class SendMessage<V, Meta : Any> @JvmOverloads constructor(
    val data: V,
    val meta: Meta? = null,
    val sendOptions: SendOptions? = null
)
