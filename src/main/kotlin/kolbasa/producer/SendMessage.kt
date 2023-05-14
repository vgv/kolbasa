package kolbasa.producer

data class SendMessage<V, M : Any> @JvmOverloads constructor(
    val data: V,
    val meta: M? = null,
    val sendOptions: SendOptions? = null
)
