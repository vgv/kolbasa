package kolbasa.producer

data class SendMessage<Data, Meta : Any> @JvmOverloads constructor(
    val data: Data,
    val meta: Meta? = null,
    val sendOptions: SendOptions? = null
)
