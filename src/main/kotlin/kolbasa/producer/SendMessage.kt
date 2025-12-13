package kolbasa.producer

import kolbasa.queue.meta.MetaValues

data class SendMessage<Data> @JvmOverloads constructor(
    val data: Data,
    val meta: MetaValues = MetaValues.EMPTY,
    val messageOptions: MessageOptions = MessageOptions.NOT_SET
)
