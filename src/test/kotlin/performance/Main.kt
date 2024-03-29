package performance

fun main() {
    when (val test = Env.test) {
        "producer" -> ProducerTest().run()
        "empty-consumer" -> EmptyConsumerTest().run()
        "producer-consumer" -> ProducerConsumerTest().run()
        else -> throw IllegalArgumentException("Unknown test: $test")
    }
}
