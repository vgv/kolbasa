package performance

fun main() {
    when (val test = Env.test) {
        "producer" -> ProducerTest().run()
        //"consumer" -> ConsumerTest().run()
        else -> throw IllegalArgumentException("Unknown test: $test")
    }
}
