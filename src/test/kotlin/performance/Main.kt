package performance

import java.util.concurrent.TimeUnit

fun main() {
    TimeUnit.MILLISECONDS.sleep(Env.Common.pauseBeforeStart.toMillis())

    when (val test = Env.test) {
        "only-producer" -> OnlyProducerTest().run()
        "only-consumer" -> OnlyConsumerTest().run()
        "producer-consumer" -> ProducerConsumerTest().run()
        else -> throw IllegalArgumentException("Unknown test: $test. Add test name as 'test' environment variable, like test=producer-consumer")
    }
}
