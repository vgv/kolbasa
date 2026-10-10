package kolbasa.performance

import java.time.Duration
import java.util.concurrent.TimeUnit

fun main() {
    println("------------------------------------------------------------------")
    println("Starting test: ${Env.test} ${if (Env.Common.pauseBeforeStart > Duration.ZERO) "(pause ${Env.Common.pauseBeforeStart.toSeconds()}s)" else ""}")
    TimeUnit.MILLISECONDS.sleep(Env.Common.pauseBeforeStart.toMillis())
    println("Started...")

    when (val test = Env.test) {
        Env.OnlySend.TEST_NAME -> OnlySendTest().run()
        Env.OnlyReceive.TEST_NAME -> OnlyReceiveTest().run()

        Env.EmptyReceive.TEST_NAME -> EmptyReceiveTest().run()
        Env.EmptyDelete.TEST_NAME -> EmptyDeleteTest().run()

        Env.ProducerConsumer.TEST_NAME -> ProducerConsumerTest().run()

        else -> throw IllegalArgumentException("Unknown test: $test. Add test name as 'test' environment variable, like test=${Env.ProducerConsumer.TEST_NAME}")
    }

    println("Finish test: ${Env.test} ${if (Env.Common.pauseAfterFinish > Duration.ZERO) "(pause ${Env.Common.pauseAfterFinish.toSeconds()}s)" else ""}")
    println("------------------------------------------------------------------")
    TimeUnit.MILLISECONDS.sleep(Env.Common.pauseAfterFinish.toMillis())
    println("Finished.")
}
