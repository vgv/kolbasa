package performance

import java.util.concurrent.TimeUnit

fun main() {
    TimeUnit.MILLISECONDS.sleep(Env.Common.pauseBeforeStart.toMillis())
    println("------------------------------------------------------------------")
    println("Starting test: ${Env.test}")

    when (val test = Env.test) {
        Env.OnlySend.TEST_NAME -> OnlySendTest().run()
        Env.OnlyReceive.TEST_NAME -> OnlyReceiveTest().run()

        Env.EmptyReceive.TEST_NAME -> EmptyReceiveTest().run()
        Env.EmptyDelete.TEST_NAME -> EmptyDeleteTest().run()

        Env.ProducerConsumer.TEST_NAME -> ProducerConsumerTest().run()

        else -> throw IllegalArgumentException("Unknown test: $test. Add test name as 'test' environment variable, like test=${Env.ProducerConsumer.TEST_NAME}")
    }

    println("Finish test: ${Env.test}")
    println("------------------------------------------------------------------")
    TimeUnit.MILLISECONDS.sleep(Env.Common.pauseAfterFinish.toMillis())
}
