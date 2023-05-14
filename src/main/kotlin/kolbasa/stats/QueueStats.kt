package kolbasa.stats

import kolbasa.queue.Queue

internal class QueueStats(private val queue: Queue<*, *>) {

    private val sendCallsWindows: List<SlidingWindow>
    private val sendBytesWindows: List<SlidingWindow>
    private val receiveCallsWindows: List<SlidingWindow>
    private val receiveBytesWindows: List<SlidingWindow>

    private val realtimeWindows: List<SlidingWindow>
    private val allWindows: List<SlidingWindow>

    init {
        val tempSendCallsWindows = mutableListOf<SlidingWindow>()
        val tempSendBytesWindows = mutableListOf<SlidingWindow>()
        val tempReceiveCallsWindows = mutableListOf<SlidingWindow>()
        val tempReceiveBytesWindows = mutableListOf<SlidingWindow>()

        val tempRealtimeWindows = mutableListOf<SlidingWindow>()
        val tempAllWindows = mutableListOf<SlidingWindow>()

        Measure.values().forEach { measure ->
            val window = SlidingWindow(measure)
            when (measure.measureType) {
                MeasureType.SEND_CALLS -> tempSendCallsWindows += window
                MeasureType.SEND_BYTES -> tempSendBytesWindows += window
                MeasureType.RECEIVE_CALLS -> tempReceiveCallsWindows += window
                MeasureType.RECEIVE_BYTES -> tempReceiveBytesWindows += window
            }

            tempAllWindows += window
            if (measure.realtime) {
                tempRealtimeWindows += window
            }
        }

        sendCallsWindows = tempSendCallsWindows
        sendBytesWindows = tempSendBytesWindows
        receiveCallsWindows = tempReceiveCallsWindows
        receiveBytesWindows = tempReceiveBytesWindows

        realtimeWindows = tempRealtimeWindows
        allWindows = tempAllWindows
    }

    fun sendInc(calls: Long, bytes: Long) {
        sendCallsWindows.forEach { sendWindow ->
            sendWindow.inc(calls)
        }

        sendBytesWindows.forEach { sendWindow ->
            sendWindow.inc(bytes)
        }
    }

    fun receiveInc(calls: Long, bytes: Long) {
        receiveCallsWindows.forEach { window ->
            window.inc(calls)
        }

        receiveBytesWindows.forEach { window ->
            window.inc(bytes)
        }
    }

    fun dumpAndReset(onlyRealtimeDumps: Boolean): QueueDump {
        val windows = if (onlyRealtimeDumps)
            realtimeWindows
        else
            allWindows

        val measures = windows.map(SlidingWindow::dumpAndReset)
        return QueueDump(queue.name, measures)
    }

}
