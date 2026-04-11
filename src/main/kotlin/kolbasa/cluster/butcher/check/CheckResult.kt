package kolbasa.cluster.butcher.check

internal data class CheckResult(val results: List<Any>) {

    override fun toString(): String = buildString {
        results.forEachIndexed { index, result ->
            if (index > 0) appendLine()
            appendLine("=================================================")
            append(result)
        }
    }
}
