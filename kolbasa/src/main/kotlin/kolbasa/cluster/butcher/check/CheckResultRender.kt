package kolbasa.cluster.butcher.check

internal fun CheckResult.render(): String = buildString {
    val sections = listOfNotNull(
        shardBalance?.render(),
        schemaConsistency?.render(),
        orphanTables?.render(),
        migrationState?.render()
    )

    sections.forEachIndexed { index, section ->
        if (index > 0) appendLine()
        appendLine(SECTION_SEPARATOR)
        append(section)
    }
}

private const val SECTION_SEPARATOR = "================================================="
