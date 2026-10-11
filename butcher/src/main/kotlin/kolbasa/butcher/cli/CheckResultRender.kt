package kolbasa.butcher.cli

import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.table.verticalLayout
import com.github.ajalt.mordant.widgets.HorizontalRule
import kolbasa.butcher.CheckResult

/**
 * Renders a [CheckResult] the way butcher shows it, one section per check that was asked for
 *
 * Presentation only: it belongs to the CLI, and nothing except console output may depend on it.
 */
internal fun CheckResult.render(): Widget = verticalLayout {
    val sections = listOfNotNull(
        shardBalance?.render(),
        schemaConsistency?.render(),
        orphanTables?.render(),
        migrationState?.render()
    )

    sections.forEachIndexed { index, section ->
        if (index > 0) {
            cell(HorizontalRule())
        }
        cell(section)
    }
}
