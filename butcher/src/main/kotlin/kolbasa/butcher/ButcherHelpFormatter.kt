package kolbasa.butcher

import com.github.ajalt.clikt.core.Context
import com.github.ajalt.clikt.output.HelpFormatter
import com.github.ajalt.clikt.output.MordantHelpFormatter
import com.github.ajalt.mordant.rendering.Widget

/**
 * Clikt's help, with the one list of commands replaced by one list per [HelpSection]
 */
internal class ButcherHelpFormatter(context: Context) : MordantHelpFormatter(context) {

    override fun renderCommands(parameters: List<HelpFormatter.ParameterHelp>): List<RenderedSection<Widget>> {
        val commandsBySection = parameters
            .filterIsInstance<HelpFormatter.ParameterHelp.Subcommand>()
            .groupBy { HelpSection.fromHelpTags(it.tags) }

        // Sections in their declared order. A command that named no section goes last, under Clikt's own
        // title, so nothing can fall out of the help
        val sections: List<HelpSection?> = HelpSection.entries + listOf(null)

        return sections.mapNotNull { section ->
            val commands = commandsBySection[section] ?: return@mapNotNull null

            val rows = commands.map { command ->
                DefinitionRow(
                    term = styleSubcommandName(command.name),
                    description = renderParameterHelpText(command.help, command.tags)
                )
            }
            val title = section?.title ?: localization.commandsTitle()

            RenderedSection(styleSectionTitle(renderSectionTitle(title)), buildParameterList(rows))
        }
    }

    /**
     * Hides the section tag. Clikt prints every tag at the end of a command's help line. Without this,
     * each command would end with `(butcher-help-section: LOW_LEVEL)`.
     */
    override fun shouldShowTag(tag: String, value: String): Boolean =
        !HelpSection.isSectionTag(tag) && super.shouldShowTag(tag, value)
}
