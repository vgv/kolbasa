package kolbasa.butcher.cli

import com.github.ajalt.clikt.core.Abort
import com.github.ajalt.clikt.core.CliktError
import com.github.ajalt.clikt.core.terminal
import com.github.ajalt.clikt.parameters.options.flag
import com.github.ajalt.clikt.parameters.options.help
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.mordant.rendering.TextColors
import com.github.ajalt.mordant.terminal.YesNoPrompt

/**
 * A command that changes the cluster, and therefore asks before it does
 *
 * The three migration steps are the only commands that write anything, and each of them is hard to undo in
 * a hurry, so none of them runs without a yes. [confirm] states what is about to happen in terms of the
 * cluster the operator just pointed at, not in terms of flags.
 */
internal abstract class MutatingClusterCommand(name: String) : ClusterCommand(name) {

    private val assumeYes by option("--yes", "-y")
        .flag()
        .help("Do not ask for confirmation before changing the cluster")

    /**
     * Describes what is about to happen and waits for a yes
     *
     * With no terminal to read an answer from – a pipe, a CI job – this fails instead of either hanging or
     * assuming consent: a tool that moves production data must not guess that nobody objected.
     */
    protected fun confirm(what: String) {
        if (assumeYes) {
            return
        }

        // Asked before prompting rather than after: with no terminal attached a prompt does not fail, it
        // silently takes its default, and "nobody objected" is not an answer worth acting on
        if (!terminal.terminalInfo.inputInteractive) {
            throw CliktError(
                "Cannot ask for confirmation: there is no terminal to read the answer from. " +
                    "Re-run with --yes if you are sure."
            )
        }

        val answer = YesNoPrompt("$what Continue?", terminal, default = false).ask() ?: false
        if (!answer) {
            echo(TextColors.yellow("Aborted, nothing was changed."))
            throw Abort()
        }
    }
}
