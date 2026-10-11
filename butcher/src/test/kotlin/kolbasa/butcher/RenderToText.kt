package kolbasa.butcher

import com.github.ajalt.mordant.rendering.AnsiLevel
import com.github.ajalt.mordant.rendering.Widget
import com.github.ajalt.mordant.terminal.Terminal

/**
 * Renders a widget the way a plain terminal of a known width would
 *
 * The width is fixed rather than detected, and the colour turned off, so that a report can be asserted on
 * as text. Detection would make the result depend on where the suite runs - under Gradle there is no
 * terminal at all and mordant falls back to 79 columns - and a table that wraps differently would break
 * assertions that are about content. The real CLI detects the width, it does not pass one.
 */
internal fun Widget.renderToText(width: Int = 120): String =
    Terminal(ansiLevel = AnsiLevel.NONE, width = width).render(this)
