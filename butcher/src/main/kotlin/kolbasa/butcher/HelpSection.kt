package kolbasa.butcher

/**
 * The sections butcher's commands are grouped into in `--help`
 *
 * A command names its own section (see [ClusterCommand]), so a new one does not compile until it says where
 * it belongs.
 */
internal enum class HelpSection(val title: String) {

    DIAGNOSTICS("Diagnostics"),

    HIGH_LEVEL("High-level commands"),

    LOW_LEVEL("Low-level commands");

    companion object {
        /** The help tags that tell the formatter which section [section] belongs in */
        fun helpTags(section: HelpSection): Map<String, String> = mapOf(TAG to section.name)

        /** The section a command's help tags name, or null for one that names none */
        fun fromHelpTags(tags: Map<String, String>): HelpSection? =
            tags[TAG]?.let { name -> entries.find { it.name == name } }

        /** Whether [tag] is the one above: it steers the layout and is not for printing */
        fun isSectionTag(tag: String): Boolean = tag == TAG

        /** Clikt prints tags it does not recognise, so this must not collide with its own */
        private const val TAG = "butcher-help-section"
    }
}
