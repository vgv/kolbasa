package kolbasa.queue.meta

enum class FieldOption {
    NONE,
    SEARCHABLE, // Rename to JUST_SEARCH
    UNIQUE_SEARCHABLE, // STRICT_UNIQUE_SEARCH
    RELAXED_UNIQUE_SEARCH // RELAXED_UNIQUE_SEARCH
}
