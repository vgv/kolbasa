package kolbasa.butcher

import kolbasa.schema.Column

/**
 * One column of a queue table, as a schema report describes it
 *
 * A reduced view of a column: enough to tell two nodes' tables apart and to say what differs, without
 * exposing how kolbasa models a schema internally. [type] is the column's type as kolbasa names it, for
 * example `VARCHAR` or `TIMESTAMPTZ`.
 */
data class ColumnShape(
    val name: String,
    val type: String,
    val nullable: Boolean,
    val defaultExpression: String?
)

internal fun Column.toShape(): ColumnShape = ColumnShape(
    name = name,
    type = type.name,
    nullable = nullable,
    defaultExpression = defaultExpression
)
