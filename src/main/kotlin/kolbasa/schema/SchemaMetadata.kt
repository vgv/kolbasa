package kolbasa.schema

data class Schema(
    val all: SchemaStatements,
    val required: SchemaStatements
)

data class SchemaStatements(
    /**
     * All "create table", "alter column", "set default" etc. statements
     */
    val tableStatements: List<String>,
    /**
     * All "create" and "drop index" statements
     */
    val indexStatements: List<String>
) {
    fun isEmpty() = tableStatements.isEmpty() && indexStatements.isEmpty()
}

internal data class Table(
    val name: String,
    val columns: Set<Column>,
    val indexes: Set<Index>,
) {
    fun findColumn(name: String): Column? = columns.find { it.name == name }
    fun findIndex(name: String): Index? = indexes.find { it.name == name }
}

internal data class Column(
    val name: String,
    val type: String,
    val nullable: Boolean,
    val defaultExpression: String?
)

internal data class Index(
    val name: String,
    val unique: Boolean,
    val columns: List<IndexColumn>,
    val filterCondition: String?,
    val invalid: Boolean
)

internal data class IndexColumn(
    val name: String,
    val asc: Boolean
)
