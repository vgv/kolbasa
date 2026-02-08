package kolbasa.schema

data class SchemaResult(
    val schema: Schema,
    val failedStatements: Int,
    val failedTableStatements: List<FailedStatement>,
    val failedIndexStatements: List<FailedStatement>
) {
    init {
        check(failedTableStatements.size + failedIndexStatements.size == failedStatements) {
            "Inconsistent schema result: failedStatements=$failedStatements, " +
                "failedTableStatements=${failedTableStatements.size}, " +
                "failedIndexStatements=${failedIndexStatements.size}"
        }
    }
}

data class FailedStatement(val statement: String, val error: Exception)
