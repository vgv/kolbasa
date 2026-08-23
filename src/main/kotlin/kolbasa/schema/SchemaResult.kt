package kolbasa.schema

/**
 * What happened when a [Schema] was executed – which statements ran, and which ones failed.
 *
 * Returned by [SchemaHelpers.createOrUpdateQueues], [SchemaHelpers.renameQueues], [SchemaHelpers.deleteQueues] and
 * [SchemaHelpers.executeSchemaStatements].
 *
 * READ THE RESULT. A statement that fails does not stop the run and does not throw an exception: the other
 * statements still run, and the failure is recorded here. If you ignore the result, an application can start with a
 * queue table that is missing a column or an index, and find out much later. The short check is `failedStatements == 0`.
 *
 * A failure is not always a problem you have to solve. Two statements that add the same index from two application
 * instances starting at the same time is the common case: one of them wins, the other one fails, and the schema is
 * correct either way. What you should not do is ignore failures you have never looked at.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val result = SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers)
 *
 * if (result.failedStatements > 0) {
 *     result.failedTableStatements.forEach { println("${it.statement} -> ${it.error}") }
 *     result.failedIndexStatements.forEach { println("${it.statement} -> ${it.error}") }
 * }
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var result = SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers);
 *
 * if (result.getFailedStatements() > 0) {
 *     result.getFailedTableStatements().forEach(f -> System.out.println(f.getStatement() + " -> " + f.getError()));
 *     result.getFailedIndexStatements().forEach(f -> System.out.println(f.getStatement() + " -> " + f.getError()));
 * }
 * ```
 *
 * @see Schema
 * @see SchemaHelpers
 */
data class SchemaResult(
    /**
     * The statements that were executed – the same [Schema] that was passed to
     * [SchemaHelpers.executeSchemaStatements], or the one generated for you.
     */
    val schema: Schema,

    /**
     * How many statements failed, `0` when everything went through.
     *
     * This is always the size of [failedTableStatements] plus the size of [failedIndexStatements].
     */
    val failedStatements: Int,

    /**
     * The failed statements that create or change tables, each with the exception it raised.
     *
     * A failure here usually means the table is not in the shape the queue needs, so this is the list to look at
     * first.
     */
    val failedTableStatements: List<FailedStatement>,

    /**
     * The failed statements that create or drop indexes, each with the exception it raised.
     *
     * A missing index does not break correctness: queues keep working, but filtering and sorting on the meta field
     * behind that index become slow on a large table.
     */
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

/**
 * One SQL statement that failed, together with the exception PostgreSQL raised.
 *
 * @property statement the SQL text, exactly as it was sent to the database
 * @property error the exception raised by that statement
 */
data class FailedStatement(val statement: String, val error: Exception)
