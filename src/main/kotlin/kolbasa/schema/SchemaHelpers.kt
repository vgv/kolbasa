package kolbasa.schema

import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
import kolbasa.queue.Queue
import kolbasa.queue.QueueRole
import kolbasa.schema.Schema.Companion.merge
import javax.sql.DataSource

/**
 * Creates, renames and deletes the database tables behind your queues.
 *
 * A [Queue][kolbasa.queue.Queue] object only describes a queue; the table it describes is created here. A queue
 * named `orders` lives in a table named `q_orders`, and this object is what creates that table, adds a column when
 * you add a meta field, and adds an index when you make a field searchable.
 *
 * The methods come in two families:
 * - `generate…` – build the statements and return them as a [Schema] per queue. Nothing is executed, the database is only read.
 * - [createOrUpdateQueues], [renameQueues], [deleteQueues] – build the statements and run them, and report the
 *   result as a [SchemaResult].
 *
 * Two rules are worth knowing before you call anything here:
 * - It is better to pass all your queues in one call. One call reads the existing schema once and then works on every queue, so one
 *   call with 10 000 queues is far faster than 10 000 calls with one queue each.
 * - Pass only [MAIN][kolbasa.queue.QueueRole.MAIN] queues. Companion queues
 *   ([DLQ][kolbasa.queue.Queue.deadLetterQueue]/[archive queue][kolbasa.queue.Queue.archiveQueue]) are created, renamed and
 *   deleted together with its [MAIN][kolbasa.queue.QueueRole.MAIN] queue. Passing one directly is rejected with an exception.
 *
 * ## Usage Example
 *
 * ```kotlin
 * val orders = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(ACCOUNT_ID))
 * val customers = Queue.of("customers", PredefinedDataTypes.String)
 *
 * // Usually at application startup
 * val result = SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers)
 *
 * check(result.failedStatements == 0) { "Queue schema is not up to date" }
 * ```
 *
 * The same from Java:
 *
 * ```java
 * var orders = Queue.of("orders", PredefinedDataTypes.String, Metadata.of(ACCOUNT_ID));
 * var customers = Queue.of("customers", PredefinedDataTypes.String);
 *
 * // Usually at application startup
 * var result = SchemaHelpers.createOrUpdateQueues(dataSource, orders, customers);
 *
 * if (result.getFailedStatements() > 0) {
 *     throw new IllegalStateException("Queue schema is not up to date");
 * }
 * ```
 *
 * Typically, applications work with statically defined queues that don't change while the application is running, so the
 * typical approach is to update the schema only once, at application startup.
 *
 * However, if your workflow is more complex and queues can be created dynamically in code (by HTTP request, for example),
 * it's perfectly fine to update the queue schemas in the database as often as needed.
 *
 * In any case, whether you update the schema once at application startup or dynamically, each time you need to create a new
 * queue, the process remains the same: Kolbasa reads the current schema, compares it with the queues defined in the code,
 * calculates the difference, and pushes it to the database. If there is no difference, there's nothing to do, and the
 * process completes very quickly.
 *
 * @see Schema
 * @see SchemaResult
 */
object SchemaHelpers {

    // ----------------------------------------------------------------------------------------
    // Create/Update functions

    /**
     * Generate all statements needed to create/update database schema but doesn't execute them
     */
    @JvmStatic
    fun generateCreateOrUpdateStatements(dataSource: DataSource, mainQueues: List<Queue<*>>): Map<Queue<*>, Schema> {
        checkAllQueuesAreMain(mainQueues)

        // Init system tables
        IdSchema.createAndInitIdTable(dataSource)
        val node = IdSchema.readNodeInfo(dataSource)
        val idRange = IdRange.generateRange(node.identifiersBucket)

        // Expand queue list to include companion queues
        val allQueues = buildList {
            mainQueues.forEach { queue ->
                add(queue)
                queue.deadLetterQueue?.let { add(it) }
                queue.archiveQueue?.let { add(it) }
            }
        }

        val existingTables = SchemaExtractor
            .extractRawSchema(dataSource, allQueues.map { it.dbTableName }.toSet())
            .filter { it.value.isQueueTable() }

        return allQueues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateTableSchema(queue, existingTable, idRange)
        }
    }

    /**
     * Generate all statements needed to create/update database schema but doesn't execute them
     */
    @JvmStatic
    fun generateCreateOrUpdateStatements(dataSource: DataSource, vararg mainQueues: Queue<*>): Map<Queue<*>, Schema> {
        return generateCreateOrUpdateStatements(dataSource, mainQueues.toList())
    }

    /**
     * Update database schema.
     *
     * Every kolbasa queue has its own real table in the database. This method creates or updates the table schema for the queue.
     * When we want to use a queue, we may have the following situations:
     * 1) This is the first use of the queue and the table in the database simply does not exist, it must be created from scratch
     * 2) The queue has already been used, the table in the database exists, but since the last use, the queue metadata
     *    has changed and one or more columns/indexes must be added to the table in the database
     * 3) The queue has not changed, but the internal data representation in kolbasa itself has changed and several service
     *    columns/indexes must be added/removed
     * 4) The queue has not changed, the table in the database is up-to-date, nothing needs to be done
     *
     * This is a convenient method that allows you to simply bring the table in the database to the current queue state,
     * making the correct data migration for each of the above cases
     *
     * This method is heavily optimized for many queues at once. Whatever the number of queues, one call initializes
     * the system tables, reads the existing database schema once and then applies the statements for all the queues
     * together. Calling it once per queue repeats all of that fixed work for every single queue.
     *
     * So update the schema for all your queues in one call whenever you can. One call for 10 000 queues is much,
     * much faster than 10 000 calls with one queue each.
     */
    @JvmStatic
    fun createOrUpdateQueues(dataSource: DataSource, mainQueues: List<Queue<*>>): SchemaResult {
        val mergedSchema = generateCreateOrUpdateStatements(dataSource, mainQueues).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Update database schema
     *
     * See [createOrUpdateQueues] for more details
     */
    @JvmStatic
    fun createOrUpdateQueues(dataSource: DataSource, vararg mainQueues: Queue<*>): SchemaResult {
        return createOrUpdateQueues(dataSource, mainQueues.toList())
    }

    // ----------------------------------------------------------------------------------------
    // Rename functions
    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateRenameStatements(
        dataSource: DataSource,
        mainQueues: List<Queue<*>>,
        renameFunction: (Queue<*>) -> String
    ): Map<Queue<*>, Schema> {
        checkAllQueuesAreMain(mainQueues)

        // Expand queue list to include companion queues
        val allQueues = buildList {
            mainQueues.forEach { queue ->
                add(queue)
                queue.deadLetterQueue?.let { add(it) }
                queue.archiveQueue?.let { add(it) }
            }
        }

        val existingTables = SchemaExtractor
            .extractRawSchema(dataSource, allQueues.map { it.dbTableName }.toSet())
            .filter { it.value.isQueueTable() }

        // Build rename map: for each parent queue, derive new names for it and its companions
        val renameMap = mutableMapOf<Queue<*>, String>()
        mainQueues.forEach { queue ->
            val newParentName = renameFunction(queue)
            renameMap[queue] = newParentName
            queue.deadLetterQueue?.let { renameMap[it] = newParentName + Const.DLQ_TABLE_NAME_SUFFIX }
            queue.archiveQueue?.let { renameMap[it] = newParentName + Const.ARCHIVE_TABLE_NAME_SUFFIX }
        }

        return allQueues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            val newTableName = renameMap.getValue(queue)
            SchemaGenerator.generateRenameTableSchema(queue, existingTable, newTableName)
        }
    }

    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateRenameStatements(
        dataSource: DataSource,
        vararg mainQueues: Queue<*>,
        renameFunction: (Queue<*>) -> String
    ): Map<Queue<*>, Schema> {
        return generateRenameStatements(dataSource, mainQueues.toList(), renameFunction)
    }

    /**
     * Rename queue tables
     *
     * Every Kolbasa queue has its own real table in the database. This method renames the queue table based on the
     * existing database state and the queue's real table name, since the table name differs from the queue name.
     *
     * This is a convenient method that allows you to rename the table in the database to a new name generated by
     * the [renameFunction].
     */
    @JvmStatic
    fun renameQueues(dataSource: DataSource, mainQueues: List<Queue<*>>, renameFunction: (Queue<*>) -> String): SchemaResult {
        val mergedSchema = generateRenameStatements(dataSource, mainQueues, renameFunction).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Rename queue tables
     *
     * See [renameQueues] for more details
     */
    @JvmStatic
    fun renameQueues(dataSource: DataSource, vararg mainQueues: Queue<*>, renameFunction: (Queue<*>) -> String): SchemaResult {
        return renameQueues(dataSource, mainQueues.toList(), renameFunction)
    }


    // ----------------------------------------------------------------------------------------
    // Delete functions
    /**
     * Generate all statements needed to rename queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateDeleteStatements(dataSource: DataSource, mainQueues: List<Queue<*>>): Map<Queue<*>, Schema> {
        checkAllQueuesAreMain(mainQueues)

        // Expand queue list to include companion queues (companions first, then parent)
        val allQueues = buildList {
            mainQueues.forEach { queue ->
                queue.deadLetterQueue?.let { add(it) }
                queue.archiveQueue?.let { add(it) }
                add(queue)
            }
        }

        val existingTables = SchemaExtractor
            .extractRawSchema(dataSource, allQueues.map { it.dbTableName }.toSet())
            .filter { it.value.isQueueTable() }

        return allQueues.associateWith { queue ->
            val existingTable = existingTables[queue.dbTableName]
            SchemaGenerator.generateDropTableSchema(queue, existingTable)
        }
    }

    /**
     * Generate all statements needed to delete queue tables but doesn't execute them
     */
    @JvmStatic
    fun generateDeleteStatements(dataSource: DataSource, vararg mainQueues: Queue<*>): Map<Queue<*>, Schema> {
        return generateDeleteStatements(dataSource, mainQueues.toList())
    }

    /**
     * Rename queue tables
     *
     * Every Kolbasa queue has its own real table in the database. This method drops the queue table based on the
     * existing database state and the queue's real table name, since the table name differs from the queue name.
     *
     * This is a convenient method that allows you to drop the table in the database
     */
    @JvmStatic
    fun deleteQueues(dataSource: DataSource, mainQueues: List<Queue<*>>): SchemaResult {
        val mergedSchema = generateDeleteStatements(dataSource, mainQueues).values.merge()
        return executeSchemaStatements(dataSource, mergedSchema)
    }

    /**
     * Delete queue tables
     *
     * See [deleteQueues] for more details
     */
    @JvmStatic
    fun deleteQueues(dataSource: DataSource, vararg mainQueues: Queue<*>): SchemaResult {
        return deleteQueues(dataSource, mainQueues.toList())
    }


    /**
     * Executes the statements of a [Schema] and reports what failed.
     *
     * Most applications never call this: [createOrUpdateQueues] and its siblings generate the statements and run
     * them in one step. Use this method when you generated the statements yourself with one of the `generate…`
     * methods – to log them, to review them, or to run them at a moment you choose.
     *
     * How the statements are executed:
     * - every statement runs with autocommit on, so each one commits on its own and nothing is rolled back later;
     * - table statements are sent in groups to save round trips. If a group fails, its statements are sent again one
     *   by one, so one bad statement cannot hide the others;
     * - index statements always run one by one, because `create index concurrently` cannot run inside a transaction.
     *
     * A failing statement does not stop the run and does not throw. The remaining statements are still executed, and
     * every failure is collected in the result, so check
     * [SchemaResult.failedStatements] before you trust the schema.
     *
     * An empty schema executes nothing and returns a result with no failures.
     *
     * @param dataSource the database to work on
     * @param schema the statements to execute
     * @return which statements were executed and which of them failed
     */
    @JvmStatic
    fun executeSchemaStatements(dataSource: DataSource, schema: Schema): SchemaResult {
        if (schema.isEmpty) {
            // nothing to execute
            return SchemaResult(schema, 0, emptyList(), emptyList())
        }

        // separate transaction for each statement
        return dataSource.useConnectionWithAutocommit { connection ->
            connection.createStatement().use { statement ->

                // Execute table statements
                // Try to execute them in chunks to reduce number of round-trips to the database
                val failedTableStatements = mutableListOf<FailedStatement>()
                schema.tableStatements.chunked(DEFAULT_CHUNK_SIZE).forEach { statementsChunk ->
                    try {
                        val combined = statementsChunk.joinToString(separator = ";")
                        statement.execute(combined)
                    } catch (_: Exception) {
                        // If we failed to execute the chunk, try to execute statements one by one
                        statementsChunk.forEach { sql ->
                            try {
                                statement.execute(sql)
                            } catch (e: Exception) {
                                failedTableStatements += FailedStatement(sql, e)
                            }
                        }
                    }
                }

                // Execute index statements
                // Can't batch them because almost all Kolbasa indexes use CREATE INDEX CONCURRENTLY which
                // cannot be executed inside a transaction block
                val failedIndexStatements = mutableListOf<FailedStatement>()
                schema.indexStatements.forEach { sql ->
                    try {
                        statement.execute(sql)
                    } catch (e: Exception) {
                        failedIndexStatements += FailedStatement(sql, e)
                    }
                }

                SchemaResult(
                    schema = schema,
                    failedStatements = failedTableStatements.size + failedIndexStatements.size,
                    failedTableStatements = failedTableStatements,
                    failedIndexStatements = failedIndexStatements
                )
            }
        }
    }

    private const val DEFAULT_CHUNK_SIZE = 25

    private fun checkAllQueuesAreMain(mainQueues: List<Queue<*>>) {
        mainQueues.forEach { queue ->
            check(queue.queueRole == QueueRole.MAIN) {
                "Only MAIN queues are allowed, but '${queue.name}' has type ${queue.queueRole}. " +
                    "DLQ and Archive queues are managed automatically through their parent MAIN queue."
            }
        }
    }

}
