package kolbasa.schema

import kolbasa.utils.JdbcHelpers.useConnectionWithAutocommit
import kolbasa.queue.Queue
import kolbasa.queue.QueueType
import kolbasa.schema.Schema.Companion.merge
import kolbasa.schema.SchemaHelpers.createOrUpdateQueues
import kolbasa.schema.SchemaHelpers.deleteQueues
import kolbasa.schema.SchemaHelpers.renameQueues
import javax.sql.DataSource

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
            check(queue.queueType == QueueType.MAIN) {
                "Only MAIN queues are allowed, but '${queue.name}' has type ${queue.queueType}. " +
                    "DLQ and Archive queues are managed automatically through their parent MAIN queue."
            }
        }
    }

}
