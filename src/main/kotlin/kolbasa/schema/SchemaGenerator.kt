package kolbasa.schema

import kolbasa.cluster.Shard
import kolbasa.queue.Queue
import kolbasa.queue.QueueOptions
import kolbasa.queue.meta.MetaField
import kolbasa.queue.meta.MetaIndexType

internal object SchemaGenerator {

    internal fun generateTableSchema(queue: Queue<*, *>, existingTable: Table?, idRange: IdRange): Schema {
        val mutableSchema = MutableSchema()

        // table
        forTable(queue, existingTable, mutableSchema, idRange)

        // table sequence
        forIdSequence(queue, existingTable, mutableSchema, idRange)

        // shard column
        forShard(queue, existingTable, mutableSchema)

        // scheduledAt column
        forScheduledAtColumn(queue, existingTable, mutableSchema)

        // attempts
        forRemainingAttemptsColumn(queue, existingTable, mutableSchema)

        // metadata
        queue.metadataDescription?.fields?.forEach { metaField ->
            forMetaFieldColumn(queue, metaField, existingTable, mutableSchema)
        }

        return Schema(
            SchemaStatements(mutableSchema.allTables, mutableSchema.allIndexes),
            SchemaStatements(mutableSchema.requiredTables, mutableSchema.requiredIndexes)
        )
    }

    private fun forTable(queue: Queue<*, *>, existingTable: Table?, mutableSchema: MutableSchema, idRange: IdRange) {
        val createTableStatement = """
            create table if not exists ${queue.dbTableName}(
                ${Const.ID_COLUMN_NAME} bigint generated always as identity (minvalue ${idRange.start} maxvalue ${idRange.end} cycle) primary key,
                ${Const.USELESS_COUNTER_COLUMN_NAME} int,
                ${Const.OPENTELEMETRY_COLUMN_NAME} varchar(${Const.OPENTELEMETRY_VALUE_LENGTH})[],
                ${Const.SHARD_COLUMN_NAME} int not null,
                ${Const.CREATED_AT_COLUMN_NAME} timestamp not null default clock_timestamp(),
                ${Const.SCHEDULED_AT_COLUMN_NAME} timestamp not null,
                ${Const.PROCESSING_AT_COLUMN_NAME} timestamp,
                ${Const.PRODUCER_COLUMN_NAME} varchar(${Const.PRODUCER_CONSUMER_VALUE_LENGTH}),
                ${Const.CONSUMER_COLUMN_NAME} varchar(${Const.PRODUCER_CONSUMER_VALUE_LENGTH}),
                ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} int not null,
                ${Const.DATA_COLUMN_NAME} ${queue.databaseDataType.dbColumnType} not null
            )
            """.trimIndent()

        mutableSchema.allTables += createTableStatement
        if (existingTable == null) {
            mutableSchema.requiredTables += createTableStatement
        }
    }

    private fun forIdSequence(
        queue: Queue<*, *>,
        existingTable: Table?,
        mutableSchema: MutableSchema,
        idRange: IdRange
    ) {
        // TODO("Not yet implemented")
    }



    private fun forShard(queue: Queue<*, *>, existingTable: Table?, mutableSchema: MutableSchema){
        val hasColumn = existingTable?.findColumn(Const.SHARD_COLUMN_NAME) != null
        val shardColumn = """
            alter table ${queue.dbTableName}
            add if not exists ${Const.SHARD_COLUMN_NAME} int not null default ${Shard.MIN_SHARD}
        """.trimIndent()

        mutableSchema.allTables += shardColumn
        if (!hasColumn) {
            mutableSchema.requiredTables += shardColumn
        }

        // index
        val indexName = queue.dbTableName + "_" + Const.SHARD_COLUMN_NAME
        val indexStatement = """
                create index concurrently if not exists $indexName
                on ${queue.dbTableName}(${Const.SHARD_COLUMN_NAME})
            """.trimIndent()

        val hasIndex = existingTable?.findIndex(indexName) != null
        mutableSchema.allIndexes += indexStatement
        if (!hasIndex) {
            mutableSchema.requiredIndexes += indexStatement
        }
    }

    private fun forScheduledAtColumn(queue: Queue<*, *>, existingTable: Table?, mutableSchema: MutableSchema) {
        val hasDefaultClause = existingTable
            ?.findColumn(Const.SCHEDULED_AT_COLUMN_NAME)
            ?.defaultExpression != null

        if (queue.options == null || queue.options.defaultDelay.isZero) {
            val alterStatement = """
                    alter table ${queue.dbTableName}
                    alter ${Const.SCHEDULED_AT_COLUMN_NAME}
                    set default clock_timestamp()
                """.trimIndent()

            if (existingTable != null) {
                mutableSchema.allTables += alterStatement
                if (hasDefaultClause) {
                    mutableSchema.requiredTables += alterStatement
                }
            }
        } else {
            val alterStatement = """
                    alter table ${queue.dbTableName}
                    alter ${Const.SCHEDULED_AT_COLUMN_NAME}
                    set default clock_timestamp() + interval '${queue.options.defaultDelay.toMillis()} millisecond'
                """.trimIndent()

            mutableSchema.allTables += alterStatement
            if (!hasDefaultClause) {
                mutableSchema.requiredTables += alterStatement
            }
        }

        // index
        val indexName = queue.dbTableName + "_" + Const.SCHEDULED_AT_COLUMN_NAME
        val indexStatement = """
                create index concurrently if not exists $indexName
                on ${queue.dbTableName}(${Const.SCHEDULED_AT_COLUMN_NAME})
            """.trimIndent()

        val hasIndex = existingTable?.findIndex(indexName) != null
        mutableSchema.allIndexes += indexStatement
        if (!hasIndex) {
            mutableSchema.requiredIndexes += indexStatement
        }
    }

    private fun forRemainingAttemptsColumn(queue: Queue<*, *>, existingTable: Table?, mutableSchema: MutableSchema) {
        val desiredAttempts = queue.options?.defaultAttempts ?: QueueOptions.DEFAULT_ATTEMPTS
        val currentDefaultClause = existingTable
            ?.findColumn(Const.REMAINING_ATTEMPTS_COLUMN_NAME)
            ?.defaultExpression

        val alterStatement = """
                alter table ${queue.dbTableName}
                alter ${Const.REMAINING_ATTEMPTS_COLUMN_NAME}
                set default $desiredAttempts
            """.trimIndent()

        mutableSchema.allTables += alterStatement
        if (desiredAttempts.toString() != currentDefaultClause) {
            mutableSchema.requiredTables += alterStatement
        }
    }

    private fun forMetaFieldColumn(
        queue: Queue<*, *>,
        metaField: MetaField<*>,
        existingTable: Table?,
        mutableSchema: MutableSchema
    ) {
        val hasColumn = existingTable?.findColumn(metaField.dbColumnName) != null
        val alterStatement = """
            alter table ${queue.dbTableName}
            add if not exists ${metaField.dbColumnName} ${metaField.dbColumnType}
        """.trimIndent()
        mutableSchema.allTables += alterStatement
        if (!hasColumn) {
            mutableSchema.requiredTables += alterStatement
        }

        // index
        val indexName = queue.dbTableName + "_" + metaField.dbColumnName
        val index = existingTable?.findIndex(indexName)
        val dropIndexStatement = "drop index concurrently if exists $indexName"

        when (metaField.dbIndexType) {
            MetaIndexType.NO_INDEX -> {
                if (existingTable != null) {
                    mutableSchema.allIndexes += dropIndexStatement
                    if (index != null) {
                        mutableSchema.requiredIndexes += dropIndexStatement
                    }
                }
            }

            MetaIndexType.JUST_INDEX -> {
                val indexStatement = """
                    create index concurrently if not exists $indexName
                    on ${queue.dbTableName}(${metaField.dbColumnName})
                """.trimIndent()

                if (index == null) {
                    mutableSchema.allIndexes += indexStatement
                    mutableSchema.requiredIndexes += indexStatement
                } else if (index.unique) {
                    mutableSchema.allIndexes += dropIndexStatement
                    mutableSchema.allIndexes += indexStatement
                    mutableSchema.requiredIndexes += dropIndexStatement
                    mutableSchema.requiredIndexes += indexStatement
                } else {
                    mutableSchema.allIndexes += indexStatement
                }
            }

            MetaIndexType.UNIQUE_INDEX -> {
                val indexStatement = """
                    create unique index concurrently if not exists $indexName
                    on ${queue.dbTableName}(${metaField.dbColumnName})
                    where ${Const.REMAINING_ATTEMPTS_COLUMN_NAME} > 0
                """.trimIndent()

                if (index == null) {
                    mutableSchema.allIndexes += indexStatement
                    mutableSchema.requiredIndexes += indexStatement
                } else if (index.unique) {
                    mutableSchema.allIndexes += indexStatement
                } else {
                    mutableSchema.allIndexes += dropIndexStatement
                    mutableSchema.allIndexes += indexStatement
                    mutableSchema.requiredIndexes += dropIndexStatement
                    mutableSchema.requiredIndexes += indexStatement
                }
            }
        }
    }


    // Just a holder class for a few mutable lists
    private class MutableSchema {
        val allTables = mutableListOf<String>()
        val allIndexes = mutableListOf<String>()
        val requiredTables = mutableListOf<String>()
        val requiredIndexes = mutableListOf<String>()
    }

}

