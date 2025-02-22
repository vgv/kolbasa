package kolbasa.cluster.migrate

import kolbasa.pg.DatabaseExtensions.usePreparedStatement
import kolbasa.pg.DatabaseExtensions.useStatement
import kolbasa.schema.*
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import javax.sql.DataSource

internal class MigrateOneTable(
    private val shards: List<Int>,
    private val schema: Table,
    private val sourceDataSource: DataSource,
    private val targetDataSource: DataSource,
    private val rowsPerBatch: Int,
    private val moveProgressCallback: MigrateEvents
) {

    fun migrate(): Int {
        val dbTableName = schema.name
        val columns = schema.columns.toList()
        val columnsString = columns.joinToString(",") { it.name }
        val shardsString = shards.joinToString(",") { it.toString() }
        val insertQuestionMarks = columns.joinToString(",") { "?" }

        val selectQuery = "select $columnsString from $dbTableName where shard in ($shardsString) limit $rowsPerBatch"
        val insertQuery = "insert into $dbTableName ($columnsString) values ($insertQuestionMarks) on conflict do nothing"

        moveProgressCallback.migrateStart(dbTableName, sourceDataSource, targetDataSource)

        var totalMovedRows = 0
        do {
            val movedRows = moveOneBatch(insertQuery, selectQuery, columns, dbTableName)
            totalMovedRows += movedRows
            moveProgressCallback.migrateNextBatch(dbTableName, sourceDataSource, targetDataSource, totalMovedRows)
        } while (movedRows > 0)

        moveProgressCallback.migrateEnd(dbTableName, sourceDataSource, targetDataSource, totalMovedRows)

        return totalMovedRows
    }

    private fun moveOneBatch(insertQuery: String, selectQuery: String, columns: List<Column>, dbTableName: String): Int {
        val movedIds = mutableListOf<Long>()

        // Move rows from the source table to the target table
        sourceDataSource.useStatement { selectStatement: Statement ->
            targetDataSource.usePreparedStatement(insertQuery) { insertStatement ->
                val resultSet = selectStatement.executeQuery(selectQuery)
                while (resultSet.next()) {
                    movedIds += moveOneRow(columns, resultSet, insertStatement)
                    insertStatement.addBatch()
                }

                insertStatement.executeBatch()
            }
        }

        // Delete moved rows from the source table
        if (movedIds.isNotEmpty()) {
            sourceDataSource.useStatement { statement: Statement ->
                val deleteQuery = "delete from $dbTableName where id in (${movedIds.joinToString(",")})"
                statement.executeUpdate(deleteQuery)
            }
        }

        return movedIds.size
    }

    private fun moveOneRow(columns: List<Column>, resultSet: ResultSet, insertStatement: PreparedStatement): Long {
        var index = 1
        var rowId: Long = -1

        columns.forEach { column ->
            moveOneValue(column, resultSet, index, insertStatement)
            if (column.name == Const.ID_COLUMN_NAME) {
                rowId = resultSet.getLong(index)
            }

            index++
        }

        check(rowId != -1L) {
            "Row ID is not found"
        }

        return rowId
    }

    private fun moveOneValue(column: Column, resultSet: ResultSet, resultSetIndex: Int, insertStatement: PreparedStatement) {
        when (column.type) {
            ColumnType.SMALLINT -> {
                val value = resultSet.getShort(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setShort(resultSetIndex, value)
                }
            }

            ColumnType.INT -> {
                val value = resultSet.getInt(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setInt(resultSetIndex, value)
                }
            }

            ColumnType.BIGINT -> {
                val value = resultSet.getLong(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setLong(resultSetIndex, value)
                }
            }

            ColumnType.REAL -> {
                val value = resultSet.getFloat(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setFloat(resultSetIndex, value)
                }
            }

            ColumnType.DOUBLE -> {
                val value = resultSet.getDouble(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setDouble(resultSetIndex, value)
                }
            }

            ColumnType.NUMERIC -> {
                val value = resultSet.getBigDecimal(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setBigDecimal(resultSetIndex, value)
                }
            }

            ColumnType.BOOLEAN -> {
                val value = resultSet.getBoolean(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setBoolean(resultSetIndex, value)
                }
            }

            ColumnType.TIMESTAMP -> {
                val value = resultSet.getTimestamp(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setTimestamp(resultSetIndex, value)
                }
            }

            ColumnType.VARCHAR -> {
                val value = resultSet.getString(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setString(resultSetIndex, value)
                }
            }

            ColumnType.VARCHAR_ARRAY -> {
                val value = resultSet.getArray(resultSetIndex)
                if (resultSet.wasNull()) {
                    insertStatement.setNull(resultSetIndex, column.type.sqlType)
                } else {
                    insertStatement.setArray(resultSetIndex, value)
                }
            }
        }
    }

}
