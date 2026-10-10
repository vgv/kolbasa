package kolbasa.cluster.butcher

import kolbasa.utils.JdbcHelpers.usePreparedStatement
import kolbasa.utils.JdbcHelpers.useStatement
import kolbasa.schema.*
import org.postgresql.util.PGobject
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Types
import java.time.OffsetDateTime
import javax.sql.DataSource

internal class MoveOneTable(
    private val sourceDataSource: DataSource,
    private val targetDataSource: DataSource,
    private val moveProgressCallback: ProgressCallback,
    private val shards: List<Int>,
    private val table: Table,
    private val rowsPerBatch: Int
) {

    fun move(): Int {
        val dbTableName = table.name
        val columns = table.columns.toList()
        val columnsString = columns.joinToString(",") { it.name }
        val shardsString = shards.joinToString(",") { it.toString() }
        val insertQuestionMarks = columns.joinToString(",") { "?" }
        val selectQuery = "select $columnsString from $dbTableName where shard in ($shardsString) limit $rowsPerBatch"
        val insertQuery = "insert into $dbTableName ($columnsString) values ($insertQuestionMarks) on conflict do nothing"

        moveProgressCallback.moveStart(dbTableName, sourceDataSource, targetDataSource)

        var totalMovedRows = 0
        do {
            val movedRows = moveOneBatch(insertQuery, selectQuery, columns, dbTableName)
            totalMovedRows += movedRows
            moveProgressCallback.moveNextBatch(dbTableName, sourceDataSource, targetDataSource, totalMovedRows)
        } while (movedRows > 0)

        moveProgressCallback.moveEnd(dbTableName, sourceDataSource, targetDataSource, totalMovedRows)

        return totalMovedRows
    }

    private fun moveOneBatch(insertQuery: String, selectQuery: String, columns: List<Column>, dbTableName: String): Int {
        val movedIds = ArrayList<Long>(rowsPerBatch)

        // Move rows from the source table to the target table
        sourceDataSource.useStatement { sourceStmt: Statement ->
            targetDataSource.usePreparedStatement(insertQuery) { targetStmt ->
                val resultSet = sourceStmt.executeQuery(selectQuery)
                while (resultSet.next()) {
                    movedIds += moveOneRow(columns, resultSet, targetStmt)
                    targetStmt.addBatch()
                }

                targetStmt.executeBatch()
            }

            // Delete moved rows from the source table in the same transaction
            if (movedIds.isNotEmpty()) {
                val deleteQuery = "delete from $dbTableName where id in (${movedIds.joinToString(",")})"
                sourceStmt.executeUpdate(deleteQuery)
            }
        }

        return movedIds.size
    }

    private fun moveOneRow(columns: List<Column>, resultSet: ResultSet, targetStmt: PreparedStatement): Long {
        var index = 1
        var rowId: Long = -1

        columns.forEach { column ->
            moveOneValue(column.type, resultSet, index, targetStmt)
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

    private fun moveOneValue(
        columnType: ColumnType,
        resultSet: ResultSet,
        resultSetIndex: Int,
        targetStmt: PreparedStatement
    ) {
        when (columnType) {
            ColumnType.SMALLINT -> {
                val value = resultSet.getShort(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setShort(resultSetIndex, value)
                }
            }

            ColumnType.INT -> {
                val value = resultSet.getInt(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setInt(resultSetIndex, value)
                }
            }

            ColumnType.BIGINT -> {
                val value = resultSet.getLong(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setLong(resultSetIndex, value)
                }
            }

            ColumnType.REAL -> {
                val value = resultSet.getFloat(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setFloat(resultSetIndex, value)
                }
            }

            ColumnType.DOUBLE -> {
                val value = resultSet.getDouble(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setDouble(resultSetIndex, value)
                }
            }

            ColumnType.NUMERIC -> {
                val value = resultSet.getBigDecimal(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setBigDecimal(resultSetIndex, value)
                }
            }

            ColumnType.BOOLEAN -> {
                val value = resultSet.getBoolean(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setBoolean(resultSetIndex, value)
                }
            }

            ColumnType.TIMESTAMP -> {
                val value = resultSet.getTimestamp(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setTimestamp(resultSetIndex, value)
                }
            }

            ColumnType.TIMESTAMPTZ -> {
                // OffsetDateTime is the JSR-310 bridge for timestamptz — timezone-safe across sessions.
                val value = resultSet.getObject(resultSetIndex, OffsetDateTime::class.java)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setObject(resultSetIndex, value, Types.TIMESTAMP_WITH_TIMEZONE)
                }
            }

            ColumnType.VARCHAR -> {
                val value = resultSet.getString(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setString(resultSetIndex, value)
                }
            }

            ColumnType.VARCHAR_ARRAY -> {
                val value = resultSet.getArray(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setArray(resultSetIndex, value)
                }
            }

            ColumnType.BYTEARRAY -> {
                val value = resultSet.getBytes(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    targetStmt.setBytes(resultSetIndex, value)
                }
            }

            ColumnType.JSONB -> {
                val value = resultSet.getString(resultSetIndex)
                if (resultSet.wasNull()) {
                    targetStmt.setNull(resultSetIndex, columnType.sqlType)
                } else {
                    val jsonObject = PGobject()
                    jsonObject.type = "jsonb"
                    jsonObject.value = value
                    targetStmt.setObject(resultSetIndex, jsonObject)
                }
            }
        }
    }

}
