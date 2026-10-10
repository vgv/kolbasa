package kolbasa.cluster.butcher.check

import kolbasa.assertNotNull
import kolbasa.schema.Column
import kolbasa.schema.ColumnType
import kolbasa.schema.Const
import kolbasa.schema.Identity
import kolbasa.schema.NodeId
import kolbasa.schema.Table
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SchemaConsistencyTest {

    @Test
    fun testEmptyCluster_IsClean() {
        val result = SchemaConsistency(emptyMap()).compute()

        assertTrue(result.isClean)
        assertEquals(0, result.totalDivergent)
    }

    @Test
    fun testAllNodesIdentical_IsClean() {
        val schema = mapOf("q_orders" to queueTable("q_orders"))
        val schemas = mapOf(n1 to schema, n2 to schema, n3 to schema)

        val result = SchemaConsistency(schemas).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testExtraColumnOnOneNode_ReferenceIsMajority() {
        val baseline = queueTable("q_orders")
        val drifted = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_priority")))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        assertEquals(setOf(n1, n2), divergence.referenceNodes)
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertEquals(setOf(metaColumn("meta_priority")), delta.addedColumns)
        assertTrue(delta.removedColumns.isEmpty())
    }

    @Test
    fun testMissingColumnOnOneNode_HasRemovedDelta() {
        val baseline = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_user_id")))
        val drifted = queueTable("q_orders")
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertEquals(setOf(metaColumn("meta_user_id")), delta.removedColumns)
        assertTrue(delta.addedColumns.isEmpty())
    }

    @Test
    fun testTableMissingOnOneNode() {
        val schema = mapOf("q_orders" to queueTable("q_orders"))
        val schemas = mapOf(n1 to schema, n2 to schema, n3 to emptyMap())

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        assertEquals(setOf(n1, n2), divergence.referenceNodes)
        assertEquals(NodeDelta.Missing, divergence.deltasByNode[n3])
    }

    @Test
    fun testMultipleDeltasOnSameNode() {
        val baseline = queueTable("q_orders")
        val drifted = queueTable(
            "q_orders",
            extraColumns = setOf(metaColumn("meta_priority")),
            extraIndexes = setOf("q_orders_priority_idx"),
        )
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertEquals(setOf(metaColumn("meta_priority")), delta.addedColumns)
        assertEquals(setOf("q_orders_priority_idx"), delta.addedIndexes)
    }

    @Test
    fun testAddAndRemoveOnSameNode() {
        val baseline = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_user_id")))
        val drifted = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_priority")))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertEquals(setOf(metaColumn("meta_priority")), delta.addedColumns)
        assertEquals(setOf(metaColumn("meta_user_id")), delta.removedColumns)
    }

    @Test
    fun testThreeShapeSplit() {
        val a = queueTable("q_orders")
        val b = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_priority")))
        val c = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_other")))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to a),
            n2 to mapOf("q_orders" to a),
            n3 to mapOf("q_orders" to b),
            n4 to mapOf("q_orders" to c),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        assertEquals(setOf(n1, n2), divergence.referenceNodes)
        assertTrue(divergence.deltasByNode[n3] is NodeDelta.ShapeDiff)
        assertTrue(divergence.deltasByNode[n4] is NodeDelta.ShapeDiff)
    }

    @Test
    fun testTiedBucketSizes_ReferenceContainsLowestNodeId() {
        val a = queueTable("q_orders")
        val b = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_b")))
        val c = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_c")))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to a),
            n2 to mapOf("q_orders" to b),
            n3 to mapOf("q_orders" to c),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        assertEquals(setOf(n1), divergence.referenceNodes)
    }

    @Test
    fun testIdentityPresenceDiffers() {
        val withIdentity = queueTable("q_orders")
        val withoutIdentity = withIdentity.copy(identity = null)
        val schemas = mapOf(
            n1 to mapOf("q_orders" to withIdentity),
            n2 to mapOf("q_orders" to withIdentity),
            n3 to mapOf("q_orders" to withoutIdentity),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertTrue(delta.identityPresenceDiffers)
    }

    @Test
    fun testIdentityFieldValuesDiffer_NotADivergence() {
        val tableA = queueTable("q_orders", identity = identityWith(min = 1, max = 100))
        val tableB = queueTable("q_orders", identity = identityWith(min = 200, max = 300))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to tableA),
            n2 to mapOf("q_orders" to tableB),
        )

        val result = SchemaConsistency(schemas).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testNonKolbasaTablesIgnored() {
        val schemas = mapOf(
            n1 to mapOf("users" to nonQueueTable("users")),
            n2 to mapOf<String, Table>(),
        )

        val result = SchemaConsistency(schemas).compute()

        assertTrue(result.isClean)
    }

    @Test
    fun testCompanionTableTreatedLikeAnyKolbasaTable() {
        val main = queueTable("q_orders")
        val companion = queueTable("q_orders_dlq")
        val schemas = mapOf(
            n1 to mapOf("q_orders" to main, "q_orders_dlq" to companion),
            n2 to mapOf("q_orders" to main),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders_dlq"])
        assertEquals(NodeDelta.Missing, divergence.deltasByNode[n2])
    }

    @Test
    fun testIndexSetDiffers() {
        val baseline = queueTable("q_orders", extraIndexes = setOf("q_orders_extra_idx"))
        val drifted = queueTable("q_orders")
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val result = SchemaConsistency(schemas).compute()

        val divergence = assertNotNull(result.divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff
        assertEquals(setOf("q_orders_extra_idx"), delta.removedIndexes)
    }

    @Test
    fun testColumnTypeChanged_ReportedAsChange() {
        val baseline = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.INT, true, null)))
        val drifted = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.VARCHAR, true, null)))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val divergence = assertNotNull(SchemaConsistency(schemas).compute().divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff

        assertTrue(delta.addedColumns.isEmpty())
        assertTrue(delta.removedColumns.isEmpty())
        assertEquals(1, delta.changedColumns.size)
        val change = delta.changedColumns.single()
        assertEquals("meta_foo", change.name)
        assertEquals(ColumnType.INT, change.reference.type)
        assertEquals(ColumnType.VARCHAR, change.other.type)
    }

    @Test
    fun testColumnNullabilityChanged_ReportedAsChange() {
        val baseline = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.INT, true, null)))
        val drifted = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.INT, false, null)))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val divergence = assertNotNull(SchemaConsistency(schemas).compute().divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff

        assertEquals(1, delta.changedColumns.size)
        val change = delta.changedColumns.single()
        assertEquals(true, change.reference.nullable)
        assertEquals(false, change.other.nullable)
    }

    @Test
    fun testColumnAddRemoveChangeCombined() {
        val baseline = queueTable(
            "q_orders",
            extraColumns = setOf(
                Column("meta_keep", ColumnType.INT, true, null),
                Column("meta_change", ColumnType.INT, true, null),
                Column("meta_remove", ColumnType.INT, true, null),
            ),
        )
        val drifted = queueTable(
            "q_orders",
            extraColumns = setOf(
                Column("meta_keep", ColumnType.INT, true, null),
                Column("meta_change", ColumnType.VARCHAR, true, null),
                Column("meta_add", ColumnType.INT, true, null),
            ),
        )
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val divergence = assertNotNull(SchemaConsistency(schemas).compute().divergencesByTable["q_orders"])
        val delta = divergence.deltasByNode[n3] as NodeDelta.ShapeDiff

        assertEquals(setOf("meta_add"), delta.addedColumns.map { it.name }.toSet())
        assertEquals(setOf("meta_remove"), delta.removedColumns.map { it.name }.toSet())
        assertEquals(setOf("meta_change"), delta.changedColumns.map { it.name }.toSet())
    }

    @Test
    fun testToString_ColumnChange_RendersArrow() {
        val baseline = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.INT, true, null)))
        val drifted = queueTable("q_orders", extraColumns = setOf(Column("meta_foo", ColumnType.VARCHAR, true, null)))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
        )

        val text = SchemaConsistency(schemas).compute().toString()

        assertTrue(text.contains("~column meta_foo INT NULL -> VARCHAR NULL"), text)
    }

    @Test
    fun testToString_Clean() {
        val result = SchemaConsistency(emptyMap()).compute()

        assertEquals("Schema consistency: all tables consistent across cluster", result.toString())
    }

    @Test
    fun testToString_Dirty_ContainsExpectedFragments() {
        val baseline = queueTable("q_orders")
        val drifted = queueTable("q_orders", extraColumns = setOf(metaColumn("meta_priority")))
        val schemas = mapOf(
            n1 to mapOf("q_orders" to baseline),
            n2 to mapOf("q_orders" to baseline),
            n3 to mapOf("q_orders" to drifted),
            n4 to mapOf<String, Table>(),
        )

        val text = SchemaConsistency(schemas).compute().toString()

        assertTrue(text.contains("1 table(s) inconsistent"), text)
        assertTrue(text.contains("q_orders"), text)
        assertTrue(text.contains("[n1, n2]"), text)
        assertTrue(text.contains("n3: +column meta_priority"), text)
        assertTrue(text.contains("n4: missing"), text)
    }

    private val n1 = NodeId("n1")
    private val n2 = NodeId("n2")
    private val n3 = NodeId("n3")
    private val n4 = NodeId("n4")

    private fun queueTable(
        name: String,
        extraColumns: Set<Column> = emptySet(),
        extraIndexes: Set<String> = emptySet(),
        identity: Identity? = identityWith(),
    ): Table = Table(
        name = name,
        columns = REQUIRED_QUEUE_COLUMNS + extraColumns,
        indexes = setOf("${name}_pkey") + extraIndexes,
        identity = identity,
    )

    private fun nonQueueTable(name: String): Table = Table(
        name = name,
        columns = setOf(Column("foo", ColumnType.VARCHAR, false, null)),
        indexes = emptySet(),
        identity = null,
    )

    private fun metaColumn(name: String) = Column(name, ColumnType.INT, true, null)

    private fun identityWith(
        min: Long = 1L,
        max: Long = Long.MAX_VALUE,
    ) = Identity(
        name = "seq",
        start = min,
        min = min,
        max = max,
        increment = 1,
        cycles = false,
        cache = 1,
    )

    private val REQUIRED_QUEUE_COLUMNS = setOf(
        Column(Const.ID_COLUMN_NAME, ColumnType.BIGINT, false, null),
        Column(Const.USELESS_COUNTER_COLUMN_NAME, ColumnType.INT, false, null),
        Column(Const.OPENTELEMETRY_COLUMN_NAME, ColumnType.VARCHAR_ARRAY, true, null),
        Column(Const.SHARD_COLUMN_NAME, ColumnType.INT, false, null),
        Column(Const.CREATED_AT_COLUMN_NAME, ColumnType.TIMESTAMP, false, null),
        Column(Const.SCHEDULED_AT_COLUMN_NAME, ColumnType.TIMESTAMP, false, null),
        Column(Const.PROCESSING_AT_COLUMN_NAME, ColumnType.TIMESTAMP, true, null),
        Column(Const.PRODUCER_COLUMN_NAME, ColumnType.VARCHAR, true, null),
        Column(Const.CONSUMER_COLUMN_NAME, ColumnType.VARCHAR, true, null),
        Column(Const.REMAINING_ATTEMPTS_COLUMN_NAME, ColumnType.INT, false, null),
        Column(Const.DATA_COLUMN_NAME, ColumnType.BYTEARRAY, true, null),
    )
}
