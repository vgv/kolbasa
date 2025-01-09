package kolbasa.producer

import kolbasa.cluster.Shard
import kolbasa.schema.Const

/**
 * Full record id
 *
 * This class is a container for all the parts that make up an ID. Only all components, only full ID uniquely
 * identifies the record. Any part doesn't make sense without the others.
 */
data class Id(
    val localId: Long,
    val shard: Int
) {

    override fun toString(): String {
        return "$localId/$shard"
    }

}
