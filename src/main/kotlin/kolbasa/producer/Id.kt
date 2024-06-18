package kolbasa.producer

import kolbasa.schema.Const

/**
 * Full record id
 *
 * This class is a container for all the parts that make up an ID. Only all components, only full ID uniquely
 * identifies the record. Any part doesn't make sense without the others.
 */
data class Id(
    val localId: Long,
    val serverId: String?
) {

    override fun toString(): String {
        return if (serverId == null) {
            localId.toString()
        } else {
            "$localId/$serverId"
        }
    }

    companion object {
        internal val DEFAULT_DUPLICATE_ID = Id(Const.RESERVED_DUPLICATE_ID, null)
    }

}
