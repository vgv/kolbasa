package kolbasa.producer

import kolbasa.schema.Const

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

        val DEFAULT_DUPLICATE_ID = Id(Const.RESERVED_DUPLICATE_ID, null)

    }

}
