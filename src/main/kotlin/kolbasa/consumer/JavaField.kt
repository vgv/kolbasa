package kolbasa.consumer

data class JavaField<M, T>(val name: String) {

    companion object {
        @JvmStatic
        @Suppress("UNUSED_PARAMETER")
        fun <M, T> of(name: String, accessor: java.util.function.Function<M, T>): JavaField<M, T> {
            return JavaField(name)
        }
    }

}
