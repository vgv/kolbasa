package kolbasa.consumer

data class JavaField<Meta, T>(val name: String) {

    companion object {
        @JvmStatic
        @Suppress("UNUSED_PARAMETER")
        fun <Meta, T> of(name: String, accessor: java.util.function.Function<Meta, T>): JavaField<Meta, T> {
            return JavaField(name)
        }
    }

}
