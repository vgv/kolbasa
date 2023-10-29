package kolbasa.consumer

/**
 * Class describes field accessor for Java Record classes.
 *
 * It's very convenient to get a reference to record class field by `MyRecordClass::userId`. Return type
 * will be `Function<MyRecordClass, Integer>` (in this example) and this is exactly what Kolbasa needs – function
 * that accepts `MyRecordClass` instance and returns `userId` field value.
 *
 * However, there is a problem – [Function][java.util.function.Function] doesn't have a name, it't just an anonymous
 * function. This is the reason why Kolbasa requires to pass two parameters: field name and reference to the field.
 *
 * Usage
 * ```
 * var field = JavaField.of("userId", MyRecordClass::userId)
 * var messages = consumer.receive(100, () -> Filter.eq(field, 42));
 * ```
 * 
 * @param Meta meta class
 * @param T field type
 */
data class JavaField<Meta, T>(val name: String) {

    companion object {
        @JvmStatic
        @Suppress("UNUSED_PARAMETER")
        fun <Meta, T> of(name: String, accessor: java.util.function.Function<Meta, T>): JavaField<Meta, T> {
            return JavaField(name)
        }
    }

}
