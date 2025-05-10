package kolbasa.consumer

/**
 * Class describes field accessor for Java Record classes.
 *
 * > NOTE: This class is only needed for Java. If you write your business logic using Kotlin, you don't need it. Kotlin has
 * a bit more reflection magic that makes the tricks described in this class unnecessary.
 *
 * For example, you have a Record Class defined as follows:
 * ```
 * record UserMeta(int userId, boolean active) {}
 * ```
 *
 * It's very convenient (and elegant) to get a references to `UserMeta` fields by method reference operator `::`, for example
 * `UserMeta::userId`. Return type will be `Function<UserMeta, Integer>` (in this example) and this is exactly what Kolbasa
 * needs – function that accepts `UserMeta` instance and returns `userId` field value.
 *
 * However, there is a problem – [Function][java.util.function.Function] doesn't allow to understand from which record class
 * field it has been created. It doesn't contain a field name (`userId` in this example), it's just an anonymous function.
 * This is the reason why Kolbasa requires to pass two parameters: field name and reference to the field.
 *
 * I didn't find any elegant way to get the field name from the field reference, so I chose what I think is the simplest and most
 * understandable solution – just ask the developer to specify the field name as a string. The disadvantage of this solution is
 * that if the developer renames the field in the Record Class, then the field name in this string will also need to be changed.
 *
 * Usage
 * ```
 * var userIdField = JavaField.of("userId", UserMeta::userId);
 * var activeField = JavaField.of("active", UserMeta::active);
 *
 * // it literally converts to "userId=123 AND active=true"
 * var filter = Filter.and(
 *    Filter.eq(userIdField, 123),
 *    Filter.eq(activeField, true)
 * );
 *
 * // receives up to 100 messages using the custom filter
 * var messages = consumer.receive(queue, 100, f -> filter);
 * ```
 *
 * @param Meta Kolbasa metadata class for which we create a full description (field name and method reference)
 * @param T field type (`int` for the `userId` field in the example above)
 * @param name Record Class field name (`userId` in the example above)
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
