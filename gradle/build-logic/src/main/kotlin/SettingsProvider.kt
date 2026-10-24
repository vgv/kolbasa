/**
 * Coordinates of the published artifacts and the CI variables the release pipeline reads from the environment.
 *
 * Secrets are not here: the publishing plugin reads them as Gradle properties, see kolbasa.publishing.gradle.kts
 */
object SettingsProvider {

    val githubHeadRef: String?
        get() = System.getenv(GITHUB_HEAD_REF_PROPERTY)

    const val ARTIFACT_GROUP_ID = "io.github.vgv"

    // Every module published to Maven Central. The module name is its artifact id
    val ARTIFACT_NAMES = listOf("kolbasa", "kolbasa-opentelemetry")

    private const val GITHUB_HEAD_REF_PROPERTY = "GITHUB_HEAD_REF"
}
