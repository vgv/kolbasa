/**
 * Coordinates of the published artifact and the CI variables the release pipeline reads from the environment.
 *
 * Secrets are not here: the publishing plugin reads them as Gradle properties, see kolbasa/build.gradle.kts
 */
object SettingsProvider {

    val githubHeadRef: String?
        get() = System.getenv(GITHUB_HEAD_REF_PROPERTY)

    const val ARTIFACT_GROUP_ID = "io.github.vgv"
    const val ARTIFACT_NAME = "kolbasa"

    private const val GITHUB_HEAD_REF_PROPERTY = "GITHUB_HEAD_REF"
}
