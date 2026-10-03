/**
 * Secrets and CI variables the release pipeline needs, all of them read from the environment.
 */
object SettingsProvider {

    const val ARTIFACT_GROUP_ID = "io.github.vgv"
    const val ARTIFACT_NAME = "kolbasa"

    // it should be a so-called "ascii-armored in-memory PGP secret key"
    private const val GPG_SIGNING_KEY_PROPERTY = "GPG_SIGNING_KEY"
    private const val GPG_SIGNING_PASSWORD_PROPERTY = "GPG_SIGNING_PASSWORD"

    // generate a name/password at https://central.sonatype.com/usertoken
    private const val SONATYPE_USERNAME_PROPERTY = "SONATYPE_USERNAME"
    private const val SONATYPE_PASSWORD_PROPERTY = "SONATYPE_PASSWORD"
    private const val GITHUB_HEAD_REF_PROPERTY = "GITHUB_HEAD_REF"

    val gpgSigningKey: String?
        get() = System.getenv(GPG_SIGNING_KEY_PROPERTY)

    val gpgSigningPassword: String?
        get() = System.getenv(GPG_SIGNING_PASSWORD_PROPERTY)

    val sonatypeUsername: String?
        get() = System.getenv(SONATYPE_USERNAME_PROPERTY)

    val sonatypePassword: String?
        get() = System.getenv(SONATYPE_PASSWORD_PROPERTY)

    val githubHeadRef: String?
        get() = System.getenv(GITHUB_HEAD_REF_PROPERTY)

    fun validateGPGSecrets() = require(
        value = !gpgSigningKey.isNullOrBlank() && !gpgSigningPassword.isNullOrBlank(),
        lazyMessage = { "Both $GPG_SIGNING_KEY_PROPERTY and $GPG_SIGNING_PASSWORD_PROPERTY environment variables must not be empty" }
    )

    fun validateSonatypeCredentials() = require(
        value = !sonatypeUsername.isNullOrBlank() && !sonatypePassword.isNullOrBlank(),
        lazyMessage = { "Both $SONATYPE_USERNAME_PROPERTY and $SONATYPE_PASSWORD_PROPERTY environment variables must not be empty" }
    )
}
