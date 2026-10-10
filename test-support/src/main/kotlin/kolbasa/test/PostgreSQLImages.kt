package kolbasa.test

/**
 * The PostgreSQL docker images the project is tested against.
 *
 * The list itself lives next to this file, in `postgresql-test-images.txt`: one image per line, blank
 * lines and `#` comments ignored. It is the single source of truth, read from three places - the
 * library's own test suite, the examples, the benchmarks - and separately by the Gradle build, which
 * generates one `testPg_*` task per line.
 */
object PostgreSQLImages {

    private const val IMAGES_FILE = "/postgresql-test-images.txt"

    // Every image from the list.
    val allImages: Set<String> = requireNotNull(PostgreSQLImages::class.java.getResourceAsStream(IMAGES_FILE)) {
        "$IMAGES_FILE not found on the classpath"
    }.bufferedReader().useLines { lines ->
        lines.map(String::trim)
            .filter { it.isNotEmpty() && !it.startsWith("#") }
            .toSet()
    }.also {
        require(it.isNotEmpty()) {
            "$IMAGES_FILE parsed to 0 images"
        }
    }

    /**
     * The latest patch of every major version, e.g. 14.24, 15.19, 16.15, 17.11, 18.6.
     */
    val latestPerMajor: List<String> = allImages
        .groupBy { version(it).first }
        .map { (_, images) -> images.maxBy { version(it).second } }

    /**
     * The newest image overall
     */
    val newestImage: String = allImages.maxWith(compareBy({ version(it).first }, { version(it).second }))

    /**
     * Chosen once per JVM: an explicit override (that is what the per-version Gradle tasks set),
     * otherwise a random latest-per-major image. Sampling only the latest patches keeps the local Docker
     * image cache bounded to about one per major, while still rotating coverage across majors run to run.
     */
    val randomImage: String = System.getProperty("kolbasa.test.postgresql.image") ?: latestPerMajor.random()

    /**
     * Parses PG image string to (major, minor) pair, for example 'postgres:16.4-alpine' => (16, 4)
     */
    private fun version(image: String): Pair<Int, Int> {
        val (major, minor) = image.substringAfter(':').substringBefore('-').split('.').map(String::toInt)
        return major to minor
    }
}
