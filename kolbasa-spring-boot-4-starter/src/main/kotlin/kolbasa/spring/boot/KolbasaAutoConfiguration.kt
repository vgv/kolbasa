package kolbasa.spring.boot

import org.springframework.boot.autoconfigure.AutoConfiguration

/**
 * Spring Boot auto-configuration of kolbasa.
 *
 * Spring Boot finds it on its own, so adding the starter to the dependencies is enough. To turn it off, exclude it:
 * `@SpringBootApplication(exclude = [KolbasaAutoConfiguration::class])`.
 */
@AutoConfiguration
class KolbasaAutoConfiguration
