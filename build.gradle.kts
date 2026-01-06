/*
 * Root build configuration for PretenderDB
 */

plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

// Configure Nexus publishing for automated OSSRH release
nexusPublishing {
    repositories {
        sonatype {
            nexusUrl.set(uri("https://oss.sonatype.org/service/local/"))
            snapshotRepositoryUrl.set(uri("https://oss.sonatype.org/content/repositories/snapshots/"))

            // Credentials from environment variables or gradle.properties
            username.set(System.getenv("OSSRH_USERNAME") ?: findProperty("ossrhUsername")?.toString())
            password.set(System.getenv("OSSRH_PASSWORD") ?: findProperty("ossrhPassword")?.toString())
        }
    }
}
