plugins {
    // Apply the java Plugin to add support for Java.
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])

            // Set artifact coordinates
            groupId = project.group.toString()
            artifactId = project.name
            version = project.version.toString()

            pom {
                // Module-specific name and description
                // Override these in individual module build.gradle.kts if needed
                name.set(project.findProperty("pomName")?.toString() ?: project.name)
                description.set(project.findProperty("description")?.toString()
                    ?: "Part of PretenderDB - DynamoDB-compatible library using SQL databases")
                url.set("https://github.com/PretenderDB/PretenderDB")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("http://www.apache.org/licenses/LICENSE-2.0.txt")
                    }
                }

                developers {
                    developer {
                        id.set("wolpert")
                        name.set("Ned Wolpert")
                        email.set("ned.wolpert@gmail.com")
                    }
                }

                scm {
                    connection.set("scm:git:git://github.com/PretenderDB/PretenderDB.git")
                    developerConnection.set("scm:git:ssh://git@github.com:PretenderDB/PretenderDB.git")
                    url.set("https://github.com/PretenderDB/PretenderDB")
                }
            }
        }
    }
    repositories {
        maven {
            val releasesRepoUrl = "https://oss.sonatype.org/service/local/staging/deploy/maven2"
            val snapshotsRepoUrl = "https://oss.sonatype.org/content/repositories/snapshots"
            url = uri(if (version.toString().endsWith("SNAPSHOT")) snapshotsRepoUrl else releasesRepoUrl)
            name = "ossrh"
            credentials(PasswordCredentials::class)
        }
    }
}

signing {
    // Only sign if credentials are available (not required for local builds)
    val signingRequired = project.hasProperty("signing.gnupg.keyName")
        || System.getenv("GPG_KEY_ID") != null

    isRequired = signingRequired && !version.toString().endsWith("SNAPSHOT")

    useGpgCmd()
    sign(publishing.publications["mavenJava"])
}

// Task to verify publication configuration
tasks.register("verifyPublishConfig") {
    doLast {
        println("Group: ${project.group}")
        println("Artifact: ${project.name}")
        println("Version: ${project.version}")
        println("Is SNAPSHOT: ${version.toString().endsWith("SNAPSHOT")}")
        println("Repository: ${if (version.toString().endsWith("SNAPSHOT")) "snapshots" else "releases"}")
    }
}
