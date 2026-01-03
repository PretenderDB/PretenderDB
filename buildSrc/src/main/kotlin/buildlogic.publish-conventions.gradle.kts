plugins {
    // Apply the java Plugin to add support for Java.
    `maven-publish`
    signing
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            from(components["java"])
            pom {
                name = "local-queue"
                description = "LQ: a minimalistic local queue"
                url = "https://github.com/wolpert/local-queue"
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "http://www.apache.org/licenses/LICENSE-2.0.txt"
                    }
                }
                developers {
                    developer {
                        id = "wolpert"
                        name = "Ned Wolpert"
                        email = "ned.wolpert@gmail.com"
                    }
                }
                scm {
                    connection = "scm:git:git://github.com/PretenderDB/PretenderDB.git"
                    developerConnection = "scm:git:ssh://github.com/PretenderDB/PretenderDB.git"
                    url = "https://github.com/PretenderDB/PretenderDB/"
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
    useGpgCmd()
    sign(publishing.publications["mavenJava"])
}
