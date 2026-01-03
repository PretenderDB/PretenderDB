plugins {
    id("buildlogic.java-library-conventions")
}

dependencies {
    // Pretender module to test
    testImplementation(project(":pretender"))
    testImplementation(project(":database-utils"))

    // AWS DynamoDB SDK
    testImplementation(libs.aws.sdk.ddb)

    // DynamoDB Local for testing
    testImplementation("com.amazonaws:DynamoDBLocal:2.5.2")

    // SQLite for DynamoDB Local (required dependency)
    testImplementation("com.almworks.sqlite4java:sqlite4java:1.0.392")

    // Testcontainers for PostgreSQL
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.pgjdbc)

    // HSQLDB for in-memory testing
    testImplementation(libs.hsqldb)

    // JDBI
    testImplementation(libs.jdbi.core)
    testImplementation(libs.jdbi.sqlobject)

    // Jackson for JSON
    testImplementation(libs.jackson.databind)

    // Testing - use the testing bundle which includes junit, assertj, mockito
    testImplementation(libs.bundles.testing)

    // SLF4J logging (logback is a runtime dependency usually)
    testImplementation(libs.slf4j.api)
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.16")

    // Immutables
    testCompileOnly(libs.immutables.annotations)
    testAnnotationProcessor(libs.immutables.value)
}

tasks.test {
    useJUnitPlatform()

    // Set system property for DynamoDB Local native libraries
    // DynamoDB Local will look for native libs in this directory
    systemProperty("sqlite4java.library.path", "native-libs")

    // Increase heap for testcontainers
    maxHeapSize = "2g"

    // Increase test timeout for container startup
    testLogging {
        events("passed", "skipped", "failed")
        showStandardStreams = false
    }
}

// Task to extract SQLite native libraries from dependency JARs
tasks.register<Copy>("extractNativeLibs") {
    from({
        configurations.testRuntimeClasspath.get()
            .filter { it.name.contains("sqlite4java") && it.name.endsWith(".jar") }
            .map { zipTree(it) }
    })
    into(layout.buildDirectory.dir("native-libs"))
    include("**/*.so", "**/*.dll", "**/*.dylib", "**/*.jnilib")
    includeEmptyDirs = false

    // Flatten the directory structure
    eachFile {
        path = name
    }
}

tasks.test {
    dependsOn("extractNativeLibs")
    // Update to use the build directory
    systemProperty("sqlite4java.library.path", layout.buildDirectory.dir("native-libs").get().asFile.absolutePath)
}
