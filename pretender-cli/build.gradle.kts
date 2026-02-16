plugins {
    id("buildlogic.java-application-conventions")
}

description = "CLI tool for export/import of PretenderDB tables"

application {
    mainClass.set("io.github.pretenderdb.cli.PretenderCli")
}

dependencies {
    implementation(project(":pretender"))
    implementation(project(":database-utils"))

    // AWS SDK (for DynamoDB types)
    implementation(libs.aws.sdk.ddb)

    // JDBI (required by Dagger component)
    implementation(libs.bundles.jdbi)

    // CLI framework
    implementation(libs.picocli)
    annotationProcessor(libs.picocli)

    // Ion format support
    implementation(libs.ion.java)

    // CSV support
    implementation(libs.commons.csv)

    // Jackson
    implementation(libs.jackson.databind)
    implementation(libs.jackson.datatype.jsr310)
    implementation(libs.jackson.datatype.jdk8)

    // Immutables
    implementation(libs.immutables.annotations)
    annotationProcessor(libs.immutables.value)

    // Dagger
    implementation(libs.dagger)
    annotationProcessor(libs.dagger.compiler)

    // Logging
    implementation(libs.slf4j.api)
    runtimeOnly("ch.qos.logback:logback-classic:1.5.32")

    // Testing
    testImplementation(libs.bundles.testing)
    testImplementation(libs.hsqldb)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit.jupiter)
}

tasks.test {
    maxHeapSize = "2g"
}
