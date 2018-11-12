plugins {
    kotlin("jvm") version "1.3.0"
    id("org.jetbrains.dokka") version "0.9.16"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.3.0"
}

val dokka by tasks.getting(org.jetbrains.dokka.gradle.DokkaTask::class) {
    outputFormat = "javadoc"
    outputDirectory = "$buildDir/javadoc"
}

val dokkaJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    classifier = "javadoc"
    from(dokka)
}

repositories {
    jcenter()
}

tasks.withType(org.jetbrains.kotlin.gradle.tasks.KotlinCompile::class.java).all {
    kotlinOptions {
        jvmTarget = "1.8"
        javaParameters = true
    }
}

allOpen {
    annotation("org.apache.beam.sdk.coders.DefaultCoder")
}

tasks.withType<Test> {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}

val junit_version = "5.3.1"
val beam_version = "2.8.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    compile(group = "org.apache.beam", name = "beam-sdks-java-core", version = beam_version)

    testCompile(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit_version)
    testCompile(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junit_version)
    testRuntime(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit_version)
    testRuntime(group = "org.apache.beam", name = "beam-runners-direct-java", version = beam_version)
    testCompile(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.9.7")
    testRuntime(group = "ch.qos.logback", name = "logback-classic", version = "1.2.3")
}