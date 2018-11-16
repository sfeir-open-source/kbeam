/*
 * Copyright 2018 SFEIR S.A.S.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

/*
 * Copyright 2018 SFEIR S.A.S.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
import com.sfeir.open.kbeam.generator.ParDosGenerator

plugins {
    kotlin("jvm") version "1.3.0"
    `maven-publish`
    id("org.jetbrains.dokka") version "0.9.16"
    id("org.jetbrains.kotlin.plugin.allopen") version "1.3.0"
}

version = System.getenv().getOrDefault("KBEAM_VERSION", "SNAPSHOT")
group = "com.sfeir.open"

sourceSets["main"].java.srcDir("src/main/generated")

val dokka by tasks.getting(org.jetbrains.dokka.gradle.DokkaTask::class) {
    outputFormat = "html"
    outputDirectory = "$buildDir/javadoc"
}

val docJar by tasks.creating(Jar::class) {
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    description = "Assembles Kotlin docs with Dokka"
    classifier = "javadoc"
    from(dokka)
}

val sourcesJar by tasks.registering(Jar::class) {
    classifier = "sources"
    from(sourceSets["main"].allSource)
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

publishing {
    publications {
        publications {
            register("mavenJava", MavenPublication::class) {
                from(components["java"])
                artifact(sourcesJar.get())
                artifact(docJar)
            }
        }
    }
    repositories {
        maven {
            name = "test"
            url = uri("$buildDir/repo")
        }
    }
}

task("generateKotlinSource") {
    ParDosGenerator.getParDos(projectDir.absolutePath + "/src/main/generated")
}

val junit_version = "5.3.1"
val beam_version = "2.8.0"
val kryo_version = "5.0.0-RC1"
dependencies {
    implementation(kotlin("stdlib-jdk8"))
    compile(group = "org.apache.beam", name = "beam-sdks-java-core", version = beam_version)
    compile(group = "com.esotericsoftware", name = "kryo", version = kryo_version)

    testCompile(group = "org.junit.jupiter", name = "junit-jupiter-api", version = junit_version)
    testCompile(group = "org.junit.jupiter", name = "junit-jupiter-params", version = junit_version)
    testRuntime(group = "org.junit.jupiter", name = "junit-jupiter-engine", version = junit_version)
    testRuntime(group = "org.apache.beam", name = "beam-runners-direct-java", version = beam_version)
    testCompile(group = "com.fasterxml.jackson.core", name = "jackson-databind", version = "2.9.7")
    testRuntime(group = "ch.qos.logback", name = "logback-classic", version = "1.2.3")
}