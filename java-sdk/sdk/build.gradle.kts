/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

buildscript {
    repositories {
        mavenCentral()
    }
}

val airflowSupervisorSchemaVersion: String by project

val sdkArtifact = "airflow-sdk"
val sdkVersion: String by project

// Full Maven coordinate: org.apache.airflow:airflow-sdk:<version>
// artifactId is set explicitly on the MavenPublication below.
group = "org.apache.airflow"
version = sdkVersion

plugins {
    `java-library`
    `maven-publish`
    signing
    kotlin("plugin.serialization") version "2.3.0"
    id("org.jetbrains.dokka") version "2.2.0"
    id("org.jetbrains.dokka-javadoc") version "2.2.0"
    id("org.jsonschema2pojo") version "1.2.2"
}

// TODO: Use a hosted file instead.
val schemaInput = rootProject.file("../task-sdk/src/airflow/sdk/execution_time/schema/schema.json")
val pointersDir = layout.buildDirectory.dir("schema-pointers/main")
val jsonSchemaPackage = "org.apache.airflow.sdk.execution.comm"
val discriminatorDir = layout.buildDirectory.dir("generated-resources/main/src/main/kotlin")

dependencies {
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.9.8")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")

    implementation("com.fasterxml.jackson.core:jackson-annotations:2.21")
    implementation("com.fasterxml.jackson.core:jackson-core:2.21.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.0")
    implementation("com.squareup:javapoet:1.13.0")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("io.ktor:ktor-network:3.3.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
    implementation("org.msgpack:msgpack-core:0.9.11")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.11")

    testImplementation(kotlin("test"))
    testImplementation("com.google.testing.compile:compile-testing:0.23.0")
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}

// jsonSchema2Pojo does not accept the single JSON Schema file directly.
// It needs a list of schema files, each containing a "$ref" pointer to
// a $def. This task walks over all $ref items in the Supervisor Schema
// file and generates one JSON file with $ref for each one.
abstract class GeneratePointersTask : DefaultTask() {
    @get:InputFile
    abstract val schemaFile: RegularFileProperty

    @get:OutputDirectory
    abstract val targetDirectory: DirectoryProperty

    @TaskAction
    fun generate() {
        val srcFile = schemaFile.get().asFile
        val outDir =
            targetDirectory.get().asFile.also {
                it.deleteRecursively()
                it.mkdirs()
            }

        srcFile.copyTo(outDir.resolve(srcFile.name), overwrite = true)

        com.fasterxml.jackson.databind
            .ObjectMapper()
            .readTree(srcFile)
            .path("\$defs")
            .fieldNames()
            .forEach { type ->
                outDir
                    .resolve("$type.json")
                    .writeText("""{"${"$"}ref": "${srcFile.name}#/${"$"}defs/$type"}""" + "\n")
            }
    }
}

// Generate a name->class mapping of known jsonSchema2Pojo models.
// This is needed for type discrimination in the MessagePack decoder.
abstract class GenerateDiscriminatorTask : DefaultTask() {
    @get:Input
    abstract val modelPackage: Property<String>

    @get:InputFile
    abstract val schemaFile: RegularFileProperty

    @get:OutputDirectory
    abstract val targetDirectory: DirectoryProperty

    @TaskAction
    fun generate() {
        data class Entry(
            val wireType: String,
            val className: String,
        )

        val entries =
            buildList {
                com.fasterxml.jackson.databind
                    .ObjectMapper()
                    .readTree(schemaFile.get().asFile)
                    .path("\$defs")
                    .fields()
                    .forEach { (className, def) ->
                        val constNode = def.path("properties").path("type").path("const")
                        if (!constNode.isMissingNode && !constNode.isNull) {
                            add(Entry(constNode.asText(), className))
                        }
                    }
            }.sortedBy { it.className }

        val outDir =
            targetDirectory
                .get()
                .asFile
                .resolve("org/apache/airflow/sdk/execution/comm")
                .also { it.mkdirs() }

        outDir.resolve("Discriminator.kt").writeText(
            buildString {
                appendLine("package ${modelPackage.get()}")
                appendLine()
                appendLine("// Maps every wire `type` discriminator string to its generated model class.")
                appendLine("// Generated from the Supervisor Schema; do not edit by hand.")
                appendLine("internal object Discriminator {")
                appendLine("  val types: Map<String, Class<*>> =")
                appendLine("    mapOf(")
                entries.forEach { appendLine("      \"${it.wireType}\" to ${it.className}::class.java,") }
                appendLine("    )")
                appendLine("}")
            },
        )
    }
}

tasks.register<GenerateDiscriminatorTask>("generateDiscriminator") {
    description = "Generate Discriminator to wire type strings to model classes"
    schemaFile = layout.file(provider { schemaInput })
    modelPackage = jsonSchemaPackage
    targetDirectory = discriminatorDir
}

tasks.register<GeneratePointersTask>("generatePointers") {
    description = "Generate pointer files for jsonSchema2Pojo"
    schemaFile = layout.file(provider { schemaInput })
    targetDirectory = pointersDir
}

val javadocJar by tasks.registering(Jar::class) {
    description = "Assembles Javadoc JAR from Dokka output"
    group = JavaBasePlugin.DOCUMENTATION_GROUP
    archiveClassifier.set("javadoc")
    from(tasks.named("dokkaGeneratePublicationJavadoc"))
}

jsonSchema2Pojo {
    setSource(listOf(pointersDir.get().asFile))
    targetPackage = jsonSchemaPackage
    targetDirectory =
        layout.buildDirectory
            .dir("generate-resources/main/src/main/java")
            .get()
            .asFile
    setAnnotationStyle("jackson")
    dateTimeType = "java.time.OffsetDateTime"
    generateBuilders = false
    includeAdditionalProperties = false
    includeConstructors = false
    includeHashcodeAndEquals = true
    includeJsr305Annotations = true
    includeToString = true
    initializeCollections = true
    removeOldOutput = true
    useTitleAsClassname = true
}

sourceSets {
    main {
        java.srcDir(layout.buildDirectory.dir("generate-resources/main/src/main/java"))
        kotlin.srcDir(discriminatorDir)
    }
}

dokka {
    moduleVersion.set(sdkVersion)
    dokkaSourceSets.configureEach {
        // Suppress everything in 'execution' since it's implementation detail.
        perPackageOption {
            matchingRegex = """org\.apache\.airflow\.sdk\.execution.*"""
            suppress.set(true)
        }
    }
}

java {
    withSourcesJar() // Required by Maven Central.
    // Do NOT call withJavadocJar(); we use Dokka to generate documentation. See javadocJar above.
}

tasks.named("generateJsonSchema2Pojo") {
    dependsOn("generatePointers")
}

tasks.named("compileJava") {
    dependsOn("generateJsonSchema2Pojo")
}

tasks.named("compileKotlin") {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator")
}

tasks.named("runKtlintCheckOverMainSourceSet") {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator")
}

tasks.matching { it.name.startsWith("dokkaGenerate") }.configureEach {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator")
}

tasks.withType<Jar> {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator")
    manifest {
        attributes(
            "Airflow-Supervisor-Schema-Version" to airflowSupervisorSchemaVersion,
        )
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

private fun getProperty(name: String) = providers.gradleProperty(name).orNull

private fun getProperty(
    name: String,
    env: String,
): String? = getProperty(name) ?: System.getenv(env)

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = sdkArtifact
            from(components["java"])
            artifact(javadocJar)
            pom {
                name = "Apache Airflow Java SDK"
                description = "Java SDK for implementing Apache Airflow task logic on the JVM."
                url = "https://airflow.apache.org"

                organization {
                    name = "The Apache Software Foundation"
                    url = "https://www.apache.org/"
                }
                licenses {
                    license {
                        name = "The Apache License, Version 2.0"
                        url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                        distribution = "repo"
                    }
                }
                scm {
                    connection = "scm:git:https://gitbox.apache.org/repos/asf/airflow.git"
                    developerConnection = "scm:git:https://gitbox.apache.org/repos/asf/airflow.git"
                    url = "https://github.com/apache/airflow"
                }
            }
        }
    }

    repositories {
        maven {
            name = "mavenRepo"
            url =
                uri(
                    getProperty("mavenUrl")
                        ?: if (sdkVersion.endsWith("-SNAPSHOT")) {
                            "https://repository.apache.org/content/repositories/snapshots/"
                        } else {
                            "https://repository.apache.org/service/local/staging/deploy/maven2/"
                        },
                )
            getProperty("mavenUsername", "ASF_NEXUS_USERNAME").let { user ->
                credentials {
                    username = user
                    password = getProperty("mavenPassword", "ASF_NEXUS_PASSWORD")
                }
            }
        }
    }
}

signing {
    getProperty("signing.key", "SIGNING_KEY").let { secretKey ->
        val password = getProperty("signing.password", "SIGNING_PASSWORD")
        useInMemoryPgpKeys(secretKey, password)
        sign(publishing.publications["mavenJava"])
    }
}
