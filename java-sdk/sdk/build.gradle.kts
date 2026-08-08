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

import java.io.File
import java.net.URI
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.time.Duration

val airflowSupervisorSchemaVersion: String by project

plugins {
    `java-library`
    `java-test-fixtures`
    id("airflow-jvm-conventions")
    id("airflow-publish")
    id("org.jetbrains.dokka") version "2.2.0"
    id("org.jetbrains.dokka-javadoc") version "2.2.0"
    id("org.jsonschema2pojo") version "1.2.2"
    kotlin("plugin.serialization") version "2.3.0"
}

val schemaBaseUrl = "https://airflow.staged.apache.org/schemas/supervisor-schema"
val schemaInput = layout.projectDirectory.file("schema/schema.json")
val pointersDir = layout.buildDirectory.dir("schema-pointers/main")
val jsonSchemaPackage = "org.apache.airflow.sdk.execution.comm"
val schemaModelsDir = layout.buildDirectory.dir("generate-resources/main/src/main/java")
val discriminatorDir = layout.buildDirectory.dir("generated-resources/main/src/main/kotlin")
val dagSchemaInput = layout.projectDirectory.file("schema/dag-schema.json")
val dagDslDir = layout.buildDirectory.dir("generated-resources/dsl/src/main/kotlin")

dependencies {
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.9.8")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")

    implementation("com.fasterxml.jackson.core:jackson-annotations:2.21")
    implementation("com.fasterxml.jackson.core:jackson-core:2.21.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.0")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("io.ktor:ktor-network:3.3.3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
    implementation("org.msgpack:msgpack-core:0.9.11")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.11")

    testImplementation(kotlin("test"))
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

abstract class SyncSupervisorSchemaTask : DefaultTask() {
    @get:Input
    abstract val schemaVersion: Property<String>

    @get:Input
    abstract val baseUrl: Property<String>

    @get:Internal
    abstract val schemaFile: RegularFileProperty

    private fun apiVersionOf(file: File): String =
        if (file.exists()) {
            com.fasterxml.jackson.databind
                .ObjectMapper()
                .readTree(file)
                .path("api_version")
                .asText()
        } else {
            ""
        }

    @TaskAction
    fun sync() {
        val file = schemaFile.get().asFile
        val version = schemaVersion.get()
        if (apiVersionOf(file) == version) {
            logger.lifecycle("Supervisor Schema is up-to-date (api_version=$version).")
            return
        }
        val url = "${baseUrl.get()}/$version.json"
        logger.lifecycle("Refreshing Supervisor Schema with $url")
        file.parentFile.mkdirs()
        val tempTarget = Files.createTempFile(file.parentFile.toPath(), "schema", ".json")
        try {
            val connection =
                URI(url).toURL().openConnection().apply {
                    // Timeout values are arbitrary.
                    connectTimeout = 30_000
                    readTimeout = 30_000
                }
            connection.getInputStream().use { input ->
                Files.copy(input, tempTarget, StandardCopyOption.REPLACE_EXISTING)
            }
            val downloaded = apiVersionOf(tempTarget.toFile())
            if (downloaded != version) {
                throw GradleException("Schema declares api_version='$downloaded' but expected '$version' ($url)")
            }
            Files.move(tempTarget, file.toPath(), StandardCopyOption.REPLACE_EXISTING)
        } finally {
            Files.deleteIfExists(tempTarget)
        }
    }
}

// Keep the vendored Dag serialization schema in sync with the monorepo copy.
// The vendored file makes standalone (source-release) builds work; in-repo
// builds refresh it from airflow-core, and a prek hook guards against drift.
abstract class SyncDagSchemaTask : DefaultTask() {
    @get:Internal
    abstract val sourceFile: RegularFileProperty

    @get:Internal
    abstract val targetFile: RegularFileProperty

    @TaskAction
    fun sync() {
        val src = sourceFile.get().asFile
        if (!src.exists()) {
            logger.lifecycle("Monorepo serialization schema not present; keeping vendored dag-schema.json.")
            return
        }
        val dst = targetFile.get().asFile
        if (dst.exists() && dst.readText() == src.readText()) {
            logger.lifecycle("Vendored dag-schema.json is up-to-date.")
            return
        }
        logger.lifecycle("Refreshing vendored dag-schema.json from ${src.path}")
        src.copyTo(dst, overwrite = true)
    }
}

// Generate the Dag-authoring DSL surface from the Dag serialization schema:
//
//  - org.apache.airflow.sdk.Builder and its nested Dag / Task annotations,
//    whose configuration attributes mirror the scalar keys of the schema's
//    "dag" and "operator" definitions (the annotation processor lowers
//    explicitly-set attributes into DagDef.config / TaskDef.config calls), and
//  - org.apache.airflow.sdk.internal.SchemaFields, the key -> type table that
//    DagDef.config / TaskDef.config validate against at registration time.
//
// Field selection mirrors the Go SDK's TaskSpec generator: scalar properties
// only (string/integer/number/boolean plus timedelta/datetime refs),
// serializer-owned keys skipped ("_"-prefixed, schema-required, "has_on_"
// callbacks), and a documented exclusion list for Python-only concerns. An
// exclusion entry that stops matching an eligible key fails generation, so
// the list cannot go stale.
abstract class GenerateDagDslTask : DefaultTask() {
    @get:InputFile
    abstract val schemaFile: RegularFileProperty

    @get:OutputDirectory
    abstract val targetDirectory: DirectoryProperty

    private data class DslField(
        val key: String,
        val attribute: String,
        val fieldType: String,
        val attrType: String,
        val attrDefault: String,
        val defaultJson: String?,
        val doc: String,
    )

    private fun camelCase(key: String): String =
        key
            .split('_')
            .filter { it.isNotEmpty() }
            .mapIndexed { i, seg ->
                if (i == 0) seg else seg.replaceFirstChar(Char::uppercase)
            }.joinToString("")

    private fun quote(s: String): String = "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

    private fun resolveField(
        key: String,
        prop: com.fasterxml.jackson.databind.JsonNode,
        typeOverride: String?,
    ): DslField? {
        val ref = prop.path("\$ref").asText("").substringAfterLast('/')
        val schemaType =
            when {
                ref == "timedelta" -> "timedelta"
                ref == "datetime" -> "datetime"
                ref.isNotEmpty() -> return null
                prop.path("type").isTextual -> prop.path("type").asText()
                else -> return null
            }
        val default = prop.path("default")
        val defaultJson = if (default.isMissingNode || default.isNull) null else default.toString()
        val attribute = camelCase(key)
        return when (schemaType) {
            "string" ->
                DslField(
                    key,
                    attribute,
                    "STRING",
                    "String",
                    quote(default.asText("")),
                    defaultJson,
                    "Schema key `$key`.",
                )
            "boolean" ->
                DslField(
                    key,
                    attribute,
                    "BOOLEAN",
                    "Boolean",
                    default.asBoolean(false).toString(),
                    defaultJson,
                    "Schema key `$key`.",
                )
            "integer", "number" ->
                if (typeOverride == "Double") {
                    DslField(
                        key,
                        attribute,
                        "NUMBER",
                        "Double",
                        if (defaultJson != null) default.asDouble().toString() else "-1.0",
                        defaultJson,
                        "Schema key `$key`.",
                    )
                } else {
                    DslField(
                        key,
                        attribute,
                        "INTEGER",
                        "Int",
                        if (defaultJson != null) default.asInt().toString() else "-1",
                        defaultJson,
                        "Schema key `$key`." + if (defaultJson == null) " Negative means unset." else "",
                    )
                }
            "timedelta" ->
                DslField(
                    key,
                    attribute,
                    "TIMEDELTA",
                    "String",
                    if (defaultJson != null) {
                        quote(Duration.ofSeconds(default.asLong()).toString())
                    } else {
                        quote("")
                    },
                    defaultJson,
                    "Schema key `$key`; an ISO-8601 duration such as `\"PT5M\"`. Empty means unset.",
                )
            "datetime" ->
                DslField(
                    key,
                    attribute,
                    "DATETIME",
                    "String",
                    quote(""),
                    defaultJson,
                    "Schema key `$key`; an ISO-8601 date-time such as `\"2026-01-01T00:00:00Z\"`. Empty means unset.",
                )
            "array" ->
                if (prop.path("items").path("type").asText("") == "string" || key == "tags") {
                    DslField(
                        key,
                        attribute,
                        "STRING_ARRAY",
                        "Array<String>",
                        "[]",
                        defaultJson,
                        "Schema key `$key`.",
                    )
                } else {
                    null
                }
            else -> null
        }
    }

    @TaskAction
    fun generate() {
        // Python-only "operator" keys deliberately not exposed, mirroring the
        // Go SDK's TaskSpec generator exclusion list.
        val excludedTaskKeys =
            setOf(
                "doc",
                "doc_json",
                "doc_yaml",
                "doc_rst",
                "allow_nested_operators",
                "multiple_outputs",
                "start_from_trigger",
                "is_setup",
                "is_teardown",
                "on_failure_fail_dagrun",
            )
        // Dag-level keys exposed for configuration, mirroring the Go SDK's
        // hand-curated DagSpec field list. "schedule" is virtual: the schema
        // models it as the serializer-owned "timetable" object.
        val dagAllowlist =
            listOf(
                "description",
                "dag_display_name",
                "doc_md",
                "start_date",
                "end_date",
                "dagrun_timeout",
                "tags",
                "max_active_tasks",
                "max_active_runs",
                "max_consecutive_failed_dag_runs",
                "catchup",
                "fail_fast",
                "render_template_as_native_obj",
                "disable_bundle_versioning",
                "is_paused_upon_creation",
            )

        val root =
            com.fasterxml.jackson.databind
                .ObjectMapper()
                .readTree(schemaFile.get().asFile)
        val dagProps = root.path("definitions").path("dag").path("properties")
        val operator = root.path("definitions").path("operator")
        val operatorRequired = operator.path("required").map { it.asText() }.toSet()

        val dagFields =
            buildList {
                add(
                    DslField(
                        "schedule",
                        "schedule",
                        "STRING",
                        "String",
                        quote(""),
                        null,
                        "`\"@once\"`, `\"@continuous\"`, a cron expression, or empty for no schedule.",
                    ),
                )
                dagAllowlist.forEach { key ->
                    val prop = dagProps.path(key)
                    if (prop.isMissingNode) {
                        throw GradleException("Dag allowlist key '$key' is missing from the schema; update the allowlist")
                    }
                    add(
                        resolveField(key, prop, null)
                            ?: throw GradleException("Dag allowlist key '$key' is not a scalar the DSL can express"),
                    )
                }
            }

        val excludedSeen = mutableSetOf<String>()
        val taskFields =
            buildList {
                operator.path("properties").fields().forEach { (key, prop) ->
                    val serializerOwned =
                        key.startsWith("_") || key in operatorRequired || key.startsWith("has_on_")
                    if (serializerOwned) return@forEach
                    if (key in excludedTaskKeys) {
                        excludedSeen += key
                        return@forEach
                    }
                    // retry_exponential_backoff is "number" with an integral
                    // default, but Python declares it float (a backoff
                    // multiplier), so the mechanical mapping would pick Int.
                    val override = if (key == "retry_exponential_backoff") "Double" else null
                    resolveField(key, prop, override)?.let {
                        if (it.fieldType != "STRING_ARRAY") add(it)
                    }
                }
            }
        (excludedTaskKeys - excludedSeen).takeIf { it.isNotEmpty() }?.let {
            throw GradleException("Excluded task keys match no eligible schema property; remove or fix: $it")
        }
        // "id"/"to" name the annotations' structural attributes, so a schema
        // key camel-casing to either would silently shadow them.
        (dagFields + taskFields).firstOrNull { it.attribute == "id" || it.attribute == "to" }?.let {
            throw GradleException("Schema key '${it.key}' collides with a structural annotation attribute")
        }

        val outDir = targetDirectory.get().asFile.also { it.deleteRecursively() }

        fun attrLines(fields: List<DslField>): String =
            fields.joinToString("\n") { f ->
                "    /** ${f.doc} */\n    val ${f.attribute}: ${f.attrType} = ${f.attrDefault},"
            }

        outDir.resolve("org/apache/airflow/sdk").apply { mkdirs() }.resolve("Builder.kt").writeText(
            """
            |package org.apache.airflow.sdk
            |
            |// Generated from the Dag serialization schema (sdk/schema/dag-schema.json); do not edit by hand.
            |
            |/**
            | * Container for the annotation-based Dag-authoring API.
            | *
            | * This class is not instantiated directly. Its nested annotations drive the
            | * `BuilderProcessor` annotation processor in the :processor project, which
            | * generates a `<Class>Builder` class for each class annotated with
            | * [Builder.Dag], plus a `<Class>Ref` twin class when the Dag class declares
            | * a [Wiring] method.
            | *
            | * Example:
            | *
            | * ```java
            | * @Builder.Dag(id = "my_pipeline", schedule = "@daily")
            | * public class MyPipeline {
            | *
            | *     @Builder.Task(id = "extract", retries = 2)
            | *     public long extract(Client client) { ... }
            | *
            | *     @Builder.Task(id = "transform")
            | *     public long transform(Client client, long extracted) { ... }
            | *
            | *     @Wiring
            | *     static void depends(MyPipelineRef f) {
            | *         f.transform(f.extract());
            | *     }
            | * }
            | * ```
            | *
            | * A task method's data parameters — everything other than the injected
            | * [Client] and [Context] — receive, by position, the arguments the Python
            | * `@task.stub` call site bound, falling back to the inputs the [Wiring]
            | * method fed them. Keyword arguments bind by name instead through a single
            | * [TaskInput] bundle parameter.
            | *
            | * The processor generates `MyPipelineBuilder.build()`, which returns a
            | * fully wired-up [DagDef] ready to add to a [Bundle].
            | */
            |class Builder internal constructor() {
            |  /**
            |   * Annotation to automate a Dag-builder pattern.
            |   *
            |   * When applied on a class Foo, this generates a FooBuilder class with a
            |   * static build method to create the Dag structure automatically.
            |   *
            |   * Configuration attributes mirror the Dag serialization schema; only
            |   * attributes written explicitly at the use site are applied, so the
            |   * scheduler's own defaults win for everything left out.
            |   */
            |  @Target(AnnotationTarget.CLASS)
            |  @MustBeDocumented
            |  annotation class Dag(
            |    /** Dag ID. Empty derives it from the annotated class's name. */
            |    val id: String = "",
            |    /** Name of the generated builder class. Empty derives `<Class>Builder`. */
            |    val to: String = "",
            |${attrLines(dagFields)}
            |  )
            |
            |  /**
            |   * Annotation to automate task definition in a Dag-builder pattern.
            |   *
            |   * Configuration attributes mirror the Dag serialization schema; only
            |   * attributes written explicitly at the use site are applied.
            |   */
            |  @Target(AnnotationTarget.FUNCTION)
            |  @MustBeDocumented
            |  annotation class Task(
            |    /** Task ID. Empty derives it from the annotated function's name. */
            |    val id: String = "",
            |${attrLines(taskFields)}
            |  )
            |}
            |
            """.trimMargin(),
        )

        fun tableLines(fields: List<DslField>): String =
            fields.joinToString("\n") { f ->
                val defaultRepr = f.defaultJson?.let { quote(it) } ?: "null"
                listOf(
                    "      \"${f.key}\" to",
                    "        Field(",
                    "          \"${f.key}\",",
                    "          \"${f.attribute}\",",
                    "          FieldType.${f.fieldType},",
                    "          $defaultRepr,",
                    "        ),",
                ).joinToString("\n")
            }

        outDir.resolve("org/apache/airflow/sdk/internal").apply { mkdirs() }.resolve("SchemaFields.kt").writeText(
            """
            |package org.apache.airflow.sdk.internal
            |
            |// Generated from the Dag serialization schema (sdk/schema/dag-schema.json); do not edit by hand.
            |
            |/**
            | * Configuration keys accepted by `DagDef.config` and `TaskDef.config`,
            | * keyed by Dag serialization schema property name. Public so that the
            | * annotation processor can lower `@Builder.Dag` / `@Builder.Task`
            | * attributes onto the same tables; not user-facing API.
            | */
            |object SchemaFields {
            |  val DAG: Map<String, Field> =
            |    linkedMapOf(
            |${tableLines(dagFields)}
            |    )
            |
            |  val TASK: Map<String, Field> =
            |    linkedMapOf(
            |${tableLines(taskFields)}
            |    )
            |}
            |
            """.trimMargin(),
        )
    }
}

val syncSupervisorSchema by tasks.registering(SyncSupervisorSchemaTask::class) {
    description = "Ensure the bundled Supervisor Schema is up-to-date with the Gradle property."
    schemaVersion = airflowSupervisorSchemaVersion
    baseUrl = schemaBaseUrl
    schemaFile = schemaInput
}

tasks.register<GenerateDiscriminatorTask>("generateDiscriminator") {
    dependsOn(syncSupervisorSchema)
    description = "Generate Discriminator to wire type strings to model classes"
    schemaFile = schemaInput
    modelPackage = jsonSchemaPackage
    targetDirectory = discriminatorDir
}

tasks.register<GeneratePointersTask>("generatePointers") {
    dependsOn(syncSupervisorSchema)
    description = "Generate pointer files for jsonSchema2Pojo"
    schemaFile = schemaInput
    targetDirectory = pointersDir
}

val syncDagSchema by tasks.registering(SyncDagSchemaTask::class) {
    description = "Refresh the vendored Dag serialization schema from the monorepo copy when present."
    sourceFile = layout.projectDirectory.file("../../airflow-core/src/airflow/serialization/schema.json")
    targetFile = dagSchemaInput
}

tasks.register<GenerateDagDslTask>("generateDagDsl") {
    dependsOn(syncDagSchema)
    description = "Generate the Builder.Dag/Builder.Task annotations and SchemaFields from the Dag serialization schema"
    schemaFile = dagSchemaInput
    targetDirectory = dagDslDir
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
    targetDirectory = schemaModelsDir.get().asFile
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
        java.srcDir(tasks.named("generateJsonSchema2Pojo").map { schemaModelsDir })
        kotlin.srcDir(tasks.named("generateDiscriminator").map { discriminatorDir })
        kotlin.srcDir(tasks.named("generateDagDsl").map { dagDslDir })
    }
}

dokka {
    moduleVersion.set(project.version.toString())
    dokkaSourceSets.configureEach {
        // Suppress everything in 'execution' since it's implementation detail.
        perPackageOption {
            matchingRegex = """org\.apache\.airflow\.sdk\.execution.*"""
            suppress.set(true)
        }
        // 'internal' is public only for the annotation processor's benefit.
        perPackageOption {
            matchingRegex = """org\.apache\.airflow\.sdk\.internal.*"""
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

tasks.named("compileKotlin") {
    dependsOn("generateJsonSchema2Pojo")
}

tasks.named("runKtlintCheckOverMainSourceSet") {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator", "generateDagDsl")
}

tasks.matching { it.name.startsWith("dokkaGenerate") }.configureEach {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator", "generateDagDsl")
}

tasks.withType<Jar> {
    dependsOn("generateJsonSchema2Pojo", "generateDiscriminator", "generateDagDsl")
    manifest {
        attributes(
            "Airflow-Supervisor-Schema-Version" to airflowSupervisorSchemaVersion,
        )
    }
}

tasks.withType<Test> {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "airflow-sdk"
            from(components["java"])
            // test-fixtures are not published to Maven Central.
            suppressPomMetadataWarningsFor("testFixturesApiElements")
            suppressPomMetadataWarningsFor("testFixturesRuntimeElements")
            artifact(javadocJar)
            pom {
                name = "Apache Airflow Java SDK"
                description = "Java SDK for implementing Apache Airflow task logic on the JVM."
            }
        }
    }
}
