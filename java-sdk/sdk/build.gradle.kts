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

import org.jetbrains.kotlin.gradle.tasks.KotlinCompile
import org.jlleitschuh.gradle.ktlint.tasks.KtLintCheckTask
import java.time.ZonedDateTime

buildscript {
    repositories {
        mavenCentral()
    }
}

val airflowExecApiVersion: String by project

plugins {
    kotlin("plugin.serialization") version "2.3.0"
    id("org.openapi.generator") version "7.19.0"
}

val constantsDir = layout.buildDirectory.dir("generate-constants/main/src/main/kotlin")

dependencies {
    compileOnly("com.github.spotbugs:spotbugs-annotations:4.9.8")
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    compileOnly("org.apache.oltu.oauth2:org.apache.oltu.oauth2.client:1.0.1")

    implementation("com.fasterxml.jackson.core:jackson-annotations:2.21")
    implementation("com.fasterxml.jackson.core:jackson-core:2.21.1")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.21.0")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.21.0")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.21.0")
    implementation("com.squareup.retrofit2:converter-jackson:3.0.0")
    implementation("com.squareup.retrofit2:converter-scalars:3.0.0")
    implementation("com.squareup.retrofit2:retrofit:3.0.0")
    implementation("com.xenomachina:kotlin-argparser:2.0.7")
    implementation("io.ktor:ktor-network:3.3.3")
    implementation("javax.ws.rs:javax.ws.rs-api:2.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.10.0")
    implementation("org.msgpack:msgpack-core:0.9.11")
    implementation("org.msgpack:jackson-dataformat-msgpack:0.9.11")

    testImplementation(kotlin("test"))
    testImplementation("com.squareup.okhttp3:mockwebserver:4.12.0")
}

openApiGenerate {
    generatorName = "java"
    library = "retrofit2"

    remoteInputSpec = "https://airflow.apache.org/schemas/execution-api/$airflowExecApiVersion.json"
    apiPackage = "org.apache.airflow.sdk.execution.api.route"
    modelPackage = "org.apache.airflow.sdk.execution.api.model"
    invokerPackage = "org.apache.airflow.sdk.execution.api.client"

    generateApiDocumentation = false
    generateApiTests = false
    generateModelDocumentation = false
    generateModelTests = false

    // The spec on arbitrary mapping (e.g. 'extra') causes the OpenAPI generator to output JsonValue.
    // We should probably fix the spec instead, but this should work before that.
    // Suggested fix:
    //   type: object
    //   additionalProperties: true
    schemaMappings.put("JsonValue", "java.lang.Object")

    additionalProperties =
        mapOf(
            "dateLibrary" to "java8",
            "openApiNullable" to false,
            "serializationLibrary" to "jackson",
            "withXml" to false,
        )
}

sourceSets {
    main {
        java.srcDir(layout.buildDirectory.dir("generate-resources/main/src/main/java"))
        kotlin.srcDir(constantsDir)
    }
}

abstract class GenerateConstantsTask : DefaultTask() {
    @get:Input
    abstract val airflowExecApiVersionProp: Property<String>

    @get:OutputDirectory
    abstract val outputDirProp: DirectoryProperty

    @TaskAction
    fun generate() {
        val dir = outputDirProp.get().asFile.resolve("org/apache/airflow/sdk/execution")
        dir.mkdirs()
        dir.resolve("BuildConstants.kt").writeText(
            """
            // File generated at ${ZonedDateTime.now()}
            package org.apache.airflow.sdk.execution

            const val AIRFLOW_EXEC_API_VERSION = "${airflowExecApiVersionProp.get()}"
            """.trimIndent() + "\n",
        )
    }
}

tasks.register<GenerateConstantsTask>("generateConstants") {
    airflowExecApiVersionProp = airflowExecApiVersion
    outputDirProp = constantsDir
}

tasks.named<JavaCompile>("compileJava") {
    dependsOn("openApiGenerate")
}

tasks.named<KotlinCompile>("compileKotlin") {
    dependsOn("openApiGenerate")
    dependsOn("generateConstants")
}

tasks.named<KtLintCheckTask>("runKtlintCheckOverMainSourceSet") {
    dependsOn("openApiGenerate")
    dependsOn("generateConstants")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
