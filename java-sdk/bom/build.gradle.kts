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

import org.gradle.api.publish.PublishingExtension

plugins {
    base
    `java-platform`
    id("airflow-publish")
}

val projectVersion: String by project
val airflowSupervisorSchemaVersion: String by project

dependencies {
    constraints {
        api("org.apache.airflow:airflow-sdk:$projectVersion")
        api("org.apache.airflow:airflow-sdk-gradle-plugin:$projectVersion")
        api("org.apache.airflow:airflow-sdk-jpl:$projectVersion")
        api("org.apache.airflow:airflow-sdk-jul:$projectVersion")
        api("org.apache.airflow:airflow-sdk-log4j2:$projectVersion")
        api("org.apache.airflow:airflow-sdk-processor:${projectVersion}")
        api("org.apache.airflow:airflow-sdk-slf4j:$projectVersion")
    }
}

publishing {
    publications {
        create<MavenPublication>("mavenBom") {
            artifactId = "airflow-sdk-bom"
            from(components["javaPlatform"])
            pom {
                name = "Apache Airflow Java SDK BOM"
                description = "Bill of Materials for the Apache Airflow Java SDK."
                properties.put("airflow.supervisor.schema.version", airflowSupervisorSchemaVersion)
            }
        }
    }
}

// What the BOM currently constrains (group org.apache.airflow).
val bomArtifacts =
    configurations["api"].dependencyConstraints
        .filter { it.group == "org.apache.airflow" }
        .map { it.name }
        .toSet()

val publishedArtifacts =
    rootProject.subprojects
        .filter { it.path != project.path }
        .flatMap { sub ->
            evaluationDependsOn(sub.path)
            sub.extensions.findByType(PublishingExtension::class.java)
                ?.publications
                ?.withType(MavenPublication::class.java)
                ?.filter { it.groupId == "org.apache.airflow" }
                ?.map { it.artifactId }
                ?: emptyList()
        }
        .toSet()

val verifyBomCoverage by tasks.registering {
    group = "verification"
    description = "Fail if airflow-sdk-bom does not constrain the same set of published Java SDK artifacts."

    // Capture early to keep compatibility to the Gradle configuration cache.
    val published = publishedArtifacts
    val bom = bomArtifacts
    doLast {
        val missing = (published - bom).sorted()
        val stale = (bom - published).sorted()
        if (missing.isNotEmpty() || stale.isNotEmpty()) {
            throw GradleException(
                buildString {
                    appendLine("airflow-sdk-bom is out of sync with the published Java SDK artifacts:")
                    if (missing.isNotEmpty()) appendLine("  published but missing from the BOM: $missing")
                    if (stale.isNotEmpty()) appendLine("  listed in the BOM but not published: $stale")
                    append("Update constraints in bom/build.gradle.kts to match.")
                },
            )
        }
    }
}

tasks.named("check") { dependsOn(verifyBomCoverage) }
