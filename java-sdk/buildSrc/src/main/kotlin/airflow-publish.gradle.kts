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

/**
 * Convention plugin shared by all published Apache Airflow Java SDK subprojects.
 *
 * Applying `id("airflow-publish")` in a subproject's `plugins {}` would do the
 * following:
 *
 * - Sets `group = "org.apache.airflow"` and `version = projectVersion`.
 * - Applies `maven-publish` and `signing`.
 * - Wires the shared Maven repository (reads `mavenUrl`, `mavenUsername`,
 *   `mavenPassword`, or `ASF_NEXUS_*` env vars; auto-selects snapshot vs.
 *   release URL from `projectVersion`).
 * - Injects the common POM fields (url, organization, license, SCM) into
 *   every `MavenPublication` in the subproject
 * - Configures signing from `signing.key` / `SIGNING_KEY` and
 *   `signing.password` / `SIGNING_PASSWORD` (skipped when `skipSigning=true`)
 *
 * Each subproject only needs to provide its artifact-specific POM fields
 * (`name` and `description`) and the `artifactId`.
 */

plugins {
    `maven-publish`
    signing
}

val projectVersion: String by project

group = "org.apache.airflow"
version = projectVersion

private fun getProperty(name: String): String? = providers.gradleProperty(name).orNull

private fun getProperty(
    name: String,
    env: String,
): String? = getProperty(name) ?: System.getenv(env)

publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
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

    repositories {
        getProperty("mavenUrl")?.let { path ->
            maven {
                name = "maven"
                url = uri(path)
                if (url.scheme != "file") {
                    val user = getProperty("mavenUsername", "ASF_NEXUS_USERNAME")
                    val pass = getProperty("mavenPassword", "ASF_NEXUS_PASSWORD")
                    if (user != null && pass != null) {
                        credentials {
                            username = user
                            password = pass
                        }
                    }
                }
            }
        }
    }
}

signing {
    if (!providers.gradleProperty("skipSigning").map { it.toBoolean() }.getOrElse(false)) {
        val signingKey = getProperty("signing.key", "SIGNING_KEY")
        val signingPassword = getProperty("signing.password", "SIGNING_PASSWORD")
        useInMemoryPgpKeys(signingKey, signingPassword)
        publishing.publications.configureEach { sign(this) }
    }
}
