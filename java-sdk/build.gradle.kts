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

import org.gradle.process.CommandLineArgumentProvider

plugins {
    id("io.github.gradle-nexus.publish-plugin") version "2.0.0"
}

val projectVersion: String by project

group = "org.apache.airflow"
version = projectVersion

if (!project.hasProperty("mavenUrl")) {
    nexusPublishing {
        repositories {
            create("apache") {
                nexusUrl.set(uri("https://repository.apache.org/service/local/"))
                snapshotRepositoryUrl.set(
                    uri("https://repository.apache.org/content/repositories/snapshots/"),
                )
                username.set(
                    providers.gradleProperty("mavenUsername")
                        .orElse(providers.environmentVariable("ASF_NEXUS_USERNAME")),
                )
                password.set(
                    providers.gradleProperty("mavenPassword")
                        .orElse(providers.environmentVariable("ASF_NEXUS_PASSWORD")),
                )
            }
        }
    }
}

val sourceReleaseDir = layout.buildDirectory.dir("distributions")

// Derive the version from the tag, so the tarball's name matches its contents.
val sourceReleaseVersion =
    providers.gradleProperty("gitRef").map {
        it.substringAfterLast('/').replace(Regex("-rc\\d+$"), "")
    }
val sourceReleaseTarball =
    sourceReleaseDir.zip(sourceReleaseVersion) { dir, version ->
        dir.file("apache-airflow-java-sdk-$version-src.tar.gz")
    }

val sourceTarball by tasks.registering(Exec::class) {
    group = "release"
    description = "Assembles the source tarball from committed java-sdk sources."
    executable = "git"
    workingDir = rootDir

    // Capture early to keep compatibility to the Gradle configuration cache.
    val gitRef = providers.gradleProperty("gitRef")
    val archiveVersion = sourceReleaseVersion
    val tarball = sourceReleaseTarball

    argumentProviders.add(
        CommandLineArgumentProvider {
            listOf(
                "archive",
                "--format=tar.gz",
                "--prefix=apache-airflow-java-sdk-${archiveVersion.get()}/",
                "-o", tarball.get().asFile.absolutePath,
                gitRef.get(),
            )
        },
    )

    doFirst { tarball.get().asFile.parentFile.mkdirs() }
}

val signSourceTarball by tasks.registering(Exec::class) {
    group = "release"
    description = "Creates the detached OpenPGP signature (.asc) for the source tarball."
    executable = "gpg"
    workingDir = rootDir
    dependsOn(sourceTarball)

    // Capture early to keep compatibility to the Gradle configuration cache.
    val tarball = sourceReleaseTarball
    argumentProviders.add(
        CommandLineArgumentProvider {
            listOf("--armor", "--yes", "--detach-sign", tarball.get().asFile.absolutePath)
        },
    )
}

val checksumSourceTarball by tasks.registering {
    group = "release"
    description = "Writes the SHA-512 checksum (.sha512) for the source tarball."
    dependsOn(sourceTarball)

    // Capture early to keep compatibility to the Gradle configuration cache.
    val tarball = sourceReleaseTarball
    doLast {
        val file = tarball.get().asFile
        val digest = java.security.MessageDigest.getInstance("SHA-512")
        file.inputStream().use { input ->
            val buffer = ByteArray(8192)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        val hex = digest.digest().joinToString("") { "%02x".format(it) }
        file.resolveSibling("${file.name}.sha512").writeText("$hex  ${file.name}\n")
    }
}

tasks.register("sourceRelease") {
    group = "release"
    description = "Builds the source tarball plus its .asc signature and .sha512 checksum."
    dependsOn(sourceTarball, signSourceTarball, checksumSourceTarball)
}
