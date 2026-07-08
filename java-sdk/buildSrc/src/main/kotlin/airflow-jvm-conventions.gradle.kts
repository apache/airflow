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

import com.diffplug.gradle.spotless.SpotlessExtension
import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    id("com.diffplug.spotless")
    id("org.jetbrains.kotlin.jvm")
    id("org.jlleitschuh.gradle.ktlint")
}

repositories {
    mavenCentral()
}

// The versions below are kept in sync with the other build files by a pre-commit hook.
// See: scripts/ci/prek/check_java_sdk_version_in_sync.py
java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(11))
    }
    sourceCompatibility = JavaVersion.VERSION_11
}

kotlin {
    compilerOptions {
        jvmTarget = JvmTarget.JVM_11
    }
}

configure<SpotlessExtension> {
    java {
        target("**/*.java")
        googleJavaFormat().formatJavadoc(false)
        trimTrailingWhitespace()
        endWithNewline()
    }
}

// ASF release policy requires every distributed artifact (including convenience
// binaries such as the main, sources, javadoc, and test-fixtures jars) to carry
// LICENSE/NOTICE. `rootProject` is used rather than a bare `rootDir`/`projectDir`
// because this precompiled script plugin is applied per-subproject: an unqualified
// reference would resolve relative to whichever subproject applies the plugin,
// not the multi-project root where LICENSE/NOTICE actually live.
tasks.withType<Jar>().configureEach {
    metaInf {
        from(rootProject.layout.projectDirectory.file("LICENSE"))
        from(rootProject.layout.projectDirectory.file("NOTICE"))
    }
}

// Byte-reproducible archives: strip wall-clock timestamps, fix entry ordering,
// and pin permission bits so two builds from the same sources are bit-identical.
tasks.withType<AbstractArchiveTask>().configureEach {
    isPreserveFileTimestamps = false
    isReproducibleFileOrder = true
    dirPermissions { unix("755") }
    filePermissions { unix("644") }
}
