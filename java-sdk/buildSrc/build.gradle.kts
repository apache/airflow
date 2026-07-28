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

import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

// Pin Java and Kotlin bytecode targets so they stay aligned to avoid warning on
// misaligned Java and Kotlin target versions. (This is for the build logic and
// unrelated to the Java 11 target set in the SDK modules.)
java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}
kotlin { compilerOptions { jvmTarget = JvmTarget.JVM_11 } }

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.3.0")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:7.2.1")
    implementation("org.jlleitschuh.gradle:ktlint-gradle:14.0.1")
}
