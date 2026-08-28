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

plugins {
    id("airflow-jvm-conventions")
}

dependencies {
    testImplementation(kotlin("test"))
}

// The tests drive scripts/ci/*.sh, which locate their own java-sdk root relative
// to themselves, so they need to know where the real one is.
val javaSdkRoot = rootProject.layout.projectDirectory.asFile.absolutePath

// A Test task's inputs default to the compiled classes and the runtime classpath, so Gradle cannot know
// that these tests also read the paths below. Without these declarations, editing one of them leaves the
// task up to date, so Gradle skips the tests and a broken script still looks like it passed.
val scriptsUnderTest = rootProject.layout.projectDirectory.dir("scripts/ci")
val committedMetadata = rootProject.layout.projectDirectory.file("gradle/verification-metadata.xml")

tasks.withType<Test> {
    useJUnitPlatform()
    systemProperty("javaSdkRoot", javaSdkRoot)
    inputs.dir(scriptsUnderTest).withPropertyName("scriptsUnderTest")
    inputs.file(committedMetadata).withPropertyName("committedMetadata")
}
