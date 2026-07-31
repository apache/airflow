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
    `java-library`
    id("airflow-jvm-conventions")
    id("airflow-publish")
}

val mockkVersion: String by project

dependencies {
    implementation(project(":sdk"))
    testImplementation(kotlin("test"))
    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("io.mockk:mockk-agent:$mockkVersion")
}

java {
    withSourcesJar() // Required by Maven Central.
}

tasks.withType<Test> {
    useJUnitPlatform()
    jvmArgs("-Djdk.attach.allowAttachSelf=true")
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "airflow-sdk-jul"
            from(components["java"])
            pom {
                name = "Apache Airflow Java SDK java.util.Logging Handler"
                description = "Routes java.util.Logging calls from task code through the SDK to Airflow's task log store."
            }
        }
    }
}
