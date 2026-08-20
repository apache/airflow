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

dependencies {
    implementation(project(":sdk"))
    testImplementation(kotlin("test"))
    testImplementation(testFixtures(project(":sdk")))
}

java {
    withSourcesJar() // Required by Maven Central.
}

tasks.withType<Test> {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "airflow-sdk-jpl"
            from(components["java"])
            pom {
                name = "Apache Airflow Java SDK Java Platform Logging Provider"
                description =
                    "Java Platform Logging (System.Logger) provider for the Apache Airflow Java SDK. " +
                    "Routes java.lang.System.Logger calls from task code through the SDK " +
                    "to Airflow's task log store."
            }
        }
    }
}
