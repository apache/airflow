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

val log4jVersion: String by project

dependencies {
    annotationProcessor("org.apache.logging.log4j:log4j-core:$log4jVersion")
    api("org.apache.logging.log4j:log4j-api:$log4jVersion")
    compileOnly("org.apache.logging.log4j:log4j-core:$log4jVersion")
    implementation(project(":sdk"))

    testImplementation(kotlin("test"))
    testImplementation("org.apache.logging.log4j:log4j-core:$log4jVersion")
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
            artifactId = "airflow-sdk-log4j2"
            from(components["java"])
            pom {
                name = "Apache Airflow Java SDK Log4j 2 Appender"
                description = "Routes Log4j 2 log calls from task code through the SDK to Airflow's task log store."
            }
        }
    }
}
