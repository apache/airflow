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
    compileOnly("javax.annotation:javax.annotation-api:1.3.2")
    implementation(project(":sdk"))
    implementation("com.squareup:javapoet:1.13.0")

    testImplementation(kotlin("test"))
    testImplementation("com.google.testing.compile:compile-testing:0.23.0")
}

java {
    withSourcesJar()
}

tasks.withType<Test> {
    useJUnitPlatform()
}

publishing {
    publications {
        create<MavenPublication>("mavenJava") {
            artifactId = "airflow-sdk-processor"
            from(components["java"])
            pom {
                name = "Apache Airflow Java SDK — Annotation Processor"
                description =
                    "Annotation processor for the Apache Airflow Java SDK; " +
                    "generates *Builder classes from @Builder.Dag-annotated sources."
            }
        }
    }
}
