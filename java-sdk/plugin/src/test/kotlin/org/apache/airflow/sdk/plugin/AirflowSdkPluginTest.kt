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

package org.apache.airflow.sdk.plugin

import org.gradle.api.Project
import org.gradle.api.internal.project.ProjectInternal
import org.gradle.api.tasks.JavaExec
import org.gradle.testfixtures.ProjectBuilder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

internal class AirflowSdkPluginTest {
  private fun buildProject(mainClass: String? = "com.example.MyBundleBuilder"): Project {
    val project = ProjectBuilder.builder().build()
    project.plugins.apply("org.apache.airflow.sdk")
    mainClass?.let {
      project.extensions
        .getByType(AirflowBundleExtension::class.java)
        .mainClass
        .set(it)
    }
    (project as ProjectInternal).evaluate()
    return project
  }

  @Test
  @DisplayName("checkAirflowBundle runs BundleInspector with the configured main class")
  fun shouldRegisterCheckAirflowBundle() {
    val project = buildProject()

    val task = project.tasks.getByName("checkAirflowBundle") as JavaExec

    assertEquals("verification", task.group)
    assertEquals("org.apache.airflow.sdk.BundleInspector", task.mainClass.get())
    assertEquals(listOf("com.example.MyBundleBuilder"), task.args)
  }

  @Test
  @DisplayName("check depends on checkAirflowBundle")
  fun shouldWireCheckLifecycle() {
    val project = buildProject()

    val check = project.tasks.getByName("check")

    assertTrue(check.taskDependencies.getDependencies(check).any { it.name == "checkAirflowBundle" })
  }

  @Test
  @DisplayName("without a mainClass the task gets no arguments")
  fun shouldHaveNoArgumentsWithoutMainClass() {
    val project = buildProject(mainClass = null)

    val task = project.tasks.getByName("checkAirflowBundle") as JavaExec

    assertEquals(emptyList<String>(), task.args)
  }
}
