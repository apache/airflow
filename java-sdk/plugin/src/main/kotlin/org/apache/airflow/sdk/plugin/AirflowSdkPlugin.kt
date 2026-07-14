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

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.bundling.Jar
import java.lang.reflect.Modifier
import java.net.URLClassLoader
import java.util.jar.JarFile
import kotlin.jvm.java

/**
 * DSL extension registered by [AirflowSdkPlugin] as the `airflowBundle` block.
 *
 * ```kotlin
 * airflowBundle {
 *     mainClass = "com.example.ExampleBundleBuilder"
 *     // fatJar = false  // opt out of shadow JAR creation
 * }
 * ```
 */
abstract class AirflowBundleExtension {
  /** Fully qualified name of the entry-point class.
   *
   * This is written to the JAR manifest under the standard `Main-Class` key. The
   * coordinator uses the value to execute the bundle JAR correctly.
   */
  @get:Input
  abstract val mainClass: Property<String>

  /**
   * Whether to build a fat JAR using the Shadow plugin (default: `true`).
   *
   * When `true`, the Shadow plugin is applied automatically and
   * `Airflow-Supervisor-Schema-Version` is injected into the shadow JAR manifest.
   * When `false`, no fat JAR is produced; only `Main-Class` is set on the
   * regular `jar` task.
   */
  @get:Input
  abstract val fatJar: Property<Boolean>
}

/**
 * Gradle plugin for building Apache Airflow Java SDK bundles.
 *
 * Minimal usage:
 *
 * ```kotlin
 * plugins {
 *     id("org.apache.airflow.sdk") version <version>
 * }
 *
 * airflowBundle {
 *     mainClass = "com.example.ExampleBundleBuilder"
 * }
 * ```
 *
 * See [AirflowBundleExtension] to understand how to configure the plugin with the
 * `airflowBundle` block.
 *
 * The plugin automatically sets the `Main-Class` metadata, and provides a new
 * task `bundle` to create a Dag bundle in one command. This builds deploy-ready
 * artifacts to `build/bundle/` that can be copied directly into an Airflow Java
 * coordinator's `jars_root`.
 *
 * By default, plugin `com.github.johnrengelman.shadow` is applied automatically
 * to enable fat JAR build. In this mode, one single JAR with user code and all
 * dependencies is produced by the `bundle` task. An additional metadata entry
 * `Airflow-Supervisor-Schema-Version` is also added to the JAR for Airflow to
 * identify which version of the Supervisor Schema it should use to communicate
 * with the built JAR.
 *
 * If `fatJar` is explicitly set to `false`, the `bundle` task builds a bare JAR
 * containing only the Dag bundle, and collect all dependency JARs into the target
 * directory instead. In this mode, `Airflow-Supervisor-Schema-Version` lives in
 * the `airflow-sdk` JAR instead. The bundle JAR still contains `Main-Class`.
 *
 */
class AirflowSdkPlugin : Plugin<Project> {
  override fun apply(project: Project) {
    project.plugins.apply("java")

    val ext = project.extensions.create("airflowBundle", AirflowBundleExtension::class.java)
    ext.fatJar.convention(true)

    project.afterEvaluate {
      project.tasks.withType(Jar::class.java).configureEach { task ->
        task.doFirst {
          ext.mainClass.orNull?.let { className ->
            task.manifest.attributes(mapOf("Main-Class" to className))
          }
        }
      }

      val classFiles =
        project.objects.fileCollection().from(
          project.extensions
            .getByType(SourceSetContainer::class.java)
            .getByName("main")
            .output
            .classesDirs,
          project.configurations.getByName("runtimeClasspath"),
        )

      val verifyTask =
        project.tasks.register("verifyBundleMainClass") { task ->
          task.group = "verification"
          task.description =
            "Verifies that mainClass exists in the compiled output and declares a public static main method."
          task.dependsOn(project.tasks.named("classes"))
          task.inputs.files(classFiles).withPropertyName("classFiles")
          task.inputs.property("mainClass", ext.mainClass)
          task.doLast {
            val className =
              ext.mainClass.orNull
                ?: error("airflowBundle.mainClass is not set. Add it to your airflowBundle { } block.")
            val urls = classFiles.map { it.toURI().toURL() }.toTypedArray()

            URLClassLoader(urls, ClassLoader.getPlatformClassLoader()).use { loader ->
              val klass =
                try {
                  loader.loadClass(className)
                } catch (_: ClassNotFoundException) {
                  error("Configured main class '$className' is not found.")
                }
              val main =
                try {
                  klass.getMethod("main", Array<String>::class.java)
                } catch (_: NoSuchMethodException) {
                  error("mainClass '$className' does not have a public static main method.")
                }
              check(Modifier.isStatic(main.modifiers)) {
                "main method on '$className' is not static."
              }
              check(main.returnType == Void.TYPE) {
                "main method on '$className' must return void."
              }
            }
          }
        }

      if (ext.fatJar.get()) {
        project.plugins.apply("com.gradleup.shadow")

        val schemaVersionProvider =
          project.providers.provider {
            val runtimeClasspath =
              project.configurations.findByName("runtimeClasspath")
                ?: error(
                  "No runtimeClasspath configuration found. " +
                    "Make sure the java or application plugin is applied before " +
                    "org.apache.airflow.sdk.",
                )
            runtimeClasspath.resolvedConfiguration.resolvedArtifacts
              .firstOrNull { a ->
                a.moduleVersion.id.group == "org.apache.airflow" && a.name == "airflow-sdk"
              }?.let { artifact ->
                JarFile(artifact.file).use { jar ->
                  jar.manifest?.mainAttributes?.getValue("Airflow-Supervisor-Schema-Version")
                }
              } ?: error(
              "airflow-sdk not found in runtimeClasspath, or its JAR is missing the " +
                "Airflow-Supervisor-Schema-Version manifest attribute. " +
                "Make sure org.apache.airflow:airflow-sdk is declared as an " +
                "implementation dependency.",
            )
          }

        project.tasks.withType(ShadowJar::class.java).configureEach { task ->
          task.doFirst {
            task.manifest.attributes(
              mapOf("Airflow-Supervisor-Schema-Version" to schemaVersionProvider.get()),
            )
          }
        }

        project.tasks.register("bundle", Copy::class.java) { task ->
          task.group = "build"
          task.description = "Assembles a fat JAR bundle for deployment to Airflow."
          task.dependsOn(verifyTask)
          task.from(project.tasks.named("shadowJar"))
          task.into(project.layout.buildDirectory.dir("bundle"))
        }
      } else {
        // bundle copies the thin JAR and all runtime dependency JARs into
        // build/bundle/, mirroring what installDist puts in lib/.
        project.tasks.register("bundle", Copy::class.java) { task ->
          task.group = "build"
          task.description = "Assembles a thin JAR bundle directory for deployment to Airflow."
          task.dependsOn(verifyTask)
          task.from(project.configurations.getByName("runtimeClasspath"))
          task.from(project.tasks.named("jar"))
          task.into(project.layout.buildDirectory.dir("bundle"))
        }
      }
    }
  }
}
