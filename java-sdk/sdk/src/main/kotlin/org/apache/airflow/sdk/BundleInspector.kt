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

package org.apache.airflow.sdk

import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Modifier

/**
 * Build-time introspection entry point for Dag bundles.
 *
 * Instantiates the [BundleBuilder] named by the single command-line argument,
 * builds its [Bundle], and prints a warning to stderr for every Dag or task ID
 * the Airflow server would reject. The Gradle plugin's `checkAirflowBundle`
 * task runs this against the compiled classes, so bundle authors see the
 * warnings at build time, before anything is deployed.
 *
 * Warnings never fail the process; a bundle that cannot be built at all (for
 * example duplicate Dag IDs) does. A main class that is not an instantiable
 * [BundleBuilder] is skipped with a note, since the bundle contract only
 * requires a static `main` method.
 */
object BundleInspector {
  @JvmStatic
  fun main(args: Array<String>) {
    require(args.size == 1) { "usage: BundleInspector <bundle-builder-class>" }
    inspect(args[0], System.err)
  }

  internal fun inspect(
    className: String,
    output: Appendable,
  ) {
    val builder = findBundleBuilder(className)
    if (builder == null) {
      output.appendLine(
        "note: $className is not an instantiable BundleBuilder; skipping the Dag and task ID check",
      )
      return
    }
    val bundle = builder.build()
    for (warning in IdValidation.findSuspiciousIds(bundle.dags.values)) {
      output.appendLine("warning: ${warning.render()}")
    }
  }

  /**
   * Resolves [className] to a [BundleBuilder]: a Kotlin or Scala singleton
   * (an `INSTANCE` or `MODULE$` static field, the latter living on the
   * `$`-suffixed class scalac emits next to the forwarder class), or a public
   * no-argument constructor. Returns null when the class has neither; a
   * constructor that itself throws is a real error and propagates.
   */
  private fun findBundleBuilder(className: String): BundleBuilder? {
    val classes = listOfNotNull(Class.forName(className), loadClassOrNull(className + "$"))
    classes.firstNotNullOfOrNull { findSingleton(it) }?.let { return it }
    return classes.firstNotNullOfOrNull { constructOrNull(it) }
  }

  private fun loadClassOrNull(name: String): Class<*>? =
    try {
      Class.forName(name)
    } catch (_: ClassNotFoundException) {
      null
    }

  private fun findSingleton(clazz: Class<*>): BundleBuilder? =
    listOf("INSTANCE", "MODULE$").firstNotNullOfOrNull { name ->
      try {
        clazz
          .getDeclaredField(name)
          .takeIf { Modifier.isStatic(it.modifiers) && Modifier.isPublic(it.modifiers) }
          ?.get(null) as? BundleBuilder
      } catch (_: NoSuchFieldException) {
        null
      } catch (_: IllegalAccessException) {
        null
      }
    }

  private fun constructOrNull(clazz: Class<*>): BundleBuilder? {
    if (!BundleBuilder::class.java.isAssignableFrom(clazz)) return null
    val constructor =
      try {
        clazz.getConstructor()
      } catch (_: NoSuchMethodException) {
        return null
      }
    return try {
      constructor.newInstance() as BundleBuilder
    } catch (_: InstantiationException) {
      null
    } catch (e: InvocationTargetException) {
      throw e.cause ?: e
    }
  }
}
