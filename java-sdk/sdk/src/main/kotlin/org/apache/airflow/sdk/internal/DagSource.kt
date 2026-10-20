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

package org.apache.airflow.sdk.internal

import org.apache.airflow.sdk.DagDef

/**
 * @suppress
 *
 * Tracks which class declared a [DagDef], so the bundle can ship that class's
 * source file. Public so that processor-generated builders can call [declaredBy];
 * not user-facing API.
 */
object DagSource {
  private const val SDK_PACKAGE = "org.apache.airflow.sdk."
  private val IGNORED_PREFIXES = listOf(SDK_PACKAGE, "java.", "javax.", "jdk.", "sun.", "kotlin.", "kotlinx.")

  private val walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE)

  /**
   * Names [declaring] as the class that declared [dag], replacing what was
   * captured at construction. A generated builder calls this so the Dag points
   * at the annotated class rather than the builder.
   */
  @JvmStatic
  fun declaredBy(
    dag: DagDef,
    declaring: Class<*>,
  ): DagDef {
    dag.declaringClass = outermost(declaring)
    return dag
  }

  /** The outermost class of the first caller outside the SDK and the standard libraries. */
  internal fun capture(): Class<*>? =
    walker.walk { frames ->
      frames
        .map { it.declaringClass }
        .filter { c -> IGNORED_PREFIXES.none { c.name.startsWith(it) } }
        .findFirst()
        .map { outermost(it) }
        .orElse(null)
    }

  internal fun outermost(cls: Class<*>): Class<*> {
    var current = cls
    while (true) current = current.enclosingClass ?: return current
  }
}
