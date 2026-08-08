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

/**
 * Marks the static method of a [Builder.Dag]-annotated class that wires its
 * task graph, TaskFlow-style — the single place dependencies are declared.
 *
 * The method receives the generated twin class (`<Class>Ref`), whose
 * methods mirror the [Builder.Task]-annotated methods with data parameters
 * typed [In] and return values typed [TaskRef]. Calling a twin registers
 * the task; passing one twin's return value into another feeds the upstream's
 * output into the downstream's parameter and wires the dependency edge, all
 * type-checked at compile time. The call graph is the task graph:
 *
 * ```java
 * @Wiring
 * static void depends(EtlPipelineRef f) {
 *   f.load(f.transform(f.extract()));
 * }
 * ```
 *
 * Every [Builder.Task]-annotated method must be invoked exactly once; the
 * generated `build()` fails at Dag-parse time otherwise.
 *
 * The wiring method is optional: a [Builder.Dag] class without one registers
 * every task with no Java-side edges, which is the shape for stub-backed tasks
 * whose graph is defined by a Python Dag file.
 */
@Target(AnnotationTarget.FUNCTION)
@MustBeDocumented
annotation class Wiring
