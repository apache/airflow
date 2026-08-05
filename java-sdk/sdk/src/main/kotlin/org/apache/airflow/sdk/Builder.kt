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
 * Container for the annotation-based Dag-authoring API.
 *
 * This class is not instantiated directly. Its nested annotations drive the
 * `BuilderProcessor` annotation processor in the :processor project,
 * which generates a `*Builder` class for each class annotated with [Dag].
 *
 * Example:
 *
 * ```java
 * @Builder.Dag(id = "my_pipeline")
 * public class MyPipeline {
 *
 *     @Builder.Task(id = "extract")
 *     public long extract(Client client) { ... }
 *
 *     @Builder.Task(id = "transform")
 *     public long transform(Client client, @Builder.XCom(task = "extract") long extracted) { ... }
 * }
 * ```
 *
 * The processor generates `MyPipelineBuilder.build()`, which returns a
 * fully wired-up [Dag] ready to add to a [Bundle].
 */
class Builder internal constructor() {
  /**
   * Annotation to automate a Dag-builder pattern.
   *
   * When applied on a class Foo, this generates a FooBuilder class with a
   * static build method to create the Dag structure automatically.
   *
   * @param id Override the Dag ID. If empty or not provided, the annotated
   *    class's name is used by default.
   * @param to Name of the Dag-builder class. If empty or not provided, use the
   *    annotated class name + "Builder".
   */
  @Target(AnnotationTarget.CLASS)
  @MustBeDocumented
  annotation class Dag(
    val id: String = "",
    val to: String = "",
  )

  /**
   * Annotation to automate task definition in a Dag-builder pattern.
   *
   * @param id Override the task ID. If empty or not provided, the annotated
   *    function's name is used by default.
   */
  @Target(AnnotationTarget.FUNCTION)
  @MustBeDocumented
  annotation class Task(
    val id: String = "",
  )

  /**
   * Annotation to mark a task definition's method parameter as an XCom input.
   *
   * @param task The task ID to pull. If empty or not given, the annotated
   *    parameter's name is used by default.
   */
  @Target(AnnotationTarget.VALUE_PARAMETER)
  @MustBeDocumented
  annotation class XCom(
    val task: String = "",
  )
}
