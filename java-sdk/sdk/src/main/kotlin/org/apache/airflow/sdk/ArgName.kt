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
 * Declares the wire name of a [TaskInput] field explicitly, for when the
 * Python stub signature's argument name is not a valid (or desirable) Java
 * field name — typically `snake_case` arguments crossing into `camelCase`
 * fields.
 *
 * ```java
 * @ArgName("region_code") public String region;
 * ```
 *
 * Fields without the annotation bind their verbatim field name.
 *
 * @param value Argument name as declared in the stub task's signature.
 */
@Target(AnnotationTarget.FIELD)
@MustBeDocumented
annotation class ArgName(
  val value: String,
)
