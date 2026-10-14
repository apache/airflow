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
 * Pins the stub argument a [TaskInput] field binds, for a Python name that no
 * Java identifier reaches on its own.
 *
 * A field without the annotation already matches its argument ignoring case
 * and underscores, so an ordinary `snake_case` parameter needs no annotation:
 *
 * ```java
 * public String regionCode;                       // binds region_code
 * @ArgName("class") public String klass;          // binds a Python keyword
 * ```
 *
 * Reach for it when the argument name is not a legal or usable Java
 * identifier. A pinned name is matched as written, with no folding, so the
 * annotation says exactly which argument the field takes.
 *
 * @param value Argument name as declared in the stub task's signature.
 */
@Target(AnnotationTarget.FIELD)
@MustBeDocumented
annotation class ArgName(
  val value: String,
)
