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
 * Pins the name that a field matches when no Java identifier can match that
 * name on its own: the stub argument of a [TaskInput] field, or a key in a value
 * that the SDK reads.
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
 * annotation says exactly which argument the field takes. An argument with the
 * field's own name does not match the field when the pinned name is different.
 *
 * The annotation also pins the key of a field in any other class that the SDK
 * reads a value into. Examples are a class nested in a task argument and a class
 * passed to [Client.getXCom] or [DagRun.getConf]. A task that pushes an instance
 * of such a class writes the pinned name as the key. Do not combine this annotation
 * with Jackson's `@JsonProperty` on one field.
 *
 * @param value Argument name as declared in the stub task's signature, or the key
 *    in the value that the SDK reads.
 */
@Target(AnnotationTarget.FIELD)
@MustBeDocumented
annotation class ArgName(
  val value: String,
)
