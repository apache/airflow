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

/**
 * @suppress
 *
 * What a task group ID may contain, mirroring Python's `GROUP_KEY_REGEX`.
 * Public so the annotation processor, which is a separate module, can check it
 * against the same pattern the SDK enforces; not user-facing API.
 */
val GROUP_ID: Regex = Regex("[A-Za-z0-9_-]+")

/**
 * @suppress
 *
 * The task ID a task declared from a class alone carries: the class's simple
 * name with its first character lowercased, so `HasRows.class` and a
 * `hasRows` task method name agree. Public so the annotation processor can
 * check an ID it derives the same way; not user-facing API.
 */
fun deriveTaskId(definition: Class<*>): String = definition.simpleName.replaceFirstChar { it.lowercaseChar() }
