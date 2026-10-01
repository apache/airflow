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
 * Names the registrar generated for a class of `@Builder.TaskHandler`
 * methods: a top-level class in the handler class's package, named after that
 * class and any class enclosing it, so `Outer.Inner` is served by
 * `Outer_InnerHandlers`.
 *
 * Public so the annotation processor emits the name the runtime looks up; not
 * user-facing API.
 *
 * @param binaryName Binary name of the handler class, as `Class.getName`
 *    returns it.
 */
fun registrarName(binaryName: String): String = "${binaryName.replace('$', '_')}Handlers"
