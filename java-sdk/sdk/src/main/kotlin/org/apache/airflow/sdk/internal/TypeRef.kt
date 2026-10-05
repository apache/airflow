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

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

/**
 * @suppress
 *
 * Carries a full generic type into [TaskArgs], which a `Class` literal cannot
 * express. Subclass it anonymously so the type argument survives erasure on
 * the subclass's signature:
 *
 * ```java
 * args.require(0, new TypeRef<List<String>>() {});
 * ```
 *
 * Emitted by the annotation processor for a data parameter whose declared type
 * has type arguments; not user-facing API. The SDK owns this rather than
 * reusing Jackson's `TypeReference` so that Jackson stays off a consumer's
 * compile classpath.
 */
abstract class TypeRef<T> protected constructor() {
  internal val type: Type =
    (javaClass.genericSuperclass as? ParameterizedType)?.actualTypeArguments?.firstOrNull()
      ?: throw IllegalArgumentException(
        "TypeRef needs a concrete type argument on an anonymous subclass, e.g. new TypeRef<List<String>>() {}",
      )
}
