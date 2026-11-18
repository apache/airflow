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

import java.lang.reflect.GenericArrayType
import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type
import java.lang.reflect.TypeVariable
import java.lang.reflect.WildcardType

/**
 * Carries a full generic type into [Client.getXCom] and [DagRun.getConf]. A
 * `Class` literal cannot express a full generic type. Extend `TypeRef` directly
 * in an anonymous class, so that the type argument survives erasure in the
 * signature of the anonymous class:
 *
 * ```java
 * List<String> regions = client.getXCom("find_regions", new TypeRef<List<String>>() {});
 * ```
 *
 * `TypeRef` rejects a class between the anonymous class and `TypeRef`. For
 * example, with `abstract class ListRef<E> extends TypeRef<List<E>>`,
 * `new ListRef<String>() {}` throws. The type argument cannot contain a type
 * variable, such as the `T` of a generic method or class, because erasure
 * leaves no type for the type variable at run time.
 *
 * The SDK defines `TypeRef` instead of reusing Jackson's `TypeReference`, so
 * that Jackson stays off a consumer's compile classpath.
 *
 * @param T Type to read the value into.
 */
abstract class TypeRef<T> protected constructor() {
  internal val type: Type =
    (javaClass.genericSuperclass as? ParameterizedType)
      ?.takeIf { it.rawType == TypeRef::class.java }
      ?.actualTypeArguments
      ?.firstOrNull()
      ?.takeUnless(::hasTypeVariable)
      ?: throw IllegalArgumentException(
        "TypeRef needs an anonymous class that extends TypeRef directly and has a concrete type argument, e.g. new TypeRef<List<String>>() {}",
      )
}

private fun hasTypeVariable(type: Type): Boolean =
  when (type) {
    is TypeVariable<*> -> true
    is ParameterizedType -> type.actualTypeArguments.any(::hasTypeVariable)
    is GenericArrayType -> hasTypeVariable(type.genericComponentType)
    is WildcardType -> type.upperBounds.any(::hasTypeVariable) || type.lowerBounds.any(::hasTypeVariable)
    else -> false
  }
