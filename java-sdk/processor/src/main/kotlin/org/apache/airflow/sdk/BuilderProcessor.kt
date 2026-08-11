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

@file:Suppress("PLATFORM_CLASS_MAPPED_TO_KOTLIN")

package org.apache.airflow.sdk

import com.squareup.javapoet.ClassName
import com.squareup.javapoet.CodeBlock
import com.squareup.javapoet.JavaFile
import com.squareup.javapoet.MethodSpec
import com.squareup.javapoet.TypeName
import com.squareup.javapoet.TypeSpec
import java.util.Optional
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.annotation.processing.SupportedSourceVersion
import javax.lang.model.SourceVersion
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.Modifier
import javax.lang.model.element.TypeElement
import javax.lang.model.type.TypeKind
import javax.lang.model.type.TypeMirror
import javax.tools.Diagnostic

/**
 * @suppress
 *
 * Annotation processor for [Builder.Dag].
 *
 * This is registered as a standard javac processor via
 * `META-INF/services/javax.annotation.processing.Processor`; not intended to be
 * instantiated or referenced directly.
 *
 * For each class annotated with [Builder.Dag], generates a `*Builder` class
 * containing:
 *
 * - One inner class per [Builder.Task]-annotated method, implementing [Task].
 * - A static `build()` method that constructs the [Dag] and registers those
 *   inner classes as tasks.
 *
 * [Builder.XCom]-annotated parameters are resolved via `client.getXCom` in the
 * generated `execute` body, with the result cast to the parameter's declared
 * type. Non-`void` return values are forwarded to `client.setXCom`.
 */
@SupportedAnnotationTypes("org.apache.airflow.sdk.Builder.Dag")
@SupportedSourceVersion(SourceVersion.RELEASE_11)
class BuilderProcessor : AbstractProcessor() {
  override fun process(
    annotations: Set<TypeElement>,
    roundEnv: RoundEnvironment,
  ): Boolean {
    if (annotations.isEmpty()) return false
    roundEnv.getElementsAnnotatedWith(Builder.Dag::class.java).filterIsInstance<TypeElement>().forEach { el ->
      with(processingEnv) {
        runCatching {
          JavaFile
            .builder(
              elementUtils.getPackageOf(el).qualifiedName.toString(),
              buildDag(el),
            ).build()
            .writeTo(filer)
        }.onFailure { e ->
          messager.printMessage(
            Diagnostic.Kind.ERROR,
            e.message ?: "Unknown error",
            el,
          )
        }
      }
    }
    return true
  }

  private fun buildDag(el: TypeElement): TypeSpec {
    val ann = el.getAnnotation(Builder.Dag::class.java)!!

    val builderClass =
      TypeSpec
        .classBuilder(ann.to.ifBlank { "${el.simpleName}Builder" })
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)

    val buildMethod =
      MethodSpec
        .methodBuilder("build")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(ClassName.get(Dag::class.java))
        .addStatement($$"var dag = new $T($S)", ClassName.get(Dag::class.java), ann.id.ifBlank { el.simpleName })

    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      if (inner.isVarArgs) throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")

      val ann = inner.getAnnotation(Builder.Task::class.java) ?: continue
      val innerName = inner.simpleName.toString().replaceFirstChar(Char::uppercase)

      val task = buildTask(innerName, inner, el)
      builderClass.addType(task.spec)

      buildMethod.addStatement(
        $$"dag.addTask($S, $L.class)",
        ann.id.ifBlank { inner.simpleName },
        innerName,
      )
    }

    buildMethod.addStatement("return dag")
    builderClass.addMethod(buildMethod.build())
    return builderClass.build()
  }

  private fun buildTask(
    name: String,
    inner: ExecutableElement,
    parent: TypeElement,
  ): BuildTaskResult {
    val clientType = ClassName.get(Client::class.java)
    val contextType = ClassName.get(Context::class.java)

    val executeSpec =
      MethodSpec
        .methodBuilder("execute")
        .addAnnotation(Override::class.java)
        .addModifiers(Modifier.PUBLIC)
        .returns(TypeName.VOID)
        .addParameter(contextType, "context")
        .addParameter(clientType, "client")
        .addException(Exception::class.java)

    val required = mutableListOf<RequiredXCom>()
    val innerArgs =
      with(processingEnv) {
        inner.parameters.joinToString { param ->
          val anno = param.getAnnotation(Builder.XCom::class.java)
          val type = param.asType()
          when {
            anno != null ->
              param.simpleName.toString().also {
                required += RequiredXCom(type, it, anno.task.ifBlank { it })
              }
            isType(type, clientType) -> "client"
            isType(type, contextType) -> "context"
            else -> throw IllegalArgumentException("Unsupported task parameter '${param.simpleName}' with type: $type")
          }
        }
      }
    required.forEach {
      executeSpec.addStatement($$"var $L = $L", it.paramName, xcomAccess(it))
    }
    if (inner.returnType.kind == TypeKind.VOID) {
      $$"new $T().$L($L)"
    } else {
      $$"client.setXCom(new $T().$L($L))"
    }.also {
      executeSpec.addStatement(
        it,
        ClassName.get(parent),
        inner.simpleName,
        innerArgs,
      )
    }

    val spec =
      TypeSpec
        .classBuilder(name)
        .addSuperinterface(Task::class.java)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL, Modifier.STATIC)
        .addMethod(executeSpec.build())
        .build()
    return BuildTaskResult(spec)
  }
}

private fun ProcessingEnvironment.isType(
  t: TypeMirror,
  c: ClassName,
): Boolean = typeUtils.isSameType(t, elementUtils.getTypeElement(c.canonicalName()).asType())

private data class RequiredXCom(
  val paramType: TypeMirror,
  val paramName: String,
  val taskId: String,
)

private val NUMBER_ACCESSORS: Map<TypeName, String> =
  buildMap {
    mapOf(
      TypeName.BYTE to "byteValue",
      TypeName.SHORT to "shortValue",
      TypeName.INT to "intValue",
      TypeName.LONG to "longValue",
      TypeName.FLOAT to "floatValue",
      TypeName.DOUBLE to "doubleValue",
    ).forEach { (primitive, accessor) ->
      put(primitive, accessor)
      put(primitive.box(), accessor)
    }
  }

private fun xcomAccess(xcom: RequiredXCom): CodeBlock {
  val type = TypeName.get(xcom.paramType)
  val accessor = NUMBER_ACCESSORS[type]
  val number = ClassName.get(Number::class.java)
  val optional = ClassName.get(Optional::class.java)
  // A primitive parameter cannot hold null, so fail with a clear error instead of an
  // opaque NullPointerException while unboxing when the XCom is absent.
  val value =
    if (type.isPrimitive) {
      CodeBlock.of(
        $$"$T.ofNullable(client.getXCom($S)).orElseThrow(() -> new $T($S, $S))",
        optional,
        xcom.taskId,
        ClassName.get(MissingXComException::class.java),
        xcom.taskId,
        xcom.paramName,
      )
    } else {
      CodeBlock.of($$"client.getXCom($S)", xcom.taskId)
    }
  // Wire integers decode to Long and floats to Double, so a direct (Integer)/(Float)
  // cast throws ClassCastException; widen via Number instead.
  return when {
    accessor == null -> CodeBlock.of($$"($T) $L", if (type.isPrimitive) type.box() else type, value)
    type.isPrimitive -> CodeBlock.of($$"(($T) $L).$L()", number, value, accessor)
    else ->
      CodeBlock.of(
        $$"$T.ofNullable(($T) $L).map($T::$L).orElse(null)",
        optional,
        number,
        value,
        number,
        accessor,
      )
  }
}

private data class BuildTaskResult(
  val spec: TypeSpec,
)
