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
import com.squareup.javapoet.ParameterizedTypeName
import com.squareup.javapoet.TypeName
import com.squareup.javapoet.TypeSpec
import org.apache.airflow.sdk.internal.ArgValues
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.annotation.processing.SupportedSourceVersion
import javax.lang.model.SourceVersion
import javax.lang.model.element.ElementKind
import javax.lang.model.element.ExecutableElement
import javax.lang.model.element.Modifier
import javax.lang.model.element.TypeElement
import javax.lang.model.element.VariableElement
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
 * - A static `build()` method that constructs the [DagDef] and registers those
 *   inner classes as [TaskDef]s.
 *
 * In the generated `execute` body, a task's data parameters resolve through
 * [ArgValues] against the arg bindings the supervisor delivered for the run:
 * flat parameters by their position among the data parameters, [TaskInput]
 * bundle fields by wire name. Non-`void` return values are forwarded to
 * `client.setXCom`.
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
        .returns(DAG_DEF_TYPE)
        .addStatement($$"var dag = new $T($S)", DAG_DEF_TYPE, ann.id.ifBlank { el.simpleName })

    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      if (inner.isVarArgs) throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")

      val taskAnn = inner.getAnnotation(Builder.Task::class.java) ?: continue
      val innerName = inner.simpleName.toString().replaceFirstChar(Char::uppercase)

      builderClass.addType(buildTask(innerName, inner, el))

      buildMethod.addStatement(
        $$"dag.addTask(new $T($S, $L.class))",
        TASK_DEF_TYPE,
        taskAnn.id.ifBlank { inner.simpleName },
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
  ): TypeSpec {
    val executeSpec =
      MethodSpec
        .methodBuilder("execute")
        .addAnnotation(Override::class.java)
        .addModifiers(Modifier.PUBLIC)
        .returns(TypeName.VOID)
        .addParameter(CONTEXT_TYPE, "context")
        .addParameter(CLIENT_TYPE, "client")
        .addException(Exception::class.java)

    val dataParams = collectDataParams(inner)
    val dataByName = dataParams.associateBy { it.name }
    val innerArgs =
      with(processingEnv) {
        inner.parameters.joinToString { param ->
          val type = param.asType()
          when {
            isType(type, CLIENT_TYPE) -> "client"
            isType(type, CONTEXT_TYPE) -> "context"
            else -> dataByName.getValue(param.simpleName.toString()).name
          }
        }
      }

    dataParams.forEach { param ->
      val paramType = TypeName.get(param.type)
      if (param.isBundle) {
        executeSpec.addStatement(
          $$"$T $L = $T.bindInput(context, client, $T.class)",
          paramType,
          param.name,
          ARG_VALUES_TYPE,
          paramType,
        )
      } else {
        executeSpec.addStatement($$"$T $L = $L", paramType, param.name, positionalAccess(param))
      }
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

    return TypeSpec
      .classBuilder(name)
      .addSuperinterface(Task::class.java)
      .addModifiers(Modifier.PUBLIC, Modifier.FINAL, Modifier.STATIC)
      .addMethod(executeSpec.build())
      .build()
  }

  /**
   * Collects the task method's data parameters — every parameter the SDK does
   * not inject — in declaration order. A parameter's index in the returned
   * list is the position it binds at: Java parameter names are not API, so
   * renaming one must never rebind an input.
   */
  private fun collectDataParams(method: ExecutableElement): List<DataParam> {
    val params = mutableListOf<DataParam>()
    with(processingEnv) {
      for (param in method.parameters) {
        val type = param.asType()
        if (isType(type, CLIENT_TYPE) || isType(type, CONTEXT_TYPE)) continue
        val isBundle = isTaskInput(type)
        // TaskArgs is the SDK's own TaskInput: it holds no fields to check and
        // the SDK, not the user, constructs it.
        if (isBundle && !isType(type, TASK_ARGS_TYPE)) validateBundle(method, param)
        params += DataParam(type, param.simpleName.toString(), params.size, isBundle)
      }
    }
    val bundles = params.filter { it.isBundle }
    require(bundles.size <= 1) {
      "Task method '${method.simpleName}' declares more than one TaskInput parameter: " +
        bundles.joinToString { "'${it.name}'" }
    }
    bundles.singleOrNull()?.let { bundle ->
      require(params.size == 1) {
        "Task method '${method.simpleName}' declares TaskInput parameter '${bundle.name}' and other data " +
          "parameters; a TaskInput bundle owns the whole named-argument surface, so it must be the only one"
      }
    }
    return params
  }

  private fun ProcessingEnvironment.isTaskInput(type: TypeMirror): Boolean {
    val marker = elementUtils.getTypeElement(TASK_INPUT_TYPE.canonicalName()) ?: return false
    return !type.kind.isPrimitive && typeUtils.isAssignable(type, marker.asType())
  }

  /**
   * Checks at compile time that a [TaskInput] bundle class can be populated at
   * runtime: [ArgValues.bindInput] assigns each public non-static non-final
   * field the binding named by its [ArgName] value, or by its verbatim field
   * name.
   */
  private fun ProcessingEnvironment.validateBundle(
    method: ExecutableElement,
    param: VariableElement,
  ) {
    val bundleType =
      typeUtils.asElement(param.asType()) as? TypeElement
        ?: throw IllegalArgumentException(
          "TaskInput parameter '${param.simpleName}' of task method '${method.simpleName}' has no class type",
        )
    val hasNoArgConstructor =
      bundleType.enclosedElements
        .filterIsInstance<ExecutableElement>()
        .any { it.kind == ElementKind.CONSTRUCTOR && it.parameters.isEmpty() && Modifier.PUBLIC in it.modifiers }
    require(hasNoArgConstructor) {
      "TaskInput class ${bundleType.simpleName} needs a public no-argument constructor"
    }
    bundleType.enclosedElements
      .filterIsInstance<VariableElement>()
      .filter { it.kind == ElementKind.FIELD && Modifier.STATIC !in it.modifiers }
      .forEach { field ->
        require(Modifier.PUBLIC in field.modifiers && Modifier.FINAL !in field.modifiers) {
          "TaskInput field ${bundleType.simpleName}.${field.simpleName} must be public and non-final " +
            "so the SDK can assign its binding"
        }
      }
  }
}

/**
 * One data parameter of a task method, positioned among its peers.
 * [isBundle] marks a [TaskInput] parameter, which binds by field name instead.
 */
private class DataParam(
  val type: TypeMirror,
  val name: String,
  val position: Int,
  val isBundle: Boolean,
)

private val DAG_DEF_TYPE = ClassName.get(DagDef::class.java)
private val TASK_DEF_TYPE = ClassName.get(TaskDef::class.java)
private val CLIENT_TYPE = ClassName.get(Client::class.java)
private val CONTEXT_TYPE = ClassName.get(Context::class.java)
private val TASK_INPUT_TYPE = ClassName.get(TaskInput::class.java)
private val TASK_ARGS_TYPE = ClassName.get(TaskArgs::class.java)
private val ARG_VALUES_TYPE = ClassName.get(ArgValues::class.java)

private fun ProcessingEnvironment.isType(
  t: TypeMirror,
  c: ClassName,
): Boolean = typeUtils.isSameType(t, elementUtils.getTypeElement(c.canonicalName()).asType())

/**
 * Emits the resolve-and-decode expression for one flat data parameter, bound
 * at its position. A primitive parameter cannot hold null, so it fails with a
 * clear [MissingXComException] when the binding resolves to nothing; boxed and
 * reference parameters receive null instead.
 */
private fun positionalAccess(param: DataParam): CodeBlock {
  val type = TypeName.get(param.type)
  return if (type.isPrimitive) {
    CodeBlock.of(
      $$"$T.requiredInput(context, client, $L, $T.class, $S)",
      ARG_VALUES_TYPE,
      param.position,
      type.box(),
      param.name,
    )
  } else {
    val raw = (type as? ParameterizedTypeName)?.rawType ?: type
    val call = CodeBlock.of($$"$T.optionalInput(context, client, $L, $T.class)", ARG_VALUES_TYPE, param.position, raw)
    if (type is ParameterizedTypeName) CodeBlock.of($$"($T) $L", type, call) else call
  }
}
