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
import com.squareup.javapoet.FieldSpec
import com.squareup.javapoet.JavaFile
import com.squareup.javapoet.MethodSpec
import com.squareup.javapoet.ParameterizedTypeName
import com.squareup.javapoet.TypeName
import com.squareup.javapoet.TypeSpec
import com.squareup.javapoet.WildcardTypeName
import org.apache.airflow.sdk.internal.ArgValues
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.FieldType
import org.apache.airflow.sdk.internal.Refs
import org.apache.airflow.sdk.internal.SchemaFields
import java.time.Duration
import java.time.OffsetDateTime
import java.time.format.DateTimeParseException
import javax.annotation.processing.AbstractProcessor
import javax.annotation.processing.ProcessingEnvironment
import javax.annotation.processing.RoundEnvironment
import javax.annotation.processing.SupportedAnnotationTypes
import javax.annotation.processing.SupportedSourceVersion
import javax.lang.model.SourceVersion
import javax.lang.model.element.AnnotationValue
import javax.lang.model.element.Element
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
 * For each class annotated with [Builder.Dag], generates:
 *
 * - A `*Builder` class containing one inner class per [Builder.Task]-annotated
 *   method (implementing [Task]), a `DAG_ID` constant, a static `dag()` factory
 *   that lowers every explicitly-written `@Builder.Dag` attribute into
 *   `DagDef.config` calls, and a static `build()` that invokes the class's
 *   [Wiring] method and verifies it registered every task — or, when the class
 *   has no wiring method, registers every task with no Java-side edges.
 * - A `*Ref` twin class (only when a wiring method exists) whose methods
 *   mirror the task methods: injectable parameters ([Client], [Context]) are
 *   dropped, data parameters become [In]-typed inputs, and the return value
 *   becomes a [TaskRef]. Calling a twin registers the task with its
 *   explicitly-written `@Builder.Task` attributes lowered into `TaskDef.config`
 *   calls; passing one twin's handle to another wires the dependency edge and
 *   feeds the upstream's return-value XCom into the downstream's parameter,
 *   type-checked by javac through the [In] / [TaskRef] generics.
 *
 * In the generated `execute` bodies, a task's data parameters resolve through
 * [ArgValues] against the arg bindings the supervisor delivered for the run,
 * falling back to the wired inputs: flat parameters by their position, and
 * [TaskInput] bundle fields by wire name. Non-`void` return values are
 * forwarded to `client.setXCom`.
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
          val packageName = elementUtils.getPackageOf(el).qualifiedName.toString()
          val declarations = collectTasks(el)
          val wiring = findWiring(el)
          val builderName = ClassName.get(packageName, dagAnnotation(el).to.ifBlank { "${el.simpleName}Builder" })
          val refName = ClassName.get(packageName, "${el.simpleName}Ref")
          JavaFile
            .builder(packageName, buildBuilder(el, declarations, wiring, builderName, refName))
            .build()
            .writeTo(filer)
          if (wiring != null) {
            JavaFile.builder(packageName, buildRef(el, declarations, builderName, refName)).build().writeTo(filer)
          }
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

  private fun dagAnnotation(el: TypeElement): Builder.Dag = el.getAnnotation(Builder.Dag::class.java)!!

  private fun buildBuilder(
    el: TypeElement,
    declarations: List<TaskDeclaration>,
    wiring: ExecutableElement?,
    builderName: ClassName,
    refName: ClassName,
  ): TypeSpec {
    val ann = dagAnnotation(el)

    val builderClass =
      TypeSpec
        .classBuilder(builderName)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
        .addField(
          FieldSpec
            .builder(ClassName.get(String::class.java), "DAG_ID", Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
            .initializer($$"$S", ann.id.ifBlank { el.simpleName })
            .build(),
        )

    val dagMethod =
      MethodSpec
        .methodBuilder("dag")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(DAG_DEF_TYPE)
        .addJavadoc("Returns a new {@code DagDef} carrying the Dag attributes, with no tasks registered.\n")
        .addStatement($$"var dag = new $T(DAG_ID)", DAG_DEF_TYPE)
    explicitConfig(el, DAG_ANNOTATION, DAG_STRUCTURAL_ATTRIBUTES, SchemaFields.DAG).forEach { (key, value) ->
      dagMethod.addStatement($$"dag.config($S, $L)", key, value)
    }
    dagMethod.addStatement("return dag")
    builderClass.addMethod(dagMethod.build())

    val buildMethod =
      MethodSpec
        .methodBuilder("build")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(DAG_DEF_TYPE)
        .addStatement("var dag = dag()")
    if (wiring != null) {
      buildMethod.addStatement(
        $$"$T.$L(new $T(dag))",
        ClassName.get(el),
        wiring.simpleName,
        refName,
      )
      buildMethod.addStatement(
        $$"$T.requireRegistered(dag, $T.of($L))",
        REFS_TYPE,
        ClassName.get(List::class.java),
        declarations.joinToString { "\"${it.id}\"" },
      )
    } else {
      // No wiring method: register every task with no Java-side edges — a
      // Python stub Dag defines the graph for these tasks.
      declarations.forEach { decl ->
        buildMethod.addStatement($$"dag.addTask($L)", taskDefCode(decl, CodeBlock.of($$"$L", decl.className)))
      }
    }
    buildMethod.addStatement("return dag")
    builderClass.addMethod(buildMethod.build())

    declarations.forEach { builderClass.addType(buildTask(it, el)) }
    return builderClass.build()
  }

  private fun buildRef(
    el: TypeElement,
    declarations: List<TaskDeclaration>,
    builderName: ClassName,
    refName: ClassName,
  ): TypeSpec {
    val flowClass =
      TypeSpec
        .classBuilder(refName)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
        .addJavadoc(
          "Task-reference twins of {@link \$T}'s task methods, for wiring its task graph.\n\n" +
            "<p>Calling a twin registers the task with the Dag under construction; passing one\n" +
            "twin's return value into another feeds the upstream's output into the downstream's\n" +
            "parameter and wires the dependency edge.\n",
          ClassName.get(el),
        ).addField(DAG_DEF_TYPE, "dag", Modifier.PRIVATE, Modifier.FINAL)
        .addMethod(
          MethodSpec
            .constructorBuilder()
            .addModifiers(Modifier.PUBLIC)
            .addParameter(DAG_DEF_TYPE, "dag")
            .addStatement("this.dag = dag")
            .build(),
        )

    for (decl in declarations) {
      val twin =
        MethodSpec
          .methodBuilder(decl.method.simpleName.toString())
          .addModifiers(Modifier.PUBLIC)
          .returns(ParameterizedTypeName.get(TASK_HANDLE_TYPE, TypeName.get(decl.method.returnType).boxIfPossible()))
      decl.dataParams.forEach { twin.addParameter(inType(it.type), it.name) }
      twin.addStatement(
        $$"return $T.register(dag, $L, $T.of($L))",
        REFS_TYPE,
        taskDefCode(decl, CodeBlock.of($$"$T.$L", builderName, decl.className)),
        ClassName.get(List::class.java),
        decl.dataParams.joinToString { it.name },
      )
      flowClass.addMethod(twin.build())
    }
    return flowClass.build()
  }

  /**
   * Emits `new TaskDef(id, <classRef>.class)` with the explicitly-written
   * `@Builder.Task` attributes lowered into chained `.config` calls.
   */
  private fun taskDefCode(
    decl: TaskDeclaration,
    classRef: CodeBlock,
  ): CodeBlock {
    val taskDef =
      CodeBlock
        .builder()
        .add($$"new $T($S, $L.class)", TASK_DEF_TYPE, decl.id, classRef)
    explicitConfig(decl.method, TASK_ANNOTATION, TASK_STRUCTURAL_ATTRIBUTES, SchemaFields.TASK).forEach { (key, value) ->
      taskDef.add($$".config($S, $L)", key, value)
    }
    return taskDef.build()
  }

  /**
   * Maps a data parameter's declared type to its twin-input type. Numeric
   * parameters accept any numeric upstream (`In<? extends Number>`, widened
   * at run time); `Object`, raw `Map`, and raw `List` parameters accept any
   * upstream (`In<?>`, decoded loosely at run time); everything else accepts
   * covariant matches of the declared type (`In<? extends T>`).
   */
  private fun inType(paramType: TypeMirror): TypeName {
    val boxed = TypeName.get(paramType).boxIfPossible()
    val argument =
      when {
        isNumeric(paramType) -> WildcardTypeName.subtypeOf(TypeName.get(Number::class.java))
        else -> WildcardTypeName.subtypeOf(boxed)
      }
    return ParameterizedTypeName.get(IN_TYPE, argument)
  }

  private fun isNumeric(t: TypeMirror): Boolean = t.kind in NUMERIC_KINDS || TypeName.get(t) in BOXED_NUMERICS

  private fun collectTasks(el: TypeElement): List<TaskDeclaration> {
    val declarations = mutableListOf<TaskDeclaration>()
    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      val ann = inner.getAnnotation(Builder.Task::class.java) ?: continue
      if (inner.isVarArgs) throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")
      val id = ann.id.ifBlank { inner.simpleName.toString() }
      require(declarations.none { it.id == id }) { "Tasks in Dag have duplicate ID: $id" }
      declarations += TaskDeclaration(inner, id, collectDataParams(inner))
    }
    return declarations
  }

  /**
   * Finds and validates the class's [Wiring] method. The method is optional:
   * without one, every task registers with no Java-side edges.
   */
  private fun findWiring(el: TypeElement): ExecutableElement? {
    val methods =
      el.enclosedElements
        .filterIsInstance<ExecutableElement>()
        .filter { it.getAnnotation(Wiring::class.java) != null }
    if (methods.isEmpty()) return null
    val wiring =
      methods.singleOrNull()
        ?: throw IllegalArgumentException(
          "Dag class ${el.simpleName} declares more than one @Wiring method: " +
            methods.joinToString { it.simpleName.toString() },
        )
    require(Modifier.STATIC in wiring.modifiers && Modifier.PRIVATE !in wiring.modifiers) {
      "@Wiring method '${wiring.simpleName}' must be static and non-private"
    }
    require(wiring.returnType.kind == TypeKind.VOID && wiring.parameters.size == 1) {
      "@Wiring method '${wiring.simpleName}' must be void and take the generated ${el.simpleName}Ref as its only parameter"
    }
    return wiring
  }

  /**
   * Lowers the explicitly-written configuration attributes of [element]'s
   * [annotationName] annotation into (schema key, value code) pairs. Only
   * attributes present at the use site are lowered, so annotation defaults
   * never override the schema's own defaults.
   */
  private fun explicitConfig(
    element: Element,
    annotationName: String,
    structural: Set<String>,
    table: Map<String, Field>,
  ): List<Pair<String, CodeBlock>> {
    val mirror =
      element.annotationMirrors.firstOrNull {
        (it.annotationType.asElement() as TypeElement).qualifiedName.contentEquals(annotationName)
      } ?: return emptyList()
    val byAttribute = table.values.associateBy { it.attribute }
    return mirror.elementValues.mapNotNull { (attr, value) ->
      val name = attr.simpleName.toString()
      if (name in structural) return@mapNotNull null
      val field =
        requireNotNull(byAttribute[name]) {
          "Annotation attribute '$name' has no Dag serialization schema key"
        }
      field.key to configValueCode(field, value)
    }
  }

  private fun configValueCode(
    field: Field,
    value: AnnotationValue,
  ): CodeBlock =
    when (field.type) {
      FieldType.STRING -> CodeBlock.of($$"$S", value.value)
      FieldType.BOOLEAN, FieldType.INTEGER, FieldType.NUMBER -> CodeBlock.of($$"$L", value.value)
      FieldType.STRING_ARRAY -> {
        @Suppress("UNCHECKED_CAST")
        val items = value.value as List<AnnotationValue>
        CodeBlock.of(
          $$"$T.of($L)",
          ClassName.get(List::class.java),
          items.joinToString { "\"${it.value}\"" },
        )
      }
      FieldType.TIMEDELTA -> {
        val text = value.value as String
        parseTemporal(field, text) { Duration.parse(text) }
        CodeBlock.of($$"$T.parse($S)", ClassName.get(Duration::class.java), text)
      }
      FieldType.DATETIME -> {
        val text = value.value as String
        parseTemporal(field, text) { OffsetDateTime.parse(text) }
        CodeBlock.of($$"$T.parse($S)", ClassName.get(OffsetDateTime::class.java), text)
      }
    }

  private fun parseTemporal(
    field: Field,
    text: String,
    parse: () -> Any,
  ) {
    try {
      parse()
    } catch (e: DateTimeParseException) {
      throw IllegalArgumentException("Annotation attribute '${field.attribute}' is not valid ISO-8601: '$text'")
    }
  }

  private fun buildTask(
    decl: TaskDeclaration,
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

    val inner = decl.method
    val dataByName = decl.dataParams.associateBy { it.name }
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

    decl.dataParams.forEach { param ->
      val paramType = TypeName.get(param.type)
      val fields = param.bundleFields
      if (fields == null) {
        executeSpec.addStatement($$"$T $L = $L", paramType, param.name, positionalAccess(param))
      } else {
        // Runtime bindings bind the bundle's fields by wire name; the
        // Java-wired fallback decodes the bundle wholesale from its single
        // wired input.
        executeSpec.addStatement($$"$T $L", paramType, param.name)
        executeSpec.beginControlFlow($$"if ($T.hasRuntimeBindings(client))", ARG_VALUES_TYPE)
        executeSpec.addStatement($$"$L = new $T()", param.name, paramType)
        fields.forEach { field ->
          executeSpec.addStatement($$"$L.$L = $L", param.name, field.fieldName, namedAccess(field))
        }
        executeSpec.nextControlFlow("else")
        executeSpec.addStatement($$"$L = $L", param.name, positionalAccess(param))
        executeSpec.endControlFlow()
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
      .classBuilder(decl.className)
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
        val bundleFields = if (isTaskInput(type)) collectBundleFields(method, param) else null
        params += DataParam(type, param.simpleName.toString(), params.size, bundleFields)
      }
    }
    val bundles = params.filter { it.bundleFields != null }
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
   * Introspects a [TaskInput] bundle class: every public non-static non-final
   * field receives the binding named by its [ArgName] value, or by its
   * verbatim field name.
   */
  private fun ProcessingEnvironment.collectBundleFields(
    method: ExecutableElement,
    param: VariableElement,
  ): List<BundleField> {
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
    return bundleType.enclosedElements
      .filterIsInstance<VariableElement>()
      .filter { it.kind == ElementKind.FIELD && Modifier.STATIC !in it.modifiers }
      .map { field ->
        require(Modifier.PUBLIC in field.modifiers && Modifier.FINAL !in field.modifiers) {
          "TaskInput field ${bundleType.simpleName}.${field.simpleName} must be public and non-final " +
            "so the generated code can assign its binding"
        }
        BundleField(
          type = field.asType(),
          fieldName = field.simpleName.toString(),
          wireName = field.getAnnotation(ArgName::class.java)?.value ?: field.simpleName.toString(),
        )
      }
  }
}

/** One [Builder.Task]-annotated method with its resolved id and data parameters. */
private class TaskDeclaration(
  val method: ExecutableElement,
  val id: String,
  val dataParams: List<DataParam>,
) {
  val className: String = method.simpleName.toString().replaceFirstChar(Char::uppercase)
}

/**
 * One data parameter of a task method, positioned among its peers.
 * [bundleFields] is non-null for a [TaskInput] bundle parameter.
 */
private class DataParam(
  val type: TypeMirror,
  val name: String,
  val position: Int,
  val bundleFields: List<BundleField>?,
)

/** One public field of a [TaskInput] bundle class, with its wire name. */
private class BundleField(
  val type: TypeMirror,
  val fieldName: String,
  val wireName: String,
)

private val DAG_DEF_TYPE = ClassName.get(DagDef::class.java)
private val TASK_DEF_TYPE = ClassName.get(TaskDef::class.java)
private val CLIENT_TYPE = ClassName.get(Client::class.java)
private val CONTEXT_TYPE = ClassName.get(Context::class.java)
private val TASK_INPUT_TYPE = ClassName.get(TaskInput::class.java)
private val ARG_VALUES_TYPE = ClassName.get(ArgValues::class.java)
private val REFS_TYPE = ClassName.get(Refs::class.java)
private val IN_TYPE = ClassName.get(In::class.java)
private val TASK_HANDLE_TYPE = ClassName.get(TaskRef::class.java)

private const val DAG_ANNOTATION = "org.apache.airflow.sdk.Builder.Dag"
private const val TASK_ANNOTATION = "org.apache.airflow.sdk.Builder.Task"

private val DAG_STRUCTURAL_ATTRIBUTES = setOf("id", "to")
private val TASK_STRUCTURAL_ATTRIBUTES = setOf("id")

private val NUMERIC_KINDS =
  setOf(TypeKind.BYTE, TypeKind.SHORT, TypeKind.INT, TypeKind.LONG, TypeKind.FLOAT, TypeKind.DOUBLE)

private val BOXED_NUMERICS: Set<TypeName> =
  setOf(TypeName.BYTE, TypeName.SHORT, TypeName.INT, TypeName.LONG, TypeName.FLOAT, TypeName.DOUBLE)
    .mapTo(mutableSetOf()) { it.box() }

private fun TypeName.boxIfPossible(): TypeName = if (this == TypeName.VOID || isPrimitive) box() else this

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

/** Emits the resolve-and-decode expression for one bundle field, bound by wire name. */
private fun namedAccess(field: BundleField): CodeBlock {
  val type = TypeName.get(field.type)
  return if (type.isPrimitive) {
    CodeBlock.of(
      $$"$T.requiredNamed(client, $S, $T.class, $S)",
      ARG_VALUES_TYPE,
      field.wireName,
      type.box(),
      field.fieldName,
    )
  } else {
    val raw = (type as? ParameterizedTypeName)?.rawType ?: type
    val call = CodeBlock.of($$"$T.optionalNamed(client, $S, $T.class)", ARG_VALUES_TYPE, field.wireName, raw)
    if (type is ParameterizedTypeName) CodeBlock.of($$"($T) $L", type, call) else call
  }
}
