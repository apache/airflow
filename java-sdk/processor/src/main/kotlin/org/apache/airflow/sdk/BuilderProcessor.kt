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
import org.apache.airflow.sdk.internal.TaskArgs
import org.apache.airflow.sdk.internal.TypeRef
import org.apache.airflow.sdk.internal.foldArgName
import org.apache.airflow.sdk.internal.registrarName
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
 *   `DagDef.config` calls, and a static `build()` that runs the class's
 *   [Builder.Deps] class and verifies it registered every task — or, when the
 *   class declares none, registers every task with no Java-side edges.
 * - A `*Deps` wiring-view interface (only when a [Builder.Deps] class exists)
 *   whose methods mirror the task methods: injectable parameters ([Client],
 *   [Context]) are dropped, data parameters become [Arg]-typed inputs, and the
 *   return value becomes a [TaskRef]. Calling one registers the task with its
 *   explicitly-written `@Builder.Task` attributes lowered into `TaskDef.config`
 *   calls; passing one call's handle to another wires the dependency edge and
 *   feeds the upstream's return-value XCom into the downstream's parameter,
 *   type-checked by javac through the [Arg] / [TaskRef] generics.
 *
 * In the generated `execute` bodies, a task's data parameters resolve against
 * the arg bindings the supervisor delivered for the run: flat parameters
 * through [TaskArgs], by their position among the data parameters, and
 * [TaskInput] fields through [ArgValues], by argument name. Non-`void` return values are
 * forwarded to `client.setXCom`.
 */
@SupportedAnnotationTypes(
  "org.apache.airflow.sdk.Builder.Dag",
  "org.apache.airflow.sdk.Builder.TaskHandler",
)
@SupportedSourceVersion(SourceVersion.RELEASE_11)
class BuilderProcessor : AbstractProcessor() {
  override fun process(
    annotations: Set<TypeElement>,
    roundEnv: RoundEnvironment,
  ): Boolean {
    if (annotations.isEmpty()) return false
    roundEnv
      .getElementsAnnotatedWith(Builder.TaskHandler::class.java)
      .mapNotNull { it.enclosingElement as? TypeElement }
      .distinct()
      .forEach { el ->
        with(processingEnv) {
          runCatching {
            JavaFile
              .builder(elementUtils.getPackageOf(el).qualifiedName.toString(), buildHandlers(el))
              .build()
              .writeTo(filer)
          }.onFailure { e -> messager.printMessage(Diagnostic.Kind.ERROR, e.message ?: "Unknown error", el) }
        }
      }
    roundEnv.getElementsAnnotatedWith(Builder.Dag::class.java).filterIsInstance<TypeElement>().forEach { el ->
      with(processingEnv) {
        runCatching {
          val packageName = elementUtils.getPackageOf(el).qualifiedName.toString()
          val declarations = collectTasks(el)
          val deps = findDeps(el)
          val builderName = ClassName.get(packageName, dagAnnotation(el).to.ifBlank { "${el.simpleName}Builder" })
          val depsName = ClassName.get(packageName, "${el.simpleName}Deps")
          JavaFile
            .builder(packageName, buildBuilder(el, declarations, deps, builderName))
            .build()
            .writeTo(filer)
          if (deps != null) {
            JavaFile.builder(packageName, buildDeps(el, declarations, builderName, depsName)).build().writeTo(filer)
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

  /**
   * Generates the registrar for a class of [Builder.TaskHandler] methods: one
   * [Task] implementation per handler, and a `registerInto` that binds each to
   * the Dag and task the annotation names.
   *
   * There is no Dag to build here — the Python Dag file owns it — so this is a
   * registrar rather than a builder.
   */
  private fun buildHandlers(el: TypeElement): TypeSpec {
    require(el.enclosingElement !is TypeElement || Modifier.STATIC in el.modifiers) {
      "Nested class '${el.simpleName}' holding @Builder.TaskHandler methods must be static"
    }
    val registrar =
      TypeSpec
        .classBuilder(registrarName(ClassName.get(el).reflectionName()).substringAfterLast('.'))
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)
        .addJavadoc(
          "Registers {@link \$T}'s task handlers against the Dags the Python file owns.\n",
          ClassName.get(el),
        )
    val registerInto =
      MethodSpec
        .methodBuilder("registerInto")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .addParameter(BUNDLE_TYPE, "bundle")

    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      val handler = inner.getAnnotation(Builder.TaskHandler::class.java) ?: continue
      if (inner.isVarArgs) {
        throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")
      }
      require(handler.dag.isNotBlank()) {
        "@Builder.TaskHandler on '${inner.simpleName}' must name the Dag the Python file declares"
      }
      val decl = TaskDeclaration(inner, handler.task.ifBlank { inner.simpleName.toString() }, collectDataParams(inner))
      registrar.addType(buildTask(decl, el))
      registerInto.addStatement(
        $$"bundle.register($S, $S, $L.class)",
        handler.dag,
        decl.id,
        decl.className,
      )
    }
    return registrar.addMethod(registerInto.build()).build()
  }

  private fun dagAnnotation(el: TypeElement): Builder.Dag = el.getAnnotation(Builder.Dag::class.java)!!

  private fun buildBuilder(
    el: TypeElement,
    declarations: List<TaskDeclaration>,
    deps: TypeElement?,
    builderName: ClassName,
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
    if (deps != null) {
      buildMethod.addStatement(
        $$"return $T.record(dag, $T.of($L), new $T()::depends)",
        REFS_TYPE,
        ClassName.get(List::class.java),
        declarations.joinToString { "\"${it.id}\"" },
        ClassName.get(deps),
      )
    } else {
      // No wiring class: register every task with no Java-side edges — a
      // Python stub Dag defines the graph for these tasks.
      declarations.forEach { decl ->
        buildMethod.addStatement($$"dag.addTask($L)", taskDefCode(decl, CodeBlock.of($$"$L", decl.className)))
      }
      buildMethod.addStatement("return dag")
    }
    builderClass.addMethod(buildMethod.build())

    declarations.forEach { builderClass.addType(buildTask(it, el)) }
    return builderClass.build()
  }

  /**
   * Generates the Dag's wiring view: one default method per task, with the
   * injected arguments stripped, each data argument lifted to [Arg] and the
   * return lifted to [TaskRef].
   *
   * It is an interface so the `@Builder.Deps` class can *implement* it and
   * keep its own `extends` free, and so the Dag class's real task methods --
   * which differ only in their injected arguments -- do not clash with it.
   */
  private fun buildDeps(
    el: TypeElement,
    declarations: List<TaskDeclaration>,
    builderName: ClassName,
    depsName: ClassName,
  ): TypeSpec {
    val view =
      TypeSpec
        .interfaceBuilder(depsName)
        .addModifiers(Modifier.PUBLIC)
        .addSuperinterface(DEPS_TYPE)
        .addJavadoc(
          "Wiring view of {@link \$T}'s task methods, for declaring its task graph.\n\n" +
            "<p>Calling one registers its task with the Dag being built; passing the handle it\n" +
            "returned into another call feeds the upstream's output into that task's parameter\n" +
            "and wires the data edge. {@code then} wires an ordering-only edge.\n",
          ClassName.get(el),
        )

    for (decl in declarations) {
      val method =
        MethodSpec
          .methodBuilder(decl.method.simpleName.toString())
          .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
          .returns(ParameterizedTypeName.get(TASK_HANDLE_TYPE, TypeName.get(decl.method.returnType).boxIfPossible()))
      decl.dataParams.forEach { method.addParameter(inType(it.type), it.name) }
      val def = taskDefCode(decl, CodeBlock.of($$"$T.$L", builderName, decl.className))
      if (decl.dataParams.isEmpty()) {
        method.addStatement($$"return $T.node($L)", REFS_TYPE, def)
      } else {
        method.addStatement(
          $$"return $T.call($L, $L)",
          REFS_TYPE,
          def,
          decl.dataParams.joinToString { it.name },
        )
      }
      view.addMethod(method.build())
    }
    return view.build()
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
   * parameters accept any numeric upstream (`Arg<? extends Number>`, widened
   * at run time); `Object`, raw `Map`, and raw `List` parameters accept any
   * upstream (`Arg<?>`, decoded loosely at run time); everything else accepts
   * covariant matches of the declared type (`Arg<? extends T>`).
   */
  private fun inType(paramType: TypeMirror): TypeName {
    val boxed = TypeName.get(paramType).boxIfPossible()
    val argument =
      when {
        isNumeric(paramType) -> WildcardTypeName.subtypeOf(TypeName.get(Number::class.java))
        else -> WildcardTypeName.subtypeOf(boxed)
      }
    return ParameterizedTypeName.get(ARG_TYPE, argument)
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
   * Finds and validates the class's `@Builder.Deps` wiring class. It is
   * optional: without one, every task registers with no Java-side edges.
   */
  private fun findDeps(el: TypeElement): TypeElement? {
    val classes =
      el.enclosedElements
        .filterIsInstance<TypeElement>()
        .filter { it.getAnnotation(Builder.Deps::class.java) != null }
    if (classes.isEmpty()) return null
    val deps =
      classes.singleOrNull()
        ?: throw IllegalArgumentException(
          "Dag class ${el.simpleName} declares more than one @Builder.Deps class: " +
            classes.joinToString { it.simpleName.toString() },
        )
    require(Modifier.STATIC in deps.modifiers && Modifier.PRIVATE !in deps.modifiers) {
      "@Builder.Deps class '${deps.simpleName}' must be static and non-private"
    }
    require(deps.enclosedElements.filterIsInstance<ExecutableElement>().any { it.isNoArgDepends() }) {
      "@Builder.Deps class '${deps.simpleName}' must declare a non-private, no-argument depends() method"
    }
    return deps
  }

  private fun ExecutableElement.isNoArgDepends(): Boolean =
    simpleName.contentEquals("depends") &&
      parameters.isEmpty() &&
      Modifier.PRIVATE !in modifiers &&
      Modifier.STATIC !in modifiers

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
            else -> dataByName.getValue(param.simpleName.toString()).local
          }
        }
      }

    val taken = decl.dataParams.mapTo(mutableSetOf()) { it.local }
    val argsLocal = generateSequence("args") { "${it}_" }.first { it !in taken }
    val flatParams = decl.dataParams.filterNot { it.isTaskInput }
    if (flatParams.isNotEmpty()) {
      executeSpec.addStatement(
        $$"$T $L = $T.of(context, client, $L)",
        TASK_ARGS_TYPE,
        argsLocal,
        TASK_ARGS_TYPE,
        flatParams.size,
      )
    }
    decl.dataParams.forEach { param ->
      val paramType = TypeName.get(param.type)
      if (param.isTaskInput) {
        executeSpec.addStatement(
          $$"$T $L = $T.bindInput(context, client, $T.class)",
          paramType,
          param.local,
          ARG_VALUES_TYPE,
          paramType,
        )
      } else {
        executeSpec.addStatement($$"$T $L = $L", paramType, param.local, positionalAccess(argsLocal, param))
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
   *
   * Each gets the local the generated body reads it into, which is its own
   * name unless that is one the body already uses: `execute`'s injected
   * `context` and `client` are in scope for the whole method, so a data
   * parameter sharing a name with one binds through a suffixed local instead.
   */
  private fun collectDataParams(method: ExecutableElement): List<DataParam> {
    val params = mutableListOf<DataParam>()
    val taken = mutableSetOf("context", "client")
    with(processingEnv) {
      for (param in method.parameters) {
        val type = param.asType()
        if (isType(type, CLIENT_TYPE) || isType(type, CONTEXT_TYPE)) continue
        val declaresTaskInput = isTaskInput(type)
        if (declaresTaskInput) validateTaskInput(method, param)
        val name = param.simpleName.toString()
        val local = generateSequence(name) { "${it}_" }.first { it !in taken }
        taken += local
        params += DataParam(type, name, local, params.size, declaresTaskInput)
      }
    }
    val inputs = params.filter { it.isTaskInput }
    require(inputs.size <= 1) {
      "Task method '${method.simpleName}' declares more than one TaskInput parameter: " +
        inputs.joinToString { "'${it.name}'" }
    }
    inputs.singleOrNull()?.let { input ->
      require(params.size == 1) {
        "Task method '${method.simpleName}' declares TaskInput parameter '${input.name}' and other data " +
          "parameters; a TaskInput owns the whole named-argument surface, so it must be the only one"
      }
    }
    return params
  }

  private fun ProcessingEnvironment.isTaskInput(type: TypeMirror): Boolean {
    val marker = elementUtils.getTypeElement(TASK_INPUT_TYPE.canonicalName()) ?: return false
    return !type.kind.isPrimitive && typeUtils.isAssignable(type, marker.asType())
  }

  /**
   * Checks at compile time that a [TaskInput] class can be populated at
   * runtime: [ArgValues.bindInput] assigns each public non-static non-final
   * field the argument it claims, by its [ArgName] value or by its own name
   * folded. Two fields whose names fold alike are rejected here rather than
   * at run time, since neither could be reached.
   */
  private fun ProcessingEnvironment.validateTaskInput(
    method: ExecutableElement,
    param: VariableElement,
  ) {
    val inputType =
      typeUtils.asElement(param.asType()) as? TypeElement
        ?: throw IllegalArgumentException(
          "TaskInput parameter '${param.simpleName}' of task method '${method.simpleName}' has no class type",
        )
    val hasNoArgConstructor =
      inputType.enclosedElements
        .filterIsInstance<ExecutableElement>()
        .any { it.kind == ElementKind.CONSTRUCTOR && it.parameters.isEmpty() && Modifier.PUBLIC in it.modifiers }
    require(hasNoArgConstructor) {
      "TaskInput class ${inputType.simpleName} needs a public no-argument constructor"
    }
    val claimed = mutableMapOf<String, String>()
    instanceFields(inputType).forEach { field ->
      require(Modifier.PUBLIC in field.modifiers && Modifier.FINAL !in field.modifiers) {
        "TaskInput field ${inputType.simpleName}.${field.simpleName} must be public and non-final " +
          "so the SDK can assign its binding"
      }
      val argName = field.getAnnotation(ArgName::class.java)?.value ?: field.simpleName.toString()
      val previous = claimed.put(foldArgName(argName), field.simpleName.toString())
      require(previous == null) {
        "TaskInput fields ${inputType.simpleName}.$previous and ${inputType.simpleName}.${field.simpleName} " +
          "claim argument names that differ only in case or underscores, which the fold cannot tell " +
          "apart; rename one of them"
      }
    }
  }

  /**
   * Every instance field [ArgValues.bindInput] will reach, subclass first —
   * the same walk up the superclass chain the runtime makes. Declared members
   * alone would miss an inherited field, and a private one would then surface
   * mid-run as the very failure the build-time check exists to prevent.
   */
  private fun ProcessingEnvironment.instanceFields(inputType: TypeElement): List<VariableElement> {
    val fields = mutableListOf<VariableElement>()
    var current: TypeElement? = inputType
    while (current != null && !current.qualifiedName.contentEquals("java.lang.Object")) {
      fields +=
        current.enclosedElements
          .filterIsInstance<VariableElement>()
          .filter { it.kind == ElementKind.FIELD && Modifier.STATIC !in it.modifiers }
      current = typeUtils.asElement(current.superclass) as? TypeElement
    }
    return fields
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
 * One data parameter of a task method, positioned among its peers, read into
 * [local] by the generated body. [isTaskInput] marks a [TaskInput] parameter,
 * which binds by field name instead.
 */
private class DataParam(
  val type: TypeMirror,
  val name: String,
  val local: String,
  val position: Int,
  val isTaskInput: Boolean,
)

private val DAG_DEF_TYPE = ClassName.get(DagDef::class.java)
private val TASK_DEF_TYPE = ClassName.get(TaskDef::class.java)
private val BUNDLE_TYPE = ClassName.get(Bundle::class.java)
private val CLIENT_TYPE = ClassName.get(Client::class.java)
private val CONTEXT_TYPE = ClassName.get(Context::class.java)
private val TASK_INPUT_TYPE = ClassName.get(TaskInput::class.java)
private val TASK_ARGS_TYPE = ClassName.get(TaskArgs::class.java)
private val TYPE_REF_TYPE = ClassName.get(TypeRef::class.java)
private val ARG_VALUES_TYPE = ClassName.get(ArgValues::class.java)
private val REFS_TYPE = ClassName.get(Refs::class.java)
private val ARG_TYPE = ClassName.get(Arg::class.java)
private val TASK_HANDLE_TYPE = ClassName.get(TaskRef::class.java)
private val DEPS_TYPE = ClassName.get(Deps::class.java)

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
 * Emits the read for one flat data parameter, bound at its position. A
 * primitive parameter cannot hold null, so `require` fails with a clear
 * [MissingXComException] when the binding resolves to nothing; boxed and
 * reference parameters take `get` and receive null instead.
 *
 * A parameter whose declared type has type arguments reads through [TypeRef]
 * so its element type survives the decode — a cast cannot express
 * `List<String>`, and erasure would let one succeed over a list of numbers
 * and fail much later at the first element read.
 */
private fun positionalAccess(
  argsLocal: String,
  param: DataParam,
): CodeBlock {
  val type = TypeName.get(param.type)
  val reader = if (type.isPrimitive) "require" else "get"
  val target =
    if (type is ParameterizedTypeName) {
      CodeBlock.of($$"new $T<$T>() {}", TYPE_REF_TYPE, type)
    } else {
      CodeBlock.of($$"$T.class", type.box())
    }
  return CodeBlock.of($$"$L.$L($L, $L)", argsLocal, reader, param.position, target)
}
