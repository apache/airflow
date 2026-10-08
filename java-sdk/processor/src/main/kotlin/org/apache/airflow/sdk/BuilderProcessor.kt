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
import org.apache.airflow.sdk.internal.DagSource
import org.apache.airflow.sdk.internal.Field
import org.apache.airflow.sdk.internal.FieldType
import org.apache.airflow.sdk.internal.GROUP_ID
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
import javax.lang.model.element.AnnotationMirror
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
import java.lang.reflect.Modifier as ReflectModifier
import org.apache.airflow.sdk.internal.builderName as generatedBuilderName

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
 *   method (implementing [Task]), and a static `build()` that constructs the
 *   [DagDef], lowers every explicitly-written `@Builder.Dag` attribute into a
 *   `DagDef.config` call, then runs the class's [Builder.Deps] class and
 *   verifies it registered every task.
 * - A `*Deps` wiring-view interface whose methods mirror the task methods:
 *   injectable parameters ([Client],
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
  "org.apache.airflow.sdk.Builder.Task",
  "org.apache.airflow.sdk.Builder.If",
  "org.apache.airflow.sdk.Builder.Branch",
  "org.apache.airflow.sdk.Builder.TaskGroup",
  "org.apache.airflow.sdk.Builder.TaskHandler",
  "org.apache.airflow.sdk.Builder.Deps",
)
@SupportedSourceVersion(SourceVersion.RELEASE_11)
class BuilderProcessor : AbstractProcessor() {
  override fun process(
    annotations: Set<TypeElement>,
    roundEnv: RoundEnvironment,
  ): Boolean {
    if (annotations.isEmpty()) return false
    roundEnv.getElementsAnnotatedWith(Builder.Deps::class.java).forEach { el ->
      val owner = el.enclosingElement
      if (owner !is TypeElement || owner.getAnnotation(Builder.Dag::class.java) == null) {
        processingEnv.messager.printMessage(
          Diagnostic.Kind.ERROR,
          "@Builder.Deps class '${el.simpleName}' must be nested directly in a @Builder.Dag class",
          el,
        )
      }
    }
    roundEnv.getElementsAnnotatedWith(Builder.TaskGroup::class.java).forEach { el ->
      val owner = el.enclosingElement
      val nested =
        owner is TypeElement &&
          (owner.getAnnotation(Builder.Dag::class.java) != null || owner.getAnnotation(Builder.TaskGroup::class.java) != null)
      if (!nested) {
        processingEnv.messager.printMessage(
          Diagnostic.Kind.ERROR,
          "@Builder.TaskGroup class '${el.simpleName}' must be nested in a @Builder.Dag class or in " +
            "another @Builder.TaskGroup class",
          el,
        )
      }
    }
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
          val scope = collectScope(el, emptyList(), emptyList())
          checkIds(scope)
          val builderName =
            ClassName.get(
              packageName,
              generatedBuilderName(packageName, el.simpleName.toString(), dagAnnotation(el).to).substringAfterLast('.'),
            )
          val depsName = ClassName.get(packageName, "${el.simpleName}Deps")
          val deps = findDeps(el, depsName)
          checkViewNames(scope)
          JavaFile
            .builder(packageName, buildBuilder(el, scope, deps, builderName))
            .build()
            .writeTo(filer)
          JavaFile.builder(packageName, buildDeps(el, scope, builderName, depsName)).build().writeTo(filer)
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

    val names = mutableSetOf<String>()
    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      val handler = inner.getAnnotation(Builder.TaskHandler::class.java) ?: continue
      if (inner.isVarArgs) {
        throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")
      }
      require(handler.dag.isNotBlank()) {
        "@Builder.TaskHandler on '${inner.simpleName}' must name the Dag the Python file declares"
      }
      val decl = TaskDeclaration(inner, handler.task.ifBlank { inner.simpleName.toString() }, collectDataParams(inner), el)
      require(names.add(inner.simpleName.toString())) {
        "Class ${el.simpleName} overloads task-handler method '${inner.simpleName}'; a method's name is " +
          "the name of its generated task class, so rename one and keep its task id with " +
          "@Builder.TaskHandler(task = \"${decl.id}\")"
      }
      registrar.addType(buildTask(decl))
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
    scope: Scope,
    deps: TypeElement,
    builderName: ClassName,
  ): TypeSpec {
    val declarations = scope.allTasks()
    val ann = dagAnnotation(el)

    val builderClass =
      TypeSpec
        .classBuilder(builderName)
        .addModifiers(Modifier.PUBLIC, Modifier.FINAL)

    val buildMethod =
      MethodSpec
        .methodBuilder("build")
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
        .returns(DAG_DEF_TYPE)
        .addStatement(
          $$"var dag = $T.declaredBy(new $T($S), $T.class)",
          DAG_SOURCE_TYPE,
          DAG_DEF_TYPE,
          ann.id.ifBlank { el.simpleName },
          ClassName.get(el),
        )
    explicitConfig(el, DAG_ANNOTATION, DAG_STRUCTURAL_ATTRIBUTES, SchemaFields.DAG).forEach { (key, value) ->
      buildMethod.addStatement($$"dag.config($S, $L)", key, value)
    }
    val taskIds = CodeBlock.join(declarations.map { CodeBlock.of($$"$S", it.id) }, ", ")
    val groupIds = CodeBlock.join(scope.allGroups().map { CodeBlock.of($$"$S", it.fullId) }, ", ")
    buildMethod.addStatement(
      $$"return $T.record(dag, $T.of($L), $T.of($L), new $T()::depends)",
      REFS_TYPE,
      LIST_TYPE,
      taskIds,
      LIST_TYPE,
      groupIds,
      ClassName.get(deps),
    )
    builderClass.addMethod(buildMethod.build())

    declarations.filterNot { it.kind == TaskKind.TRIGGER }.forEach { builderClass.addType(buildTask(it)) }
    // Only a branch needs to name a task in code, so a Dag without one keeps
    // the generated builder to the classes that run its tasks.
    if (declarations.any { it.kind == TaskKind.BRANCH }) builderClass.addType(buildTaskIds(declarations))
    return builderClass.build()
  }

  /**
   * Generates the `TaskIds` holder a `@Builder.Branch` method names its case
   * with: one constant per task of the Dag, so a choice that is not a task of
   * this Dag does not compile.
   */
  private fun buildTaskIds(declarations: List<TaskDeclaration>): TypeSpec {
    val holder =
      TypeSpec
        .classBuilder(TASK_IDS)
        .addModifiers(Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
        .addJavadoc(
          "The task ids of this Dag, for a {@code @Builder.Branch} method to name its case with.\n\n" +
            "<p>Every task of the Dag has one, so naming a task that is not a case of the branch\n" +
            "compiles and fails when the branch runs.\n",
        )
    declarations.forEach { decl ->
      holder.addField(
        FieldSpec
          .builder(TASK_ID_TYPE, constantName(decl.id), Modifier.PUBLIC, Modifier.STATIC, Modifier.FINAL)
          .initializer($$"$T.of($S)", TASK_ID_TYPE, decl.id)
          .build(),
      )
    }
    return holder.build()
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
    scope: Scope,
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
            "and wires the data edge. {@code before} and {@code after} wire an ordering-only edge.\n" +
            "<p>A task group is reached by calling it, and stands at either end of an edge:\n" +
            "{@code staging().stage(rows)} and {@code extract().before(staging())}.\n",
          ClassName.get(el),
        )
    addScope(view, scope, builderName, depsName)
    return view.build()
  }

  /** Adds one scope's task methods, and a nested interface plus accessor per group it holds. */
  private fun addScope(
    view: TypeSpec.Builder,
    scope: Scope,
    builderName: ClassName,
    viewName: ClassName,
    inGroup: Boolean = false,
  ) {
    scope.tasks.forEach { view.addMethod(viewMethod(it, builderName, inGroup)) }
    for (group in scope.groups) {
      val nested = viewName.nestedClass(group.element.simpleName.toString())
      view.addMethod(
        MethodSpec
          .methodBuilder(group.accessor)
          .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
          .returns(nested)
          .addJavadoc("The task group {@code \$L}, and everything declared in it.\n", group.fullId)
          .addStatement($$"return new $T() {}", nested)
          .build(),
      )
      val groupView =
        TypeSpec
          .interfaceBuilder(nested)
          .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
          .addSuperinterface(GROUP_TYPE)
          .addJavadoc("Wiring view of the task group {@code \$L}.\n", group.fullId)
          .addMethod(
            MethodSpec
              .methodBuilder("groupId")
              .addAnnotation(Override::class.java)
              .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
              .returns(String::class.java)
              .addStatement($$"return $S", group.fullId)
              .build(),
          )
      addScope(groupView, group.scope, builderName, nested, inGroup = true)
      view.addType(groupView.build())
    }
  }

  /** One task's method on the wiring view: injected arguments stripped, inputs lifted to [Arg]. */
  private fun viewMethod(
    decl: TaskDeclaration,
    builderName: ClassName,
    inGroup: Boolean,
  ): MethodSpec {
    val method =
      MethodSpec
        .methodBuilder(decl.method.simpleName.toString())
        .addModifiers(Modifier.PUBLIC, Modifier.DEFAULT)
        .returns(
          when {
            decl.kind.refType != null -> decl.kind.refType
            decl.kind == TaskKind.TRIGGER -> ParameterizedTypeName.get(TASK_HANDLE_TYPE, VOID_TYPE)
            else -> ParameterizedTypeName.get(TASK_HANDLE_TYPE, TypeName.get(decl.method.returnType).boxIfPossible())
          },
        )
    decl.dataParams.forEach { method.addParameter(inType(it.type), it.name) }
    val def = taskDefCode(decl, CodeBlock.of($$"$T.$L", builderName, decl.className))
    // The view knows the group it belongs to, so the recorder is told where
    // the task goes instead of deriving it from the task's ID.
    val group = if (inGroup) CodeBlock.of("groupId()") else CodeBlock.of($$"$S", "")
    val wrap = { call: CodeBlock ->
      decl.kind.refType?.let { CodeBlock.of($$"$T.of($L)", it, call) } ?: call
    }
    if (decl.dataParams.isEmpty()) {
      method.addStatement($$"return $L", wrap(CodeBlock.of($$"$T.node($L, $L)", REFS_TYPE, group, def)))
    } else {
      // The parameter names ride along so the serialized Dag can name each
      // argument, as the binding spec ADR-0007 defines requires.
      method.addStatement(
        $$"return $L",
        wrap(
          CodeBlock.of(
            $$"$T.call($L, $L, $T.of($L), $L)",
            REFS_TYPE,
            group,
            def,
            LIST_TYPE,
            CodeBlock.join(decl.dataParams.map { CodeBlock.of($$"$S", it.name) }, ", "),
            decl.dataParams.joinToString { it.name },
          ),
        ),
      )
    }
    return method.build()
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
        .apply {
          if (decl.kind == TaskKind.TRIGGER) {
            add($$"new $T($S, new $T().$L())", TASK_DEF_TYPE, decl.id, ClassName.get(decl.owner), decl.method.simpleName)
          } else {
            add($$"new $T($S, $L.class)", TASK_DEF_TYPE, decl.id, classRef)
          }
        }
    explicitConfig(decl.method, decl.kind.annotation, TASK_STRUCTURAL_ATTRIBUTES, SchemaFields.TASK).forEach { (key, value) ->
      taskDef.add($$".config($S, $L)", key, value)
    }
    return taskDef.build()
  }

  /**
   * Maps a data parameter's declared type to its wiring-view input type,
   * `Arg<? extends T>` of the boxed type. A numeric parameter therefore takes
   * only its own type, so javac rejects wiring that could lose a value, such
   * as a `double` upstream into a `long` parameter. An `Object` parameter
   * takes any upstream, including a `void` task's handle, whose value is null.
   */
  private fun inType(paramType: TypeMirror): TypeName =
    ParameterizedTypeName.get(ARG_TYPE, WildcardTypeName.subtypeOf(TypeName.get(paramType).boxIfPossible()))

  /** The Dag's tasks and task groups, read from the class tree the author wrote. */
  private fun collectScope(
    el: TypeElement,
    path: List<String>,
    classPath: List<String>,
  ): Scope {
    val tasks = mutableListOf<TaskDeclaration>()
    for (inner in el.enclosedElements) {
      if (inner !is ExecutableElement) continue
      val declared =
        TaskKind.declaring.filter { kind -> inner.annotationMirrors.any { it.names(kind.annotation) } }
      if (declared.isEmpty()) continue
      val declaredKind =
        declared.singleOrNull()
          ?: throw IllegalArgumentException(
            "Method '${inner.simpleName}' carries ${declared.joinToString { it.spelling }}; a task is " +
              "declared by one of them alone",
          )
      // A task method that hands back a TriggerDagRun declares what to trigger
      // rather than a body to run, so it is read when the Dag is built.
      val triggers =
        declaredKind == TaskKind.TASK && with(processingEnv) { isType(inner.returnType, TRIGGER_TYPE) }
      val kind = if (triggers) TaskKind.TRIGGER else declaredKind
      val annotated = declaredId(inner, kind)
      if (inner.isVarArgs) throw IllegalArgumentException("Cannot create task from vararg function ${inner.simpleName}")
      checkDeciderReturn(kind, inner)
      if (kind == TaskKind.TRIGGER) {
        require(inner.parameters.isEmpty()) {
          "@Builder.Task method '${inner.simpleName}' returns a TriggerDagRun, so it runs when the Dag " +
            "is built rather than when the task runs; it takes no parameters"
        }
      }
      val localId = annotated.ifBlank { inner.simpleName.toString() }
      require(tasks.none { it.method.simpleName.contentEquals(inner.simpleName) }) {
        "Class ${el.simpleName} overloads task method '${inner.simpleName}'; a method's name is the " +
          "name of its generated task class and of its wiring-view method, so rename one and keep its " +
          "task id with ${kind.spelling}(id = \"$localId\")"
      }
      tasks +=
        TaskDeclaration(inner, (path + localId).joinToString("."), collectDataParams(inner), el, classPath, kind)
    }

    val groups = mutableListOf<GroupDeclaration>()
    for (inner in el.enclosedElements.filterIsInstance<TypeElement>()) {
      val ann = inner.getAnnotation(Builder.TaskGroup::class.java) ?: continue
      val localId = checkGroupClass(inner, ann)
      require(groups.none { it.id == localId }) {
        "Class ${el.simpleName} declares more than one task group '$localId'"
      }
      val scope = collectScope(inner, path + localId, classPath + inner.simpleName.toString())
      groups += GroupDeclaration(inner, localId, (path + localId).joinToString("."), scope)
    }
    return Scope(tasks, groups)
  }

  /** Checks that `new <group class>()` compiles and names a valid group, and returns its local ID. */
  private fun checkGroupClass(
    el: TypeElement,
    ann: Builder.TaskGroup,
  ): String {
    val name = el.simpleName
    require(el.kind == ElementKind.CLASS && Modifier.ABSTRACT !in el.modifiers) {
      "@Builder.TaskGroup '$name' must be a concrete class"
    }
    require(Modifier.STATIC in el.modifiers && Modifier.PRIVATE !in el.modifiers) {
      "@Builder.TaskGroup class '$name' must be static and non-private"
    }
    require(
      el.enclosedElements
        .filterIsInstance<ExecutableElement>()
        .any { it.kind == ElementKind.CONSTRUCTOR && it.parameters.isEmpty() && Modifier.PRIVATE !in it.modifiers },
    ) {
      "@Builder.TaskGroup class '$name' needs a non-private no-argument constructor"
    }
    val id = ann.id.ifBlank { name.toString() }
    require(GROUP_ID.matches(id)) {
      "Task group ID '$id' must contain only ASCII letters, digits, underscores, or dashes"
    }
    return id
  }

  /**
   * Finds and validates the class's `@Builder.Deps` wiring class, which
   * declares the Dag's task graph and is what makes it a Dag Java owns.
   *
   * The generated builder runs `new Wiring()::depends`, so everything that
   * expression needs is checked here, where the error can name the class.
   */
  private fun findDeps(
    el: TypeElement,
    view: ClassName,
  ): TypeElement {
    val classes =
      el.enclosedElements
        .filterIsInstance<TypeElement>()
        .filter { it.getAnnotation(Builder.Deps::class.java) != null }
    require(classes.isNotEmpty()) {
      "Dag class ${el.simpleName} must declare a @Builder.Deps class implementing ${view.simpleName()} " +
        "to declare its task graph; a class of task bodies for a Dag the Python file owns carries " +
        "@Builder.TaskHandler instead"
    }
    val deps =
      classes.singleOrNull()
        ?: throw IllegalArgumentException(
          "Dag class ${el.simpleName} declares more than one @Builder.Deps class: " +
            classes.joinToString { it.simpleName.toString() },
        )
    val name = deps.simpleName
    require(deps.kind == ElementKind.CLASS && Modifier.ABSTRACT !in deps.modifiers) {
      "@Builder.Deps '$name' must be a concrete class"
    }
    require(Modifier.STATIC in deps.modifiers && Modifier.PRIVATE !in deps.modifiers) {
      "@Builder.Deps class '$name' must be static and non-private"
    }
    require(deps.interfaces.any { it.isView(view) }) {
      "@Builder.Deps class '$name' must implement ${view.simpleName()}, the wiring view of ${el.simpleName}"
    }
    require(
      deps.enclosedElements
        .filterIsInstance<ExecutableElement>()
        .any { it.kind == ElementKind.CONSTRUCTOR && it.parameters.isEmpty() && Modifier.PRIVATE !in it.modifiers },
    ) {
      "@Builder.Deps class '$name' needs a non-private no-argument constructor"
    }
    val depends =
      processingEnv.elementUtils
        .getAllMembers(deps)
        .filterIsInstance<ExecutableElement>()
        .firstOrNull { it.isNoArgDepends() }
        ?: throw IllegalArgumentException(
          "@Builder.Deps class '$name' must have a non-private, no-argument depends() method",
        )
    val checked = depends.thrownTypes.filterNot { isUnchecked(it) }
    require(checked.isEmpty()) {
      "depends() of @Builder.Deps class '$name' must not throw checked exceptions: ${checked.joinToString()}"
    }
    return deps
  }

  /**
   * Rejects two tasks of the Dag sharing an ID, a task and a task group
   * sharing one, and two task methods whose generated classes would collide.
   */
  private fun checkIds(scope: Scope) {
    val declarations = scope.allTasks()
    val taskIds = mutableSetOf<String>()
    declarations.forEach { decl ->
      require(taskIds.add(decl.id)) { "Tasks in Dag have duplicate ID: ${decl.id}" }
    }
    scope.allGroups().forEach { group ->
      require(group.fullId !in taskIds) {
        "Dag has both a task and a task group with ID '${group.fullId}'; rename one"
      }
    }
    if (declarations.any { it.kind == TaskKind.BRANCH }) {
      val byConstant = mutableMapOf<String, TaskDeclaration>()
      declarations.forEach { decl ->
        val constant = constantName(decl.id)
        require(SourceVersion.isName(constant)) {
          "Task '${decl.id}' becomes the constant '$constant' of the generated TaskIds, which is not " +
            "a Java name; give the task an id a branch can name it by, with ${decl.kind.spelling}(id = \"...\")"
        }
        byConstant.put(constant, decl)?.let { first ->
          throw IllegalArgumentException(
            "Tasks '${first.id}' and '${decl.id}' both become the constant " +
              "'$constant' of the generated TaskIds; rename one so a branch can tell them apart",
          )
        }
      }
      declarations.firstOrNull { it.className == TASK_IDS }?.let { decl ->
        throw IllegalArgumentException(
          "Task method '${decl.method.simpleName}' generates the class '$TASK_IDS', which is the " +
            "holder of this Dag's task ids; rename the method and keep its task id with " +
            "${decl.kind.spelling}(id = \"${decl.id.substringAfterLast('.')}\")",
        )
      }
    }
    val byClassName = mutableMapOf<String, TaskDeclaration>()
    // A task that triggers a Dag run has no generated class to collide with.
    declarations.filterNot { it.kind == TaskKind.TRIGGER }.forEach { decl ->
      byClassName.put(decl.className, decl)?.let { first ->
        throw IllegalArgumentException(
          "Task methods '${first.id}' and '${decl.id}' both generate the task class " +
            "'${decl.className}'; rename one of them or an enclosing @Builder.TaskGroup class",
        )
      }
    }
  }

  /**
   * Rejects a task method or task group whose wiring-view twin would clash
   * with a member the view already has: `depends`, `lit`, a method of
   * `Object`, or, inside a group, one of `Deps.TaskGroup`'s own. Names scope to their
   * own group, so only one scope is compared.
   */
  private fun checkViewNames(
    scope: Scope,
    inGroup: Boolean = false,
  ) {
    val reserved = if (inGroup) RESERVED_VIEW_NAMES + RESERVED_GROUP_VIEW_NAMES else RESERVED_VIEW_NAMES
    scope.tasks.forEach { decl ->
      val name = decl.method.simpleName.toString()
      require(name !in reserved) {
        "Task method '$name' clashes with a member of the wiring view; rename the method and keep " +
          "the task id with ${decl.kind.spelling}(id = \"${decl.id.substringAfterLast('.')}\")"
      }
    }
    val accessors = mutableMapOf<String, GroupDeclaration>()
    scope.groups.forEach { group ->
      require(group.accessor !in reserved) {
        "Task group class '${group.element.simpleName}' clashes with a member of the wiring view; " +
          "rename the class and keep the group id with @Builder.TaskGroup(id = \"${group.id}\")"
      }
      require(scope.tasks.none { it.method.simpleName.contentEquals(group.accessor) }) {
        "Task group class '${group.element.simpleName}' and task method '${group.accessor}' would both " +
          "be '${group.accessor}()' on the wiring view; rename one"
      }
      accessors[group.accessor]?.let { first ->
        throw IllegalArgumentException(
          "Task group classes '${first.element.simpleName}' and '${group.element.simpleName}' would both " +
            "be '${group.accessor}()' on the wiring view; rename one",
        )
      }
      accessors[group.accessor] = group
      checkViewNames(group.scope, inGroup = true)
    }
  }

  /**
   * Matches the view by the name the class wrote: the view is generated in
   * this same round, so javac may not have resolved it yet.
   */
  private fun TypeMirror.isView(view: ClassName): Boolean = toString().let { it == view.canonicalName() || it == view.simpleName() }

  private fun isUnchecked(type: TypeMirror): Boolean =
    with(processingEnv) {
      listOf(RuntimeException::class.java, Error::class.java).any {
        typeUtils.isAssignable(type, elementUtils.getTypeElement(it.canonicalName).asType())
      }
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
          CodeBlock.join(items.map { CodeBlock.of($$"$S", it.value) }, ", "),
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

  /**
   * Checks what a decider hands back, which is what the SDK reads its decision
   * from. A plain task may return anything, including nothing.
   */
  private fun checkDeciderReturn(
    kind: TaskKind,
    method: ExecutableElement,
  ) {
    val returns = method.returnType
    when (kind) {
      TaskKind.TASK, TaskKind.TRIGGER -> return
      TaskKind.CONDITION ->
        require(returns.kind == TypeKind.BOOLEAN || with(processingEnv) { isType(returns, BOXED_BOOLEAN_TYPE) }) {
          "@Builder.If method '${method.simpleName}' returns $returns, but a condition returns boolean: " +
            "true runs the task named by then, false the one named by orElse"
        }
      TaskKind.BRANCH ->
        require(with(processingEnv) { isType(returns, TASK_ID_TYPE) }) {
          "@Builder.Branch method '${method.simpleName}' returns $returns, but a branch returns a " +
            "TaskId: name the case it chose with a constant of the generated TaskIds"
        }
    }
  }

  /** The `id` the declaring annotation sets, empty when it leaves it out. */
  private fun declaredId(
    method: ExecutableElement,
    kind: TaskKind,
  ): String {
    val mirror = method.annotationMirrors.first { it.names(kind.annotation) }
    val id = mirror.elementValues.entries.firstOrNull { it.key.simpleName.contentEquals("id") }
    return id?.value?.value as String? ?: ""
  }

  private fun buildTask(decl: TaskDeclaration): TypeSpec {
    val executeSpec =
      MethodSpec
        .methodBuilder(decl.kind.bodyMethod)
        .addAnnotation(Override::class.java)
        .addModifiers(Modifier.PUBLIC)
        .returns(
          when (decl.kind) {
            TaskKind.TASK, TaskKind.TRIGGER -> TypeName.VOID
            TaskKind.CONDITION -> TypeName.BOOLEAN
            TaskKind.BRANCH -> TASK_ID_TYPE
          },
        ).addParameter(CONTEXT_TYPE, "context")
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

    when {
      // The SDK pushes a decider's choice itself, after it has skipped what the choice rules out.
      decl.kind != TaskKind.TASK -> $$"return new $T().$L($L)"
      inner.returnType.kind == TypeKind.VOID -> $$"new $T().$L($L)"
      else -> $$"client.setXCom(new $T().$L($L))"
    }.also {
      executeSpec.addStatement(
        it,
        ClassName.get(decl.owner),
        inner.simpleName,
        innerArgs,
      )
    }

    return TypeSpec
      .classBuilder(decl.className)
      .addSuperinterface(decl.kind.taskInterface)
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

/** The tasks and task groups one class declares. */
private class Scope(
  val tasks: List<TaskDeclaration>,
  val groups: List<GroupDeclaration>,
) {
  /** Every task of this scope and the groups beneath it, outermost first. */
  fun allTasks(): List<TaskDeclaration> = tasks + groups.flatMap { it.scope.allTasks() }

  /** Every group beneath this scope, parents before the groups nested in them. */
  fun allGroups(): List<GroupDeclaration> = groups.flatMap { listOf(it) + it.scope.allGroups() }
}

/** One `@Builder.TaskGroup` class, and what it declares. */
private class GroupDeclaration(
  val element: TypeElement,
  val id: String,
  val fullId: String,
  val scope: Scope,
) {
  /** The view method that reaches this group, named after the class it is declared as. */
  val accessor: String = element.simpleName.toString().replaceFirstChar(Char::lowercase)
}

/**
 * One [Builder.Task]-annotated method with its resolved id and data parameters.
 * [owner] is the class that declares it, which the generated body instantiates,
 * and [classPath] the task-group classes enclosing it.
 */
private class TaskDeclaration(
  val method: ExecutableElement,
  val id: String,
  val dataParams: List<DataParam>,
  val owner: TypeElement,
  val classPath: List<String> = emptyList(),
  val kind: TaskKind = TaskKind.TASK,
) {
  val className: String =
    (classPath + method.simpleName.toString().replaceFirstChar(Char::uppercase)).joinToString("_")
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
private val DAG_SOURCE_TYPE = ClassName.get(DagSource::class.java)
private val REFS_TYPE = ClassName.get(Refs::class.java)
private val ARG_TYPE = ClassName.get(Arg::class.java)
private val TASK_HANDLE_TYPE = ClassName.get(TaskRef::class.java)
private val TASK_TYPE = ClassName.get(Task::class.java)
private val CONDITION_TASK_TYPE = ClassName.get(ConditionTask::class.java)
private val CONDITION_REF_TYPE = ClassName.get(ConditionRef::class.java)
private val BRANCH_TASK_TYPE = ClassName.get(TaskIdBranchTask::class.java)
private val BRANCH_REF_TYPE = ClassName.get(BranchRef::class.java)
private val TASK_ID_TYPE = ClassName.get(TaskId::class.java)
private val TRIGGER_TYPE = ClassName.get(TriggerDagRun::class.java)
private val VOID_TYPE = ClassName.get("java.lang", "Void")

/** Name of the generated holder of a Dag's task ids, which no task class may take. */
private const val TASK_IDS = "TaskIds"
private val BOXED_BOOLEAN_TYPE = ClassName.get("java.lang", "Boolean")
private val DEPS_TYPE = ClassName.get(Deps::class.java)
private val GROUP_TYPE = DEPS_TYPE.nestedClass("TaskGroup")
private val LIST_TYPE = ClassName.get(List::class.java)

private const val DAG_ANNOTATION = "org.apache.airflow.sdk.Builder.Dag"

/**
 * What a declared task is, and the annotation that declares it. A decider
 * differs from a plain task in what it implements and in what its wiring-view
 * method hands back, but carries the same configuration attributes.
 */
private enum class TaskKind(
  val annotation: String,
  val spelling: String,
  /** Method the generated class implements, and the type its wiring view hands back. */
  val bodyMethod: String,
  val taskInterface: ClassName,
  val refType: ClassName?,
) {
  TASK("org.apache.airflow.sdk.Builder.Task", "@Builder.Task", "execute", TASK_TYPE, null),
  TRIGGER("org.apache.airflow.sdk.Builder.Task", "@Builder.Task", "execute", TASK_TYPE, null),
  CONDITION("org.apache.airflow.sdk.Builder.If", "@Builder.If", "decide", CONDITION_TASK_TYPE, CONDITION_REF_TYPE),
  BRANCH("org.apache.airflow.sdk.Builder.Branch", "@Builder.Branch", "choose", BRANCH_TASK_TYPE, BRANCH_REF_TYPE),
  ;

  companion object {
    /**
     * The kinds an annotation declares. TRIGGER is left out: it shares
     * [TASK]'s annotation and is told apart by the method's return type.
     */
    val declaring = listOf(TASK, CONDITION, BRANCH)
  }
}

/** Whether this annotation is the one [name] qualifies. */
private fun AnnotationMirror.names(name: String): Boolean = (annotationType.asElement() as TypeElement).qualifiedName.contentEquals(name)

private val RESERVED_VIEW_NAMES =
  setOf(
    "depends",
    "lit",
    "clone",
    "equals",
    "finalize",
    "getClass",
    "hashCode",
    "notify",
    "notifyAll",
    "toString",
    "wait",
  )

/**
 * What a group's view inherits from `Deps.TaskGroup`, on top of
 * [RESERVED_VIEW_NAMES]. Read from the interface so it cannot drift when a
 * member is added there.
 */
private val RESERVED_GROUP_VIEW_NAMES: Set<String> =
  Deps.TaskGroup::class.java
    .methods
    .filterNot { ReflectModifier.isStatic(it.modifiers) }
    .map { it.name }
    .toSet()

private val DAG_STRUCTURAL_ATTRIBUTES = setOf("id", "to")
private val TASK_STRUCTURAL_ATTRIBUTES = setOf("id")

/**
 * The `TaskIds` constant for a task id: its words in upper case, joined by
 * underscores, so `handleLong` and `checks.audit` become `HANDLE_LONG` and
 * `CHECKS_AUDIT`.
 */
private fun constantName(id: String): String =
  id
    .replace(Regex("([a-z0-9])([A-Z])"), "$1_$2")
    .replace(Regex("[^A-Za-z0-9]+"), "_")
    .uppercase()

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
