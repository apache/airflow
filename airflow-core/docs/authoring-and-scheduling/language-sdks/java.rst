 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

.. _java-sdk:

Java SDK
========

|experimental|

The Java SDK lets you write Airflow Dags and tasks in Java, Kotlin, Scala or any other JVM language. There are
two ways to use it:

* **Dag definition.** The schedule, the tasks, their options and the dependencies between them are all written
  in Java, with no Python file involved. See :ref:`java-sdk/dag-definition`.
* **Python Dag with stub TaskHandler.** A Python Dag declares the tasks with ``@task.stub`` and wires them
  together, and a Java ``TaskHandler`` implements each one. See :ref:`java-sdk/stub-tasks`.

Both use the same task API and the same build plugin, and one bundle can serve both.

The SDK is published to Maven Central as ``org.apache.airflow:airflow-sdk``.

.. seealso::

  For the full API reference, see the `Java SDK API reference
  <https://airflow.apache.org/docs/java-sdk/stable/>`__, and for the released versions,
  `Maven Central <https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk>`__.

.. contents:: Contents
   :local:
   :depth: 2

Prerequisites
-------------

* JDK 11 or later to build the project. A local Gradle installation is only needed to generate the
  `Gradle Wrapper <https://docs.gradle.org/current/userguide/gradle_wrapper.html>`__ of a new project.
* JRE 11 or later on the Airflow workers and the Dag processor. If ``java`` is not on the ``PATH``, set the
  coordinator's ``java_executable`` (see :ref:`java-sdk/coordinator-config`).
* The ``apache-airflow-task-sdk`` package, installed with Airflow, provides
  :class:`~airflow.sdk.coordinators.java.JavaCoordinator`, which runs the Java code. No other Python package is
  needed.

.. _java-sdk/quick-start:

Quick start
-----------

This example declares a whole Dag in Java, builds it into a bundle, and deploys it, with no Python file
involved.

Create a Gradle project:

.. code-block:: text

    hello-java/
    ├── build.gradle
    ├── gradle.properties
    ├── settings.gradle
    └── src/main/java/com/mycompany/airflow/hello/
        ├── HelloDag.java
        └── Main.java

Set the SDK version in ``gradle.properties``, using the release you want from
`Maven Central <https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk>`__, and name the project
in ``settings.gradle``:

.. code-block:: properties

    airflowJavaSdkVersion=<version>

.. code-block:: groovy

    rootProject.name = "hello-java"

Configure the build in ``build.gradle``:

.. code-block:: groovy

    plugins {
        id("org.apache.airflow.sdk") version "${airflowJavaSdkVersion}"
    }

    repositories {
        mavenCentral()
    }

    dependencies {
        annotationProcessor("org.apache.airflow:airflow-sdk-processor:${airflowJavaSdkVersion}")
        implementation("org.apache.airflow:airflow-sdk:${airflowJavaSdkVersion}")
    }

    java {
        toolchain {
            languageVersion.set(JavaLanguageVersion.of(11))
        }
    }

    airflowBundle {
        mainClass = "com.mycompany.airflow.hello.Main"
    }

Declare the Dag in ``HelloDag.java``:

.. BEGIN SHARED SNIPPET hello-dag

.. code-block:: java

    package com.mycompany.airflow.hello;

    import org.apache.airflow.sdk.*;

    @Airflow.Dag(id = "java_hello", schedule = "@daily", queue = "java")
    public class HelloDag {

      @Airflow.Task
      public long extract() {
        return 42L;
      }

      @Airflow.Task
      public void report(long rows) {
        System.out.println("extracted " + rows + " rows");
      }

      @Airflow.Deps
      static class Wiring implements HelloDagDeps {
        void depends() {
          report(extract());
        }
      }
    }

.. END SHARED SNIPPET hello-dag

Serve it from ``Main.java``:

.. BEGIN SHARED SNIPPET hello-main

.. code-block:: java

    package com.mycompany.airflow.hello;

    import org.apache.airflow.sdk.Bundle;
    import org.apache.airflow.sdk.Server;

    public class Main {
      public static void main(String[] args) {
        Server.create(args).serve(new Bundle().register(HelloDag.class));
      }
    }

.. END SHARED SNIPPET hello-main

Build the bundle from the project root:

.. code-block:: bash

    ./gradlew bundle

Copy ``build/bundle/`` into a Dag bundle, for example a ``java-hello/`` directory in the default ``dags-folder``
bundle, which reads the ``[core] dags_folder`` directory (see
:doc:`Dag bundles </administration-and-deployment/dag-bundles>`). Then send the ``java`` queue to the Java
coordinator in ``airflow.cfg``:

.. code-block:: ini

    [sdk]
    coordinators = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"}}
    queue_to_coordinator = {"java": "java"}

Airflow reads the bundle like any other Dag file: ``java_hello`` shows up in the UI, runs on its schedule,
and ``report`` receives the value ``extract`` returned.

.. _java-sdk/dag-definition:

Declaring a Dag in Java
-----------------------

A Dag can be declared in two styles, and each section below shows both:

* **Annotations.** ``@Airflow.Dag`` marks a class as a Dag, ``@Airflow.Task`` marks its task methods, and a
  nested ``@Airflow.Deps`` class wires them together. The annotation processor generates the code that
  connects them, so ``javac`` type-checks the graph.
* **Interface.** Each task is a class implementing ``Task``, and a ``DagDef`` object registers the tasks and
  their dependencies. It needs no annotation processor.

Tasks and their inputs
~~~~~~~~~~~~~~~~~~~~~~

With annotations, ``@Airflow.Task`` marks a method as a task, and the nested ``@Airflow.Deps`` class declares
the graph by calling the task methods:

.. code-block:: java

    @Airflow.Dag(id = "java_etl", schedule = "@daily", queue = "java")
    public class EtlPipeline {

      @Airflow.Task(retries = 2)
      public long extract() {
        return 42L;
      }

      @Airflow.Task
      public long transform(long extracted, double factor) {
        return (long) (extracted * factor);
      }

      @Airflow.Task
      public void load(long transformed) {}

      @Airflow.Deps
      static class Wiring implements EtlPipelineDeps {
        void depends() {
          load(transform(extract(), lit(0.9)));
        }
      }
    }

The annotation processor generates the ``EtlPipelineDeps`` interface, with one method per task. Passing the
value one call returned into another call makes the second task wait for the first and receive its value.
``lit(...)`` passes a constant instead, which must be a string, number, boolean, list or map.

* Every task method is called in the wiring class. A task the wiring leaves out fails when the Dag is read.
* A task method that takes arguments is called once; keep its result in a local variable to use it twice.
* Values move between tasks as XComs, so a task returns data JSON can hold (see :ref:`java-sdk/types`).

With the interface, each task is a class, and ``dag.task(taskId, TaskClass)`` registers it. A task reads the
value of an upstream task itself, through ``client.getXCom``:

.. code-block:: java

    public class Extract implements Task {
      @Override
      public void execute(Context context, Client client) {
        client.setXCom(42L);
      }
    }

    public class Load implements Task {
      @Override
      public void execute(Context context, Client client) {
        var extracted = ((Number) client.getXCom("extract")).longValue();
      }
    }

    var dag = new DagDef("java_etl").config("schedule", "@daily").config("queue", "java");
    var extract = dag.task("extract", Extract.class).config("retries", 2);
    var load = dag.task("load", Load.class);
    extract.before(load);

A task class must be ``public`` and concrete, have a public no-argument constructor, and be ``static`` if it is
nested in another class. The runtime creates a new instance for every task run.

Register either kind of Dag on the ``Bundle`` that ``main`` serves:

.. code-block:: java

    Server.create(args).serve(new Bundle().register(EtlPipeline.class).register(dag));

Task ids
~~~~~~~~

``@Airflow.Task`` takes the task id from the method name unless ``id`` is set, and ``@Airflow.Dag`` takes the
Dag id from the class name unless ``id`` is set. ``dag.If`` and ``dag.Switch`` take it from the class name unless
you pass one.

Dag and task options
~~~~~~~~~~~~~~~~~~~~

The attributes of ``@Airflow.Dag`` and ``@Airflow.Task``, and the keys of ``DagDef.config`` and
``TaskDef.config``, are Airflow's own Dag and task settings. Annotation attributes are ``camelCase``
(``retryDelay``), and ``config`` keys are spelled as in Python (``"retry_delay"``). Only the settings you write
are applied, so Airflow's defaults hold for the rest.

.. code-block:: java

    @Airflow.Dag(id = "java_etl", schedule = "@daily", startDate = "2026-01-01T00:00:00Z", catchup = false,
        queue = "java")

    @Airflow.Task(retries = 2, retryDelay = "PT5M")

.. code-block:: java

    var dag = new DagDef("java_etl").config("schedule", "@daily").config("catchup", false).config("queue", "java");
    dag.task("load", Load.class).config("retry_delay", Duration.ofMinutes(5));

Durations and date-times are ISO-8601 strings in annotations, and ``java.time.Duration`` and
``java.time.OffsetDateTime`` values in ``config``. An unknown setting or a value of the wrong type fails the
build for an annotation, and the ``config`` call for an object.

A cron ``schedule`` runs in the time zone of ``startDate``, or in UTC without one.

Every task of the Dag runs on the Java coordinator, so it needs a queue that
:ref:`queue_to_coordinator <java-sdk/coordinator-config>` sends there. Set ``queue`` once on the Dag and each
task inherits it, including a ``TriggerDagRun`` task. A task's own ``queue`` wins over the Dag's, and the Dag's
cannot be blank.

Order-only dependencies
~~~~~~~~~~~~~~~~~~~~~~~

When a task only has to run after another, without receiving its value, draw the dependency with ``before``
and ``after``, the Java spelling of Python's ``>>`` and ``<<``. Both work the same in the two styles:

.. code-block:: java

    rows.before(audit());                // rows >> audit
    load.after(transform, validate);     // [transform, validate] >> load

Both take any number of tasks and return the task they were called on, so calls chain. ``Flow.of(a, b).before(c,
d)`` draws every edge between two sets in one call. ``Flow`` is ``org.apache.airflow.sdk.Deps.Flow``, and needs
no import inside the wiring class. An edge to a task of another Dag, or one that closes a
cycle, fails when the Dag is registered.

Task groups
~~~~~~~~~~~

A task group gathers tasks that the Airflow UI shows as one node, as Python's ``TaskGroup`` does. Every id
declared in a group is prefixed with the group's id.

With annotations, a ``static`` nested ``@Airflow.TaskGroup`` class holds the tasks of one group, and nesting
one group class in another nests the groups:

.. code-block:: java

    @Airflow.TaskGroup                          // group "Staging", from the class name
    static class Staging {
      @Airflow.Task
      public long stage(long rows) { ... }      // task "Staging.stage"

      @Airflow.TaskGroup(id = "checks")
      static class Checks {
        @Airflow.Task
        public void nulls(long staged) { ... }  // task "Staging.checks.nulls"
      }
    }

    @Airflow.Deps
    static class Wiring implements EtlPipelineDeps {
      void depends() {
        var staged = staging().stage(extract());
        staging().checks().nulls(staged);
      }
    }

With the interface, ``taskGroup`` declares a group on the Dag or on another group:

.. code-block:: java

    var staging = dag.taskGroup("staging");
    var stage = staging.task("stage", Stage.class);                        // "staging.stage"
    staging.taskGroup("checks").task("nulls", Nulls.class).after(stage);   // "staging.checks.nulls"
    extract.before(staging);

A group takes ``before`` and ``after`` too, which orders the whole group. A group id is made of ASCII letters,
digits, underscores and dashes, and no task or other group in the Dag can share it.

Conditional branching
~~~~~~~~~~~~~~~~~~~~~

A condition is a task that returns a boolean, and names the task each outcome runs. The side not taken is
skipped.

With annotations, mark the method with ``@Airflow.If``, and name the sides in the wiring class:

.. code-block:: java

    @Airflow.If
    public boolean hasRows(long transformed) {
      return transformed > 0;
    }

    @Airflow.Deps
    static class Wiring implements EtlPipelineDeps {
      void depends() {
        var transformed = transform(extract(), lit(0.9));
        hasRows(transformed).Then(load(transformed)).Else(reportEmpty());
      }
    }

With the interface, implement ``ConditionTask`` and register it with ``dag.If``:

.. code-block:: java

    public class HasRows implements ConditionTask {
      @Override
      public boolean decide(Context context, Client client) {
        return ((Number) client.getXCom("transform")).longValue() > 0;
      }
    }

    dag.If(HasRows.class).after(transform).Then(load).Else(reportEmpty);

``Else`` is optional.

Only the task on the side not taken is skipped, and it stays skipped if it is cleared later. A task that runs
after both sides needs a trigger rule that accepts a skipped upstream, such as ``none_failed_min_one_success``.

Multi-way branching
~~~~~~~~~~~~~~~~~~~

A switch is a task that chooses one of several tasks to run, and skips every other one. It returns the class
of the task to run.

With annotations, mark the method with ``@Airflow.Switch``, return a class the annotation processor generated
for a task of the Dag, and list the cases in the wiring class:

.. code-block:: java

    @Airflow.Switch
    public Class<? extends Task> pickReport(long transformed) {
      return transformed > 100 ? EtlPipelineBuilder.ReportLong.class : EtlPipelineBuilder.ReportShort.class;
    }

    @Airflow.Deps
    static class Wiring implements EtlPipelineDeps {
      void depends() {
        pickReport(transform(extract(), lit(0.9))).Case(reportLong()).Case(reportShort());
      }
    }

With the interface, implement ``SwitchTask`` and register it with ``dag.Switch``:

.. code-block:: java

    public class PickReport implements SwitchTask {
      @Override
      public Class<? extends Task> choose(Context context, Client client) {
        return ((Number) client.getXCom("transform")).longValue() > 100 ? ReportLong.class : ReportShort.class;
      }
    }

    dag.Switch(PickReport.class).after(transform).Case(reportLong).Case(reportShort);

Exactly one case runs. A switch that returns a class that is not one of its cases fails, and two cases cannot
run the same class. A task that runs after several cases needs a trigger rule that accepts a skipped upstream,
such as ``none_failed_min_one_success``.

Edge labels
~~~~~~~~~~~

``Label`` puts a label on an edge, which the Airflow UI shows on the graph, as Python's ``Label`` does.
``Label.of(text).on(...)`` goes in ``before`` or ``after`` in place of the task or group at the other end of the edge,
and works the same in the two styles:

.. code-block:: java

    loaded.before(Label.of("when empty").on(reportEmpty));               // load >> Label("when empty") >> report_empty
    cleaned.after(Label.of("always").on(loaded));                        // load >> Label("always") >> cleanup
    loaded.before(Label.of("forks").on(reportEmpty, reportSuspicious));  // one label on two edges

A label goes only on the edges between the receiver of ``before`` or ``after`` and the tasks passed to ``on``, so each
edge of a fan-out can carry its own label. Declaring an edge that already exists only adds the label, which is how the
sides of a condition get theirs. A new label replaces the old one:

.. code-block:: java

    var hasRows = dag.If(HasRows.class).after(transform).Then(load).Else(reportEmpty);
    hasRows.before(Label.of("rows found").on(load), Label.of("no rows").on(reportEmpty));

``Label`` is ``org.apache.airflow.sdk.Label``. Unlike ``Flow``, the wiring class does not inherit it, so it needs an
import. Pass the value ``on`` returns only to ``before`` and ``after``: calling ``before`` or ``after`` on it fails. On
an edge to or from a task group, the UI shows the label on the group's node.


Triggering another Dag
~~~~~~~~~~~~~~~~~~~~~~

``TriggerDagRun`` makes a task that starts a run of another Dag, as Python's ``TriggerDagRunOperator`` does.

With annotations, a ``@Airflow.Task`` method returns one. The method runs when the Dag is built, not when the
task runs, so it takes no arguments:

.. code-block:: java

    @Airflow.Task
    public TriggerDagRun triggerDownstream() {
      return new TriggerDagRun("downstream_etl").config("conf", Map.of("source", "java_etl"));
    }

With the interface, pass one to ``dag.task`` in place of a task class:

.. code-block:: java

    dag.task("trigger_downstream", new TriggerDagRun("downstream_etl").config("wait_for_completion", true));

``config`` takes the parameters of ``TriggerDagRunOperator``, spelled as in Python, such as ``conf``,
``wait_for_completion``, ``deferrable``, ``poke_interval``, ``allowed_states`` and ``failed_states``. Values are
sent as written, so Jinja templates are not rendered.

The task runs no Java code of yours, takes no inputs and cannot be a case of a switch. With ``deferrable``, it
waits in the triggerer, which needs the standard provider installed.

A complete example
~~~~~~~~~~~~~~~~~~

``java-sdk/example/src/java/org/apache/airflow/example/nativedag/`` declares the same Dag in both styles, with a
task group, a condition with labeled edges, a switch and a triggered Dag run.

.. _java-sdk/stub-tasks:

Implementing stub tasks of a Python Dag
---------------------------------------

Here a Python Dag declares the tasks and their dependencies, and Java implements some of them. Use it when the
Dag needs something a Java Dag cannot express yet (see :ref:`java-sdk/limitations`), or to mix Python and Java
tasks in one Dag.

The Python Dag
~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def sales_pipeline():
        @task.stub(queue="java")
        def extract(): ...

        @task.stub(queue="java")
        def transform(extracted): ...

        @task
        def load(transformed):
            print(f"Loaded: {transformed}")

        load(transform(extract()))


    sales_pipeline()

``@task.stub`` declares a task that is implemented elsewhere, and ``queue`` sends it to the Java coordinator.

The Java side
~~~~~~~~~~~~~

With annotations, ``@Airflow.TaskHandler`` marks a method as the body of a stub task, naming the ``dag_id`` and
``task_id`` it implements. ``task`` defaults to the method name:

.. code-block:: java

    public class SalesPipeline {

      @Airflow.TaskHandler(dag = "sales_pipeline", task = "extract")
      public long extract() {
        return 3;
      }

      @Airflow.TaskHandler(dag = "sales_pipeline", task = "transform")
      public long transform(long extracted) {
        return extracted * 2;
      }
    }

    Server.create(args).serve(new Bundle().register(SalesPipeline.class));

With the interface, implement ``Task`` and register the class against the Dag and task ids:

.. code-block:: java

    public class Extract implements Task {
      @Override
      public void execute(Context context, Client client) {
        client.setXCom(3L);
      }
    }

    Server.create(args).serve(new Bundle().register("sales_pipeline", "extract", Extract.class));

The task id must match the stub's, including any task group prefix. ``register`` can be called any number of
times, so one bundle can implement the tasks of several Python Dags and declare Dags of its own.

Passing arguments
~~~~~~~~~~~~~~~~~

The arguments the Python Dag passes to a stub task reach the Java handler, either by position or by name.

**By position.** A ``@Airflow.TaskHandler`` method receives the arguments in order. ``Client`` and ``Context``
parameters, which the SDK injects, do not count:

.. code-block:: python

    @task.stub(queue="java")
    def score(rows, threshold): ...


    score(load_rows(), 0.75)

.. code-block:: java

    @Airflow.TaskHandler(dag = "etl", task = "score")
    public long score(Client client, long rows, double threshold) {
      // rows      <- the value load_rows returned
      // threshold <- 0.75
    }

**By name.** A class implementing ``TaskInput`` receives each argument in the public field of the same name,
ignoring case and underscores, so ``region_code`` reaches ``regionCode``. The class needs a public
no-argument constructor, and only its public non-final fields bind. ``@ArgName`` maps a field to an argument
whose name cannot match on its own, and is matched as written, with no folding:

.. code-block:: python

    score(region_code="emea", threshold=load_threshold())

.. code-block:: java

    public static class ScoreInput implements TaskInput {
      @ArgName("region_code")
      public String region;

      public double threshold;
    }

    @Airflow.TaskHandler(dag = "etl", task = "score")
    public long score(Client client, ScoreInput input) { ... }

With the interface, implement ``InputTask<I>`` instead of ``Task`` to receive a ``TaskInput``:

.. code-block:: java

    public class ScoreTask implements InputTask<ScoreInput> {
      @Override
      public void execute(Context context, Client client, ScoreInput input) { ... }
    }

The two forms handle a mismatch differently:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Case
     - By position (method parameters)
     - By name (``TaskInput`` fields)
   * - More arguments than the handler declares
     - The task fails.
     - A warning is logged, and the extra arguments are ignored.
   * - Fewer arguments than the handler declares
     - The task fails.
     - A warning is logged, and the field keeps its Java default.
   * - An argument resolves to ``null``
     - A boxed parameter (``Long``, ``Double``) receives ``null``; a primitive one fails the task with
       ``MissingXComException``.
     - Same as by position.
   * - Available in
     - ``@Airflow.TaskHandler`` methods.
     - ``@Airflow.TaskHandler`` methods and ``InputTask``.

An argument the Python call leaves at its default does not count towards the arguments by position, so the
method may leave it out. A method declares flat parameters or one ``TaskInput``, not both, and two fields whose
names match the same argument fail the build.

.. note::

  A dependency drawn with ``>>`` in the Python Dag orders the tasks and passes nothing. To read a value the call
  did not pass, use ``client.getXCom``.

Writing tasks
-------------

A task is an ordinary Java method or class. It works the same in both ways of using the SDK.

A ``@Airflow.Task`` or ``@Airflow.TaskHandler`` method can declare a ``Client`` and a ``Context`` parameter in any
position, and the SDK passes them in. A ``Task`` class receives both in ``execute``. ``Context`` describes the
running task, such as ``context.ti.taskId``, ``context.ti.tryNumber`` and ``context.dagRun.logicalDate``.

A value a method returns, or a ``Task`` class passes to ``client.setXCom``, becomes the task's ``return_value``
XCom, and is what a downstream task receives. An uncaught exception fails the task, and retries it if the task
has retries.

The ``Client``
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Method
     - Description
   * - ``getVariable(key)``
     - The Variable's value, or ``null`` when it does not exist.
   * - ``setVariable(key, value, description?)``
     - Stores a Variable, replacing any existing value. Values are strings, so serialize structured data, for
       example to JSON.
   * - ``deleteVariable(key)``
     - Removes a Variable.
   * - ``getConnection(connId)``
     - A ``Connection`` with ``id``, ``type``, ``host``, ``schema``, ``login``, ``password``, ``port`` and
       ``extra``. Throws when the connection does not exist.
   * - ``getXCom(taskId)``
     - The ``return_value`` XCom of a task in the same Dag run, or ``null`` when it does not exist. Overloads
       read another key, Dag or run; see the API reference. For the Java types values arrive as, see
       :ref:`java-sdk/types`.
   * - ``setXCom(value)`` / ``setXCom(key, value)``
     - Publishes an XCom value. The value must be JSON-serializable.
   * - ``getTaskStateStore()``
     - This task instance's key/value store, with ``get``, ``set``, ``delete`` and ``clear``. See
       :ref:`java-sdk/task-state-store`.

.. note::

   A value from a secrets backend, such as an ``AIRFLOW_VAR_*`` environment variable, still wins over the stored
   value when the Variable is read back. ``setVariable`` without a description clears the existing description.

.. _java-sdk/cancellation:

Stopping when Airflow asks
~~~~~~~~~~~~~~~~~~~~~~~~~~

Airflow asks a running task to stop when, for example, a user marks its task instance as failed. The
supervisor, the Airflow process that started the task's JVM, then sends the JVM ``SIGTERM``. If the JVM
is still running after a grace period, the supervisor sends ``SIGKILL``. When a user marks the task
instance failed or success, or clears it, the grace period is
:ref:`core.killed_task_cleanup_time <config:core__killed_task_cleanup_time>` seconds.

A task can check ``Context.isCancelled()``, or call ``Context.awaitCancellation(timeout)`` where it
would otherwise wait with ``Thread.sleep``. Unlike ``Thread.sleep``, ``awaitCancellation`` returns as
soon as Airflow asks the task to stop. Once a task has called either method, the SDK catches the first
``SIGTERM`` or ``SIGINT`` and the JVM keeps running. The task can then stop its work, such as a process
it started, and throw. Keep checking until the task returns, because the first signal no longer stops
the JVM:

.. code-block:: java

    public class ReportTask implements Task {
      @Override
      public void execute(Context context, Client client) throws Exception {
        Process report = new ProcessBuilder("./build-report.sh").inheritIO().start();
        while (!context.isCancelled()) {
          if (report.waitFor(1, TimeUnit.SECONDS)) {
            if (report.exitValue() != 0) {
              throw new IllegalStateException("build-report.sh failed");
            }
            return;
          }
        }
        report.destroy();
        throw new IllegalStateException("Airflow asked the task to stop");
      }
    }

Throw rather than return when the task stops early, because returning reports success for work the
task did not finish. If a user changed the task instance's state while the task ran, for example by
marking the task instance failed, Airflow keeps that state whatever the task reports.

A task that has never checked cannot stop on its own, so for that task the JVM handles the signal as it
does without the SDK: shutdown hooks run and the JVM exits. The JVM handles a second signal the same
way, so sending the signal a second time stops a task that does not stop when asked. For a task that
has checked, shutdown hooks run when the JVM exits after the task returns. The shutdown hooks do not run
at all if the supervisor has to kill the JVM with ``SIGKILL``.

.. _java-sdk/task-state-store:

Task state store
----------------

The task state store is a key/value store scoped to the task instance. A value written by one attempt is still
readable by the next, within the same Dag run, which makes it a good place for an external job id or a
checkpoint. See :doc:`/core-concepts/task-state-store` for the full concept.

.. code-block:: java

    @Airflow.Task
    public void submit(Client client) throws Exception {
      var store = client.getTaskStateStore();
      var jobId = (String) store.get("job_id");
      if (jobId == null) {
        jobId = submitJob();
        store.set("job_id", jobId, Duration.ofHours(6));
      }
      waitForJob(jobId);
      store.delete("job_id");
    }

* ``get`` returns ``null`` when the key is not set, and ``set`` stores any JSON-serializable value.
* ``set`` takes an optional retention: a positive ``Duration``, or ``TaskStateStore.NEVER_EXPIRE``. Without
  one, ``[state_store] default_retention_days`` applies, where ``0`` means never expire.
* ``delete`` removes one key, and ``clear`` removes every key of the task instance.
* ``[workers] state_store_backend`` is not used from Java tasks, so values are always stored in the metadata
  database. A key written by a Python task through a custom backend reads back as that backend's reference
  marker.

.. _java-sdk/logging:

Logging
-------

Anything a task writes to stdout or stderr appears in the Airflow task log. To send log records with their
levels, add one of the SDK's logging integrations. Each is discovered automatically once it is on the classpath,
and its version is managed by ``airflow-sdk-bom``.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Logging API
     - Artifact
     - Setup
   * - ``System.Logger`` (Java Platform Logging)
     - ``org.apache.airflow:airflow-sdk-jpl``
     - None.
   * - SLF4J 2.x, including Logback users
     - ``org.apache.airflow:airflow-sdk-slf4j``
     - None. Remove any other SLF4J binding, such as ``logback-classic``.
   * - Log4j 2
     - ``org.apache.airflow:airflow-sdk-log4j2``
     - Add ``log4j-core`` at runtime, and an ``<AirflowAppender>`` in ``log4j2.xml``.
   * - ``java.util.logging``
     - ``org.apache.airflow:airflow-sdk-jul``
     - Call ``AirflowJulHandler.setup()`` in ``main`` before serving the bundle.

.. code-block:: groovy

    implementation("org.apache.airflow:airflow-sdk-slf4j:${airflowJavaSdkVersion}")

Use one integration per logging API: a second ``System.LoggerFinder`` or SLF4J binding on the classpath makes
the choice unpredictable. Apache Commons Logging can be bridged to SLF4J with ``jcl-over-slf4j``, or to Log4j 2
with ``log4j-jcl``.

For Log4j 2, declare the appender in ``log4j2.xml``:

.. code-block:: xml

    <Configuration>
      <Appenders>
        <AirflowAppender name="Airflow"/>
      </Appenders>
      <Loggers>
        <Root level="info">
          <AppenderRef ref="Airflow"/>
        </Root>
      </Loggers>
    </Configuration>

.. _java-sdk/types:

XCom type mapping
-----------------

XComs are stored as JSON, so a value read with ``getXCom`` arrives as the matching Java type:

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Python type
     - JSON
     - Java type
   * - ``int``
     - number (integer)
     - ``Long``, or ``BigInteger`` when it does not fit
   * - ``float``
     - number (decimal)
     - ``Double``
   * - ``str``
     - string
     - ``String``
   * - ``bool``
     - boolean
     - ``Boolean``
   * - ``None``
     - null
     - ``null``
   * - ``list``
     - array
     - ``List<Object>``
   * - ``dict``
     - object
     - ``Map<String, Object>``

A typed parameter receives the type it declares, so a ``double`` parameter accepts an integer and
``List<Double>`` holds ``Double`` elements. Use ``String`` rather than ``char``: a ``char`` parameter fails on a string
longer than one character.

.. _java-sdk/build:

Building a bundle
-----------------

.. _java-sdk/build/gradle:

Gradle
~~~~~~

The ``org.apache.airflow.sdk`` Gradle plugin adds a ``bundle`` task, configured by ``airflowBundle``, as in the
:ref:`quick start <java-sdk/quick-start>`:

.. code-block:: bash

    ./gradlew bundle

It writes the bundle to ``build/bundle/``, which is all you deploy. The plugin also packs the source of each
class that declares a Dag, so the Airflow UI's **Code** view shows it.

* By default the bundle is one fat JAR with all dependencies. Set ``fatJar = false`` in ``airflowBundle`` for a
  thin JAR, and deploy every dependency JAR in ``build/bundle/`` with it.
* ``annotationProcessor`` is only needed for the annotation style.

.. _java-sdk/build/maven:

Maven
~~~~~

Set the release you want from
`Maven Central <https://central.sonatype.com/artifact/org.apache.airflow/airflow-sdk>`__ once as a property,
import ``airflow-sdk-bom`` to manage the SDK's artifact versions, and add the SDK and, for the annotation
style, the annotation processor:

.. code-block:: xml

    <properties>
      <airflow.sdk.version><version></airflow.sdk.version>
    </properties>

    <dependencyManagement>
      <dependencies>
        <dependency>
          <groupId>org.apache.airflow</groupId>
          <artifactId>airflow-sdk-bom</artifactId>
          <version>${airflow.sdk.version}</version>
          <type>pom</type>
          <scope>import</scope>
        </dependency>
      </dependencies>
    </dependencyManagement>

    <dependencies>
      <dependency>
        <groupId>org.apache.airflow</groupId>
        <artifactId>airflow-sdk</artifactId>
      </dependency>
    </dependencies>

    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-compiler-plugin</artifactId>
      <configuration>
        <annotationProcessorPaths>
          <path>
            <groupId>org.apache.airflow</groupId>
            <artifactId>airflow-sdk-processor</artifactId>
            <version>${airflow.sdk.version}</version>
          </path>
        </annotationProcessorPaths>
      </configuration>
    </plugin>

Build a fat JAR with ``maven-shade-plugin``, setting the main class and the supervisor schema version the BOM
provides:

.. code-block:: xml

    <plugin>
      <groupId>org.apache.maven.plugins</groupId>
      <artifactId>maven-shade-plugin</artifactId>
      <executions>
        <execution>
          <phase>package</phase>
          <goals><goal>shade</goal></goals>
          <configuration>
            <transformers>
              <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                <mainClass>com.mycompany.airflow.hello.Main</mainClass>
                <manifestEntries>
                  <Airflow-Supervisor-Schema-Version>${airflow.supervisor.schema.version}</Airflow-Supervisor-Schema-Version>
                </manifestEntries>
              </transformer>
            </transformers>
          </configuration>
        </execution>
      </executions>
    </plugin>

``mvn package`` writes the JAR to ``target/``. For thin JARs, set ``Main-Class`` with ``maven-jar-plugin`` and
copy the runtime dependencies next to the JAR with ``maven-dependency-plugin``; the schema version is then read
from the ``airflow-sdk`` JAR. Maven does not check that the main class exists, so a wrong one fails at runtime.

To show a Dag's source in the **Code** view, pack the main class's source under ``META-INF/airflow/sources/``,
add ``src/main/resources/META-INF/airflow/sources.json`` naming it, and point the
``Airflow-Java-SDK-Sources`` manifest entry at that file:

.. code-block:: json

    {"entrypoint_path": "com/mycompany/airflow/hello/Main.java"}

.. code-block:: xml

    <build>
      <resources>
        <resource>
          <directory>src/main/resources</directory>
        </resource>
        <resource>
          <directory>src/main/java/com/mycompany/airflow/hello</directory>
          <includes><include>Main.java</include></includes>
          <targetPath>META-INF/airflow/sources/com/mycompany/airflow/hello</targetPath>
        </resource>
      </resources>
    </build>

    <Airflow-Java-SDK-Sources>META-INF/airflow/sources.json</Airflow-Java-SDK-Sources>

Deploying
---------

Where the bundle goes depends on what it contains.

.. _java-sdk/native-dag-parsing:

Dag definition
~~~~~~~~~~~~~~

Put the bundle in a Dag bundle, such as the default ``dags-folder`` bundle. The Dag processor runs each
executable JAR it finds to read its Dags, as it reads any other Dag file, so it needs a JRE and the same
``[sdk]`` configuration as the workers:

.. code-block:: ini

    [sdk]
    coordinators = {"java": {"classpath": "airflow.sdk.coordinators.java.JavaCoordinator"}}
    queue_to_coordinator = {"java": "java"}

With more than one ``JavaCoordinator``, map each Dag bundle that holds Java Dags to one of them in
``[sdk] dag_bundle_to_coordinator``, such as ``{"dags-folder": "java"}``, or its JARs fail to parse.

A task runs the JAR of its Dag from the same Dag bundle, at the version its Dag run was created with, on the
``JavaCoordinator`` its queue routes to.

* Every JAR whose manifest sets ``Main-Class`` is parsed. Many dependency JARs set one, so keep one executable
  JAR per Dag bundle, set the coordinator's ``main_class``, or list the other JARs in ``.airflowignore``.
* Two JARs that set the same ``Main-Class`` fail to parse.
* A JAR built with an ``org.apache.airflow:airflow-sdk`` release too old for this Airflow version fails to
  parse. Rebuild it with a newer release, or list it in ``.airflowignore``; tasks still find it. The SDK
  stamps its supervisor schema version on the JAR as the ``Airflow-Supervisor-Schema-Version`` manifest
  attribute, and the Dag processor rejects anything older than ``2026-10-30``.
* A bundle that fails to parse, or whose Dags have a cycle, shows up as an import error, and the other Dags keep
  working. Do not declare the same ``dag_id`` in a Python file of that Dag bundle too.

Python Dag with stub TaskHandler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the coordinator loads the JARs that implement a stub task from the stub task's own Dag bundle, at
the version its Dag run was created with, so the JARs go next to the Python Dag that declares the stub.

To keep them in a Dag bundle of their own, name it with ``task_handler_bundle_name``. A task then uses the
version that bundle is on when the task starts:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
        {
          "name": "java-task-handlers",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {"path": "/opt/airflow/jars/sales-pipeline"}
        }
      ]

    [sdk]
    coordinators = {
      "java": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {"task_handler_bundle_name": "java-task-handlers"}
      }
    }
    queue_to_coordinator = {"java": "java"}

Every JAR in that Dag bundle goes on one classpath, so all its handlers share one set of dependencies. To isolate
conflicting versions, give the handlers a second Dag bundle, served by a second coordinator on its own queue.

The Dag processor also parses JARs that only implement stub tasks, and finds no Dags in them. List such JARs in
``.airflowignore`` to skip that; the coordinator still finds them. With its own Dag bundle, ignore everything in
it:

.. code-block:: bash

    # [core] dag_ignore_file_syntax = glob (the default)
    echo '*' > /opt/airflow/jars/sales-pipeline/.airflowignore

    # [core] dag_ignore_file_syntax = regexp
    echo '.' > /opt/airflow/jars/sales-pipeline/.airflowignore

Where the configuration is needed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The coordinator runs where tasks run, so the ``[sdk]`` configuration and a JRE are needed on the workers. With
``CeleryExecutor``, set them on the Celery workers. With ``LocalExecutor``, they run in subprocesses on the
scheduler's host, so set them there. The Dag processor needs them too: it parses the JARs, and checks the stub tasks of each
Python Dag against the handlers the JARs register. The API server never needs them.
Register every Dag bundle a coordinator names in ``[dag_processor] dag_bundle_config_list`` on every component.

The ``[sdk]`` configuration is read at startup, so restart these components after changing it. A rebuilt JAR is
picked up on the next task, since every task instance starts a new JVM.

.. _java-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.java.JavaCoordinator` configuration
---------------------------------------------------------------------

The ``kwargs`` of a ``coordinators`` entry are passed to
:class:`~airflow.sdk.coordinators.java.JavaCoordinator`, and all of them are optional:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``task_handler_bundle_name``
     - *(unset)*
     - The Dag bundle to load the JARs that implement stub tasks from. When unset, a stub task loads them from
       its own Dag bundle. It must be registered in ``[dag_processor] dag_bundle_config_list``.
   * - ``java_executable``
     - ``"java"``
     - Path to the ``java`` binary.
   * - ``jvm_args``
     - ``[]``
     - Extra JVM arguments, such as ``["-Xmx1g", "-Dsome.property=value"]``.
   * - ``main_class``
     - *(auto-detect)*
     - The entry-point class. When unset, the coordinator uses the JAR whose manifest sets ``Main-Class``, the
       first by path if several do. When set, only JARs with this ``Main-Class`` are parsed for Dags.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for the JVM to start. Increase it if your bundle starts slowly, for example with a large
       classpath.

``task_handler_bundle_name`` only concerns stub tasks: a Dag declared in Java always runs from the Dag bundle it
was read from.

``queue_to_coordinator`` maps a queue to a coordinator entry, so every task on that queue runs on the JVM.

Set ``java_executable`` to an absolute path in production, so tasks run on a known JDK whatever ``java`` the
``PATH`` resolves to:

.. code-block:: ini

    [sdk]
    coordinators = {
      "java": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {"java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java"}
      }
    }

.. _java-sdk/limitations:

Limitations
-----------

* **A Dag declared in Java cannot express everything yet.** Dag params, assets and dynamic task mapping have
  no Java form. Declare such a Dag in Python, and implement its tasks in Java with
  :func:`@task.stub <airflow.sdk.task.stub>`.
* **Cluster policies do not apply to Dags declared in Java.** ``dag_policy`` and ``task_policy`` do not run when
  the Dag processor reads them.
* **Some CLI commands do not take a Dag declared in Java.** ``airflow dags test``, ``tasks test`` and
  ``tasks render`` refuse it, and ``airflow dags reserialize`` skips it.
* **One JVM per task instance.** Tasks cannot share in-process state; use XComs or an external store.
