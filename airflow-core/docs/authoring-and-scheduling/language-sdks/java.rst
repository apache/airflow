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

The Java SDK lets you implement Airflow task logic in Java, Kotlin, or any other JVM language. The Dag and its
scheduling remain in Python; individual tasks delegate to a JVM subprocess that is spawned by
:class:`~airflow.sdk.coordinators.java.JavaCoordinator` for each task instance.

.. contents:: Contents
   :local:
   :depth: 2

API reference
-------------

The generated API reference for the Java SDK is published with the Airflow documentation at
`Java SDK API Reference <https://airflow.apache.org/docs/java-sdk/stable/>`__.

Prerequisites
-------------

* JRE 11 or later must be available on the Airflow worker nodes.
* The compiled task JAR(s) and JVM dependencies must be accessible from the worker.
* The ``apache-airflow-task-sdk`` package (installed with Airflow) provides the coordinator;
  no additional Python packages are needed.

Quick start
-----------

The following example shows the minimal moving parts: a Python Dag with two stub tasks, and a Java
implementation of those tasks.

Python Dag (the scheduling side)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def sales_pipeline():
        @task.stub(queue="java")
        def extract(): ...

        @task.stub(queue="java")
        def transform(extracted): ...

        @task()
        def load(transformed):
            print(f"Loaded: {transformed}")

        load(transform(extract()))


    sales_pipeline()

Java implementation
~~~~~~~~~~~~~~~~~~~

.. code-block:: java

    import org.apache.airflow.sdk.*;

    @Builder.Dag(id = "sales_pipeline")
    public class SalesPipeline {

      @Builder.Task(id = "extract")
      public long extract(Client client) {
        var conn = client.getConnection("sales_db");
        // ... fetch data using conn.host, conn.login, conn.password ...
        return recordCount;
      }

      @Builder.Task(id = "transform")
      public long transform(Client client, long recordCount) {
        var threshold = (String) client.getVariable("transform_threshold");
        // ... process data ...
        return transformedCount;
      }
    }

.. note::

  The graph is declared once, in the Python Dag file: ``transform(extract())`` feeds the upstream's
  return value into the downstream's parameter by calling tasks like functions. The supervisor sends
  the resulting *argument bindings* to the Java runtime, and each Java data parameter receives
  whatever the Python call site bound at its position — an upstream task's XCom or an inline
  literal. See :ref:`java-sdk/arg-binding`.

Java entry point
~~~~~~~~~~~~~~~~

.. code-block:: java

    public class Main implements BundleBuilder {
      @Override
      public Iterable<DagDef> getDags() {
        return List.of(SalesPipelineBuilder.build());  // SalesPipelineBuilder generated at compile time
      }

      public static void main(String[] args) {
        Server.create(args).serve(new Main().build());
      }
    }

Coordinator configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: ini

    [sdk]
    coordinators = {
      "java-jdk17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {"jars_root": ["/opt/airflow/jars"]}
      }
    }
    queue_to_coordinator = {"java": "java-jdk17"}

See :ref:`java-sdk/coordinator-config` for the full list of accepted ``kwargs``.

Writing tasks
-------------

The Java SDK offers two APIs for implementing tasks. Both produce the same runtime behavior; the choice is a
matter of style.

.. _java-sdk/annotation-api:

Annotation-based API
~~~~~~~~~~~~~~~~~~~~

Annotate a plain Java class and let the SDK generate the boilerplate at compile time.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Annotation
     - Purpose
   * - ``@Builder.Dag(id = "...")``
     - Marks the class as a task container.  For a stub-backed Dag the ``id`` must match the
       ``dag_id`` in the Python Dag.  Further attributes (``schedule``, ``description``, ``tags``,
       ``catchup``, …) mirror the Dag serialization schema and configure the Dag itself; only
       attributes written explicitly are applied.  See :ref:`java-sdk/native-dags`.
   * - ``@Builder.Task(id = "...")``
     - Marks a method as a task implementation.  For a stub-backed Dag the ``id`` must match the
       ``@task.stub`` function name in the Python Dag.  If ``id`` is omitted the method name is
       used.  Further attributes (``retries``, ``queue``, ``retryDelay``, …) mirror the Dag
       serialization schema; only attributes written explicitly are applied.
   * - ``@Wiring``
     - Marks the static method that declares the task graph in Java, TaskFlow-style.  Only needed
       for a Dag that has no Python stub file.  See :ref:`java-sdk/native-dags`.
   * - ``TaskInput`` / ``@ArgName("...")``
     - Marks a class as a task's input bundle, so keyword arguments bind by name instead of by
       position: each public field receives the binding whose name matches it (the ``@ArgName``
       value, or the verbatim field name).  See :ref:`java-sdk/arg-binding`.

Besides the annotations, a task method may declare a ``Client`` and a ``Context`` parameter in any
position; the SDK injects both.  Every other parameter is a *data parameter* and receives an
argument bound by the Python ``@task.stub`` call site.

The annotation processor generates a ``<ClassName>Builder`` class that wires up the task
registry and resolves data parameters and XCom pushes automatically.

.. code-block:: java

    @Builder.Dag(id = "my_dag")
    public class MyDag {

      @Builder.Task(id = "fetch")
      public String fetch(Client client) throws Exception {
        var conn = client.getConnection("my_api");
        // implement task logic
        return result;
      }

      @Builder.Task(id = "process")
      public long process(Client client, String fetched) {
        var threshold = (String) client.getVariable("process_threshold");
        // implement task logic
        return count;
      }
    }

A task method may declare ``throws Exception``; any uncaught exception causes the task instance to be marked
as failed in Airflow (triggering retries if configured on the stub).

.. _java-sdk/interface-api:

Interface-based API
~~~~~~~~~~~~~~~~~~~

Implement the ``Task`` interface directly for full control over how tasks are registered and how XComs are
read.  Each task is registered as a ``TaskDef`` on a ``DagDef``; both carry a fluent
``config(key, value)`` whose keys are Dag serialization schema property names, and ``TaskDef`` also
carries ``dependsOn(...)`` for declaring edges between task definitions.

.. code-block:: java

    import org.apache.airflow.sdk.*;

    public class FetchTask implements Task {
      @Override
      public void execute(Context context, Client client) throws Exception {
        var conn = client.getConnection("my_api");
        // implement task logic
        client.setXCom(result);
      }
    }

Register tasks manually in a ``BundleBuilder``:

.. code-block:: java

    public class MyBundle implements BundleBuilder {
      @Override
      public Iterable<DagDef> getDags() {
        var fetch = new TaskDef("fetch", FetchTask.class).config("retries", 2);
        var process = new TaskDef("process", ProcessTask.class).dependsOn(fetch);
        var dag = new DagDef("my_dag")
            .config("schedule", "@daily")
            .addTask(fetch)
            .addTask(process);
        return List.of(dag);
      }
    }

See the `Java SDK API Reference <https://airflow.apache.org/docs/java-sdk/stable/>`__ for more details.

.. _java-sdk/arg-binding:

Binding stub arguments
~~~~~~~~~~~~~~~~~~~~~~

Calling a ``@task.stub`` TaskFlow-style in the Python Dag is what declares the graph, and the
supervisor delivers the resulting argument bindings to the Java runtime with every task run.  A
binding carries either an upstream task's ``return_value`` XCom or an inline literal written at the
call site.

Positional binding
^^^^^^^^^^^^^^^^^^

A task method's data parameters bind **by position**, in declaration order — the injected ``Client``
and ``Context`` parameters do not take up a position.  Java parameter names are not part of the API,
so renaming one in an IDE never rebinds an input.

.. code-block:: python

    @task.stub(queue="java")
    def score(rows, threshold): ...


    score(load_rows(), 0.75)

.. code-block:: java

    @Builder.Task(id = "score")
    public long score(Client client, long rows, double threshold) {
      // rows      <- the load_rows XCom (position 0)
      // threshold <- the literal 0.75   (position 1)
    }

A primitive parameter cannot hold ``null``, so the task fails with ``MissingXComException`` when its
binding resolves to nothing; declare a boxed type (``Long``, ``Double``, …) to receive ``null``
instead.  Declaring more data parameters than the call site bound also fails the task, rather than
running it with missing inputs.

Named binding with a ``TaskInput`` bundle
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To bind keyword arguments by name, declare a single parameter whose class implements ``TaskInput``.
Each public non-final field receives the binding named by its ``@ArgName`` value, or by the verbatim
field name — the deliberate, tagged boundary where the stub's ``snake_case`` argument names cross
into ``camelCase`` Java fields.  The class needs a public no-argument constructor.

.. code-block:: python

    @task.stub(queue="java")
    def score(region_code, threshold): ...


    score(region_code="emea", threshold=load_threshold())

.. code-block:: java

    public static class ScoreInput implements TaskInput {
      @ArgName("region_code")
      public String region;

      public double threshold;
    }

    @Builder.Task(id = "score")
    public long score(Client client, ScoreInput input) { ... }

A task declares flat data parameters **or** one ``TaskInput`` bundle, never both, so field names and
flat positions cannot shift each other.  Mixing them, or declaring two bundles, fails the build.

Reading bindings from the interface API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tasks written against the ``Task`` interface read the same bindings imperatively:

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - ``Client`` method
     - Returns
   * - ``hasArgs()``
     - Whether the Python Dag called this stub with any TaskFlow arguments at all.
   * - ``hasArg(int position)`` / ``hasArg(String name)``
     - Whether an argument was bound at that position, or with that name.
   * - ``getArg(int position)`` / ``getArg(String name)``
     - The bound value — the inline literal, or the bound upstream's XCom.  Throws
       ``IllegalArgumentException`` when nothing was bound there; probe with ``hasArg`` first.

.. code-block:: java

    public class ScoreTask implements Task {
      @Override
      public void execute(Context context, Client client) throws Exception {
        var rows = client.getArg(0);
        var threshold = client.hasArg("threshold") ? client.getArg("threshold") : 0.5;
        // implement task logic
      }
    }

.. _java-sdk/native-dags:

Native Java Dags
----------------

A Dag can also be authored entirely in Java, with no Python stub file: the annotations (or the
``TaskDef`` / ``DagDef`` objects) carry the configuration, and Java declares the graph.

Wiring the graph with ``@Wiring``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The annotation processor generates a ``<ClassName>Ref`` twin class whose methods mirror the
``@Builder.Task`` methods: the injected ``Client`` and ``Context`` parameters are dropped, each data
parameter takes an ``In<T>`` input, and the return value becomes a ``TaskRef<T>``.  A static
``@Wiring`` method receives the twin and calls it — calling a twin registers the task, and passing
one twin's result into another feeds the upstream's output into the downstream's parameter *and*
wires the dependency edge.  The call graph is the task graph, and ``javac`` type-checks it:

.. code-block:: java

    @Builder.Dag(
        id = "java_etl",
        schedule = "@daily",
        description = "Pure-Java Dag, no Python stub file",
        tags = {"example", "java-sdk"})
    public class EtlPipeline {

      @Builder.Task(id = "extract", retries = 2)
      public long extract() {
        return 42L;
      }

      @Builder.Task(id = "transform")
      public long transform(long extracted) {
        return extracted * 2;
      }

      @Builder.Task(id = "load")
      public void load(long transformed) {
        // implement task logic
      }

      @Wiring
      static void depends(EtlPipelineRef f) {
        f.load(f.transform(f.extract()));
      }
    }

Every ``@Builder.Task`` method must be invoked in the wiring method; a task the wiring missed fails
at Dag-parse time.  ``In.value(...)`` wires an inline literal where no upstream feeds a parameter.
The wiring method is optional — a class without one registers every task with no Java-side edges,
which is the shape for stub-backed tasks whose graph the Python Dag file owns.

.. note::

   Runtime argument bindings win over Java-declared wiring.  When the supervisor delivers bindings
   for a run (see :ref:`java-sdk/arg-binding`), the binding at a parameter's position is what the
   task receives, because for a stub task the Python call site is the graph the scheduler ordered
   the run by.  Wired inputs are the fallback, which is what a native Java Dag always uses.

Configuration attributes
~~~~~~~~~~~~~~~~~~~~~~~~

The ``@Builder.Dag`` and ``@Builder.Task`` configuration attributes, and the keys accepted by
``DagDef.config`` and ``TaskDef.config``, are generated from Airflow's Dag serialization schema, so
they carry the same names and types as their Python counterparts.  Annotation attributes are
``camelCase`` (``retryDelay``); ``config`` keys are the verbatim schema names (``"retry_delay"``).
Only attributes written explicitly at the use site are applied, so Airflow's own defaults still
apply to everything left out.

Durations and date-times are ISO-8601 strings in annotations (``retryDelay = "PT5M"``,
``startDate = "2026-01-01T00:00:00Z"``, validated at compile time) and ``java.time.Duration`` /
``java.time.OffsetDateTime`` values in ``config`` calls.  An unknown key or a mismatched value type
fails the build (annotations) or Dag parsing (``config``).

.. _java-sdk/logging:

Logging
-------

Task code can emit log records through any common Java logging framework. The SDK ships optional
integration libraries that forward those records to Airflow's task log store, where they appear
alongside the standard task output in the Airflow UI.

Declare a logger as a static field on the task class, using the class's own type as the name. This
is the conventional pattern regardless of which logging framework you choose:

.. code-block:: java

    private static final System.Logger log =
        System.getLogger(SalesPipeline.class.getName());

    @Builder.Task(id = "extract")
    public long extract(Client client) {
        log.log(System.Logger.Level.INFO, "Starting extraction");
        return recordCount;
    }

The Gradle snippets below show the dependency declarations; all Airflow artifact versions are managed
by ``airflow-sdk-bom``. Maven users apply the same artifact IDs following the pattern in
:ref:`java-sdk/build/maven`.

.. _java-sdk/logging/jpl:

``System.Logger`` (Java Platform Logging)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Java 9's new logging facade ``java.lang.System.Logger`` (JEP 264), commonly abbreviated *JPL*, can be
used by libraries without pulling in any third-party API. The ``airflow-sdk-jpl`` artifact registers an
``AirflowSystemLoggerFinder`` via ``ServiceLoader``, which routes all ``System.Logger`` calls directly
to Airflow's task log store.

.. code-block:: groovy

    implementation("org.apache.airflow:airflow-sdk-jpl:${version}")

No configuration file or startup call is required. The ``ServiceLoader`` mechanism discovers the
provider automatically as long as the JAR is on the classpath.

.. note::

    Do not add a second ``System.LoggerFinder`` implementation alongside
    ``airflow-sdk-jpl``. The JVM selects one finder via ``ServiceLoader``; having
    multiple providers on the classpath leads to unpredictable behaviour.

.. _java-sdk/logging/slf4j:

SLF4J 2.x
~~~~~~~~~

The SLF4J binding is discovered automatically via ``ServiceLoader``; no configuration file or
startup call is required.

.. code-block:: groovy

    implementation("org.apache.airflow:airflow-sdk-slf4j:${version}")

The above automatically pulls in the SLF4J API, so you don't need to add ``slf4j-api`` yourself.

.. note::

    Do not add a second SLF4J binding (such as ``logback-classic`` or ``slf4j-simple``) alongside
    ``airflow-sdk-slf4j``. SLF4J 2.x warns about multiple bindings and selects one unpredictably.

.. _java-sdk/logging/log4j2:

Log4j 2
~~~~~~~

``airflow-sdk-log4j2`` declares ``log4j-api`` as a transitive dependency, so you do not need to add the latter
separately. You must also place ``log4j-core`` on the runtime classpath to host the plugin loader that
discovers the custom ``AirflowAppender`` supplied by ``airflow-sdk-log4j2`` at startup:

.. code-block:: groovy

    implementation("org.apache.airflow:airflow-sdk-log4j2:${version}")
    runtimeOnly("org.apache.logging.log4j:log4j-core:${log4jVersion}")

Declare ``AirflowAppender`` in your ``log4j2.xml``:

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
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

.. _java-sdk/logging/jul:

``java.util.logging``
~~~~~~~~~~~~~~~~~~~~~

Add the artifact:

.. code-block:: groovy

    implementation("org.apache.airflow:airflow-sdk-jul:${version}")

and call ``AirflowJulHandler.setup()`` on startup, before any task runs. It clears the JUL root
logger's existing handlers (including the default ``ConsoleHandler``, whose stderr output Airflow
would otherwise capture as ``task.stderr`` at ERROR level, duplicating each record and mislabeling
its level) and installs ``AirflowJulHandler`` in their place:

.. code-block:: java

    public static void main(String[] args) {
        AirflowJulHandler.setup();
        Server.create(args).serve(new MyBundle());
    }

Alternatively, declare the handler in a ``logging.properties`` file and point JUL at it with the
``java.util.logging.config.file`` system property (set via ``jvm_args`` in the coordinator
configuration):

.. code-block:: properties

    handlers = org.apache.airflow.sdk.jul.AirflowJulHandler

.. code-block:: ini

    [sdk]
    coordinators = {
      "java-jdk17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
          "jars_root": ["/opt/airflow/jars"],
          "jvm_args": ["-Djava.util.logging.config.file=/opt/airflow/logging.properties"]
        }
      }
    }

.. _java-sdk/logging/other:

Other frameworks
~~~~~~~~~~~~~~~~

Several commonly used logging APIs are covered without a dedicated Airflow artifact:

* **Logback** is itself an SLF4J binding. Replace ``logback-classic`` with ``airflow-sdk-slf4j``
  and no changes are needed in your task code.
* **Apache Commons Logging (JCL)** can be bridged to SLF4J via ``org.slf4j:jcl-over-slf4j`` or
  to Log4j 2 via ``org.apache.logging.log4j:log4j-jcl``.

.. _java-sdk/types:

XCom type mapping
-----------------

XCom values are stored as JSON in Airflow's metadata database.  The table below shows how JSON types are
represented as Java objects when read back via ``getXCom``.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Python type
     - JSON
     - Java type (from ``getXCom``)
   * - ``int``
     - number (integer)
     - ``Long`` (for values that fit; ``BigInteger`` otherwise)
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

.. note::

   A data parameter whose binding resolves to a value that was never pushed receives
   ``null``.  A boxed parameter (``Integer``, ``Long``, ``Boolean``, …) receives ``null``
   safely, but a primitive parameter (``int``, ``long``, ``boolean``, …) cannot represent
   ``null`` and the task fails with ``MissingXComException``.  Declare the parameter with a
   boxed type when the upstream XCom may be absent.

.. _java-sdk/build:

Building and packaging
-----------------------

The Java SDK is distributed as a JAR. The sections below show how to build a bundle with Gradle or Maven.

.. _java-sdk/build/gradle:

Gradle
~~~~~~

Apply the Airflow SDK Gradle plugin in your ``build.gradle``:

.. code-block:: groovy

    plugins {
        id("org.apache.airflow.sdk") version "${version}"
    }

    dependencies {
        annotationProcessor("org.apache.airflow:airflow-sdk-processor:${version}")
        implementation("org.apache.airflow:airflow-sdk:${version}")
    }

    airflowBundle {
        mainClass = "com.example.Main"  // Point to your main class instead.
    }

Then run:

.. code-block:: bash

    ./gradlew bundle

The ``build/bundle/`` directory contains all required JAR(s). Copy or mount it into the directory pointed to
by ``jars_root`` in the coordinator configuration. :class:`~airflow.sdk.coordinators.java.JavaCoordinator`
scans ``jars_root`` recursively and builds the classpath automatically.

.. note::

  You only need the ``annotationProcessor`` entry if you use the annotation-based API. It is not needed for
  the interface-based API.

.. note::

  The plugin generates a fat JAR with the `Shadow <https://gradleup.com/shadow/>`__ plugin by default. This is
  generally a good idea since you only deploy one JAR file to avoid dependency issues between projects. If this
  does not suit you, set ``fatJar = false`` in ``airflowBundle`` to produce thin JARs instead. The rest of the
  process stays the same, but you will need to put all dependency JARs somewhere Airflow can find with
  ``jars_root``.

.. _java-sdk/build/maven:

Maven
~~~~~

Import the ``airflow-sdk-bom`` Bill of Materials so that artifact versions and the
``${airflow.supervisor.schema.version}`` property are managed in one place:

.. code-block:: xml

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.apache.airflow</groupId>
                <artifactId>airflow-sdk-bom</artifactId>
                <version>${version}</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

Add the SDK as a dependency (version is managed by the BOM):

.. code-block:: xml

    <dependencies>
        <dependency>
            <groupId>org.apache.airflow</groupId>
            <artifactId>airflow-sdk</artifactId>
        </dependency>
    </dependencies>

Wire the annotation processor through ``maven-compiler-plugin`` so it stays off the runtime classpath:

.. code-block:: xml

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-compiler-plugin</artifactId>
        <configuration>
            <annotationProcessorPaths>
                <path>
                    <groupId>org.apache.airflow</groupId>
                    <artifactId>airflow-sdk-processor</artifactId>
                    <version>${version}</version>
                </path>
            </annotationProcessorPaths>
        </configuration>
    </plugin>

**Option 1 (recommended): fat JAR**

Use ``maven-shade-plugin`` to bundle your code and all dependencies into a single JAR. This is the
simplest deployment: one file, no dependency management at runtime.

.. code-block:: xml

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.6.0</version>
        <executions>
            <execution>
                <phase>package</phase>
                <goals><goal>shade</goal></goals>
                <configuration>
                    <transformers>
                        <transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
                            <!-- Replace with your BundleBuilder implementation. -->
                            <mainClass>com.example.Main</mainClass>
                            <manifestEntries>
                                <!-- Resolved from the BOM; do not hard-code this value. -->
                                <Airflow-Supervisor-Schema-Version>${airflow.supervisor.schema.version}</Airflow-Supervisor-Schema-Version>
                            </manifestEntries>
                        </transformer>
                    </transformers>
                </configuration>
            </execution>
        </executions>
    </plugin>

Then run:

.. code-block:: bash

    mvn package

The fat JAR is written to ``target/<artifactId>-<version>.jar``. Copy it to the directory configured as
``jars_root`` in your coordinator.

**Option 2: thin JAR with separate dependencies**

If a fat JAR does not suit your project, use ``maven-jar-plugin`` to set ``Main-Class`` on the regular
JAR and ``maven-dependency-plugin`` to collect all runtime dependencies alongside it. Note that
``Airflow-Supervisor-Schema-Version`` does not need to be set here since Airflow reads it directly from the
``airflow-sdk`` JAR on the classpath.

.. code-block:: xml

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-jar-plugin</artifactId>
        <configuration>
            <archive>
                <manifestEntries>
                    <Main-Class>com.example.Main</Main-Class>
                </manifestEntries>
            </archive>
        </configuration>
    </plugin>

    <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-dependency-plugin</artifactId>
        <executions>
            <execution>
                <id>copy-dependencies</id>
                <phase>package</phase>
                <goals><goal>copy-dependencies</goal></goals>
                <configuration>
                    <outputDirectory>${project.build.directory}/bundle</outputDirectory>
                    <includeScope>runtime</includeScope>
                </configuration>
            </execution>
            <execution>
                <id>copy-artifact</id>
                <phase>package</phase>
                <goals><goal>copy</goal></goals>
                <configuration>
                    <artifactItems>
                        <artifactItem>
                            <groupId>${project.groupId}</groupId>
                            <artifactId>${project.artifactId}</artifactId>
                            <version>${project.version}</version>
                            <outputDirectory>${project.build.directory}/bundle</outputDirectory>
                        </artifactItem>
                    </artifactItems>
                </configuration>
            </execution>
        </executions>
    </plugin>

Then run:

.. code-block:: bash

    mvn package

``target/bundle/`` will contain the thin JAR and all runtime dependency JARs. Point ``jars_root`` at
this directory.

.. note::

  You only need the ``annotationProcessorPaths`` entry if you use the annotation-based API.

.. note::

  Unlike the Gradle plugin, Maven has no equivalent of the ``verifyBundleMainClass`` validation step.
  A wrong ``<mainClass>`` value will not be caught until runtime.

.. _java-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.java.JavaCoordinator` configuration
-------------------------------------------------------------------------------

All ``kwargs`` in the ``coordinators`` config entry are passed to the
:class:`~airflow.sdk.coordinators.java.JavaCoordinator` constructor:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``jars_root``
     - *(required)*
     - One or more directories scanned recursively for ``.jar`` files. Accepts a string,
       a path, or a list of strings/paths.
   * - ``java_executable``
     - ``"java"``
     - Path to the ``java`` binary.  Defaults to ``java`` on ``$PATH``.
   * - ``jvm_args``
     - ``[]``
     - Extra JVM arguments such as ``["-Xmx1g", "-Dsome.property=value"]``.
   * - ``main_class``
     - *(auto-detect)*
     - Explicit entry-point class. If omitted,
       :class:`~airflow.sdk.coordinators.java.JavaCoordinator` scans ``jars_root`` for a
       JAR whose manifest sets ``Main-Class``. If multiple executable JARs are found the
       result is non-deterministic; set ``main_class`` explicitly in that case.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for the JVM subprocess to connect after launch.  Increase this if your
       JVM startup is slow (e.g. on constrained hardware or with a large classpath).

.. note::

  The ``[sdk]`` configuration is read at startup, so changes to ``coordinators`` or
  ``queue_to_coordinator`` (for example adding ``jvm_args``) only take effect after you restart the
  scheduler (or ``airflow standalone``). A rebuilt bundle JAR, by contrast, is picked up on the next
  task launch without a restart, because a fresh JVM is spawned per task instance.

.. _java-sdk/java-executable:

Pinning the Java executable
---------------------------

As a general recommendation, set ``java_executable`` to an absolute path rather than relying on
``java`` resolving from ``$PATH``. This pins tasks to a known JDK, which matters most in production or
corporate environments where the Airflow admin may not control the system-wide ``java`` (the same
reasoning behind pinning a Python version).

For example, if you install the JDK with Homebrew on macOS, its ``java`` is not on ``$PATH``, so
point ``java_executable`` at it explicitly:

.. code-block:: ini

    [sdk]
    coordinators = {
      "java-jdk17": {
        "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
        "kwargs": {
          "jars_root": ["/opt/airflow/jars"],
          "java_executable": "/opt/homebrew/opt/openjdk@17/bin/java"
        }
      }
    }
    queue_to_coordinator = {"java": "java-jdk17"}

.. _java-sdk/limitations:

Limitations
-----------

* **One JVM subprocess per task instance.**  Each task instance spawns a fresh JVM. Tasks that need to share
  in-process state between instances should use XCom or an external store instead.
* **Limited support for assets, deferral, and other Airflow features.** They may be implemented in the future
  based on user feedback and demand.
