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

Prerequisites
-------------

* JRE 17 or later must be available on the Airflow worker nodes.
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
      public long transform(
        Client client,
        @Builder.XCom(task = "extract") long recordCount
      ) {
        var threshold = (String) client.getVariable("transform_threshold");
        // ... process data ...
        return transformedCount;
      }
    }

.. note::

  See how both ``transform`` in Python and Java need to have an argument to accept upstream XCom. The
  Python one is needed to declare dependency, and the Java one is needed to actually retrieve the value.

Java entry point
~~~~~~~~~~~~~~~~

.. code-block:: java

    public class Main implements BundleBuilder {
      @Override
      public Iterable<Dag> getDags() {
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
     - Marks the class as a task container.  The ``id`` must match the ``dag_id`` in the Python Dag.
   * - ``@Builder.Task(id = "...")``
     - Marks a method as a task implementation.  The ``id`` must match the ``@task.stub`` function
       name in the Python Dag.  If ``id`` is omitted the method name is used.
   * - ``@Builder.XCom(task = "...")``
     - Injects the ``return_value`` XCom from the named upstream task as a method parameter.
       The parameter type must be compatible with the stored value (see :ref:`java-sdk/types`).

The annotation processor generates a ``<ClassName>Builder`` class that wires up the task
registry and handles XCom injection automatically.

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
      public long process(
        Client client,
        @Builder.XCom(task = "fetch") String fetched
      ) {
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
read.

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
      public Iterable<Dag> getDags() {
        var dag = new Dag("my_dag");
        dag.addTask("fetch", FetchTask.class);
        dag.addTask("process", ProcessTask.class);
        return List.of(dag);
      }
    }

See the Java SDK's published JavaDoc for more details.

.. TODO: (AIP-108) Put a link here once we publish the JavaDoc.

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

.. _java-sdk/limitations:

Limitations
-----------

* **One JVM subprocess per task instance.**  Each task instance spawns a fresh JVM. Tasks that need to share
  in-process state between instances should use XCom or an external store instead.
* **Limited support for assets, deferral, and other Airflow features.** They may be implemented in the future
  based on user feedback and demand.
