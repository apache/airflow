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

The Java SDK is distributed as a JAR. Use any build tool; Gradle is shown here.

**Gradle setup**

Add the SDK dependency to your ``build.gradle.kts``:

.. code-block:: kotlin

    dependencies {
      implementation("org.apache.airflow:airflow-java-sdk:<version>")
      annotationProcessor("org.apache.airflow:airflow-java-sdk:<version>")
    }

    tasks.withType<Jar> {
      manifest {
        attributes("Main-Class" to "com.example.Main")
      }
    }

.. note::

  You only need the ``annotationProcessor`` entry if you use the annotation-based API. It is not needed for
  the interface-based API.

.. note::

  The ``Main-Class`` manifest value is needed for the coordinator to know how to run the JAR. You can choose
  to set this *on the coordinator itself* too by adding the ``main_class`` kwarg in coordinator configuration.

Building a distribution
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

    ./gradlew :myproject:installDist

The ``lib/`` directory of the resulting distribution contains all required JARs. Copy or mount it into the
directory pointed to by ``jars_root`` in the coordinator configuration.
:class:`~airflow.sdk.coordinators.java.JavaCoordinator` scans ``jars_root``
recursively and builds the classpath automatically.

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
