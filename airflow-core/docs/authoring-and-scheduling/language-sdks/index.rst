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

.. _language-sdks:

Non-Python Task SDKs
====================

|experimental|

Airflow Dags are always defined in Python, but individual task *implementations* can be written in other
languages. When a task runs, Airflow's worker calls out the target language to execute the task logic, which
communicates the result back. The Dag author uses a lightweight Python *stub* to declare where the task lives;
everything else, including the actual business logic, Airflow API calls, and any library dependencies, lives
in a non-Python implementation.

.. list-table:: Available language SDKs
   :header-rows: 1
   :widths: 15 40 15 30

   * - Language
     - Coordinator class
     - Min. runtime
     - Guide
   * - JVM languages (e.g. Java)
     - :class:`task-sdk:airflow.sdk.coordinators.java.JavaCoordinator`
     - JRE 17
     - :doc:`java`
   * - Go
     - :class:`task-sdk:airflow.sdk.coordinators.executable.ExecutableCoordinator`
     - None (native binary)
     - :doc:`go`

.. toctree::
   :hidden:

   java
   go

How it works
------------

The execution model has three moving parts.

**Stub tasks in the Dag**
   The Dag file declares tasks using :func:`@task.stub <airflow.sdk.task.stub>`. A stub is a normal Airflow
   task from the scheduler's perspective. It participates in dependencies, retries, pools, and all other
   task-level features exactly like other ``@task``-decorated Python functions. The only difference is that
   the worker does not execute the Python code inside the function definition; instead it delegates execution
   to a *coordinator*.

**Coordinators**
   A coordinator is a Python object registered in the ``[sdk] coordinators`` configuration. This is considered
   a part of an Airflow worker. When the worker picks up a stub task, it looks up the coordinator mapped to
   that task's specified ``queue``, and uses the coordinator to execute the task. The coordinator is
   responsible for managing the target language's runtime, forwarding messages from, and relaying results back
   to Airflow. All coordinators extend
   :class:`task-sdk:airflow.sdk.execution_time.coordinator.BaseCoordinator`.

**Language runtime**
   The coordinator calls out one short-lived runtime per task instance. In most cases, this would be a
   subprocess of an executable implemented in a non-Python language. The runtime receives messages from the
   worker to identify the workload, executes the task, and communicates through the coordinator as a proxy
   back to the worker process.

.. _language-sdks/stub-tasks:

Stub tasks
----------

A stub task is declared with the :func:`@task.stub <airflow.sdk.task.stub>` decorator. Since it is still a
Python task declaration, every parameter available on a normal Dag or task applies. Task dependencies are also
defined in the Python Dag file. The scheduler treats a stub like any other task.

.. code-block:: python

    import datetime

    from airflow.sdk import dag, task


    @dag
    def my_pipeline():
        raw = fetch_data()  # normal Python task

        @task.stub(
            queue="java",  # routes to the JavaCoordinator
            retries=3,
            retry_delay=datetime.timedelta(minutes=5),
            execution_timeout=datetime.timedelta(hours=1),
            pool="heavy_tasks",
        )
        def process(raw_value): ...  # implemented in Java

        @task.stub(queue="java")
        def export(processed_value): ...

        export(process(raw))


    my_pipeline()

The ``queue`` parameter determines which coordinator handles the task. Any other ``@task`` keyword argument is
stored on the task instance and honored by Airflow's scheduler and worker as usual.

XCom values produced by a stub task are visible to downstream Python tasks and vice-versa. However, although
XCom references should be defined inside the Python Dag (they are task dependencies), you still need to
actually read the values out in the language implementation, and vice versa. See specific language SDK
documentation on how to do this correctly.

.. _language-sdks/coordinator-config:

Coordinator configuration
-------------------------

Coordinators are registered in ``airflow.cfg`` (or via environment variables) under ``[sdk]``.

``coordinators``
    A JSON object mapping a logical coordinator name to its class and keyword arguments:

    .. code-block:: ini

        [sdk]
        coordinators = {
            "my-coordinator": {
                "classpath": "path.to.CoordinatorClass",
                "kwargs": {}
            }
        }

    The ``classpath`` value must be importable by the worker.  The ``kwargs`` are passed directly
    to the coordinator's constructor.  See the language-specific guide for the accepted kwargs
    of each coordinator (e.g. :ref:`java-sdk/coordinator-config` for
    :class:`~airflow.sdk.coordinators.java.JavaCoordinator`).

``queue_to_coordinator``
    A JSON object mapping Celery queue names to coordinator names:

    .. code-block:: ini

        [sdk]
        queue_to_coordinator = {"jdk17": "my-coordinator"}

    Tasks with ``queue="jdk17"`` on their stub will be dispatched to the coordinator named
    ``"my-coordinator"``.  A single coordinator can serve multiple queues; a queue can only
    map to one coordinator.

Both settings can be supplied as environment variables using the standard Airflow convention:

.. code-block:: bash

    AIRFLOW__SDK__COORDINATORS='{"my-coordinator": {...}}'
    AIRFLOW__SDK__QUEUE_TO_COORDINATOR='{"jdk17": "my-coordinator"}'

.. _language-sdks/bundle-spec:

Implementing a new compiled language SDK
----------------------------------------

:class:`task-sdk:airflow.sdk.coordinators.executable.ExecutableCoordinator` runs a task by executing the
bundle file directly. It therefore fits **only compiled languages whose build artifact is a standalone
binary the worker can execute with no additional runtime dependency** - that is, no language runtime,
virtual machine, or interpreter has to be installed on the worker for the binary to run (Go, Rust, C, C++,
Zig, ...). Languages whose artifact still needs a runtime present at execution time do not fit this
coordinator; JVM languages, for example, compile to bytecode that requires a JRE, and are served by the
:class:`task-sdk:airflow.sdk.coordinators.java.JavaCoordinator` instead.

To support a new such language, produce a *bundle* in the shared on-disk format the coordinator consumes and
speak the coordinator IPC protocol (the ``--comm`` / ``--logs`` socket arguments). That format - the
``AFBNDL01`` footer appended to the executable, the binary integrity hash, and the ``airflow-metadata.yaml``
manifest of ``dag_id``\ s and ``task_id``\ s - is specified, together with the reader algorithm and the
compatibility/versioning rules, in :doc:`task-sdk:executable-bundle-spec`. That page also publishes a
machine-readable JSON Schema for the manifest, for use by build tooling and validators. Follow the spec to
make a new language's bundles discoverable by Airflow with no change to the scheduler, worker, or UI; the
:doc:`Go SDK <go>` is a worked reference implementation.
