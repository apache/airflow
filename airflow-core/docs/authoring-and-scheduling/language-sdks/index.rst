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

Airflow Dags and tasks can be written in languages other than Python. A language SDK is used in one of two
ways:

* **Dag definition.** The whole Dag, with its schedule, tasks and dependencies, is defined in the SDK's language.
  See :ref:`language-sdks/dag-definition`.
* **Python Dag with stub TaskHandler.** A Python Dag declares the tasks and their dependencies, and a
  TaskHandler in the SDK's language implements each stub task. See :ref:`language-sdks/stub-taskhandler`.

Either way, a task runs on a *coordinator*, which starts the language's runtime for the task and relays
messages between it and Airflow. See :ref:`language-sdks/coordinator-config`.

.. list-table:: Available language SDKs
   :header-rows: 1
   :widths: 15 35 15 15 20

   * - Language
     - Coordinator class
     - Min. runtime
     - Dag definition
     - Guide
   * - JVM languages (e.g. Java)
     - :class:`task-sdk:airflow.sdk.coordinators.java.JavaCoordinator`
     - JRE 17
     - Yes
     - :doc:`java`
   * - Go
     - :class:`task-sdk:airflow.sdk.coordinators.executable.ExecutableCoordinator`
     - None (native binary)
     - Yes
     - :doc:`go`
   * - TypeScript
     - :class:`task-sdk:airflow.sdk.coordinators.node.NodeCoordinator`
     - Node.js 22
     - Yes
     - :doc:`typescript`

.. toctree::
   :hidden:

   java
   go
   typescript

.. _language-sdks/dag-definition:

Dag definition
--------------

The whole Dag is declared in the SDK's language: its schedule, its tasks, their options and the dependencies
between them. No Python file is involved.

The SDK builds the Dag into an artifact, and the artifact goes into a Dag bundle like any other Dag file. The
Dag processor runs the artifact with the language's runtime to read its Dags, so the Dag processor needs that
runtime and the ``[sdk]`` coordinator configuration. From there, Airflow treats the Dag like any Python Dag:
the scheduler plans its runs, it shows up in the UI, and each task runs on the coordinator its queue maps to,
from the same Dag bundle.

The **Code** view in the Airflow UI shows the source the SDK embeds in the artifact.

For TypeScript, see :ref:`typescript-sdk/dag-definition`.

.. _language-sdks/stub-taskhandler:

Python Dag with stub TaskHandler
--------------------------------

A Python Dag declares a task that another language implements as a *stub task*. The scheduler sees a normal
task: it takes part in dependencies, retries, pools and every other task-level feature like any other
``@task`` function. When a worker picks it up, the worker does not run the Python function body; it hands the
task to the coordinator its queue maps to, which runs the implementation in the target language.

.. _language-sdks/stub-tasks:

Stub tasks
~~~~~~~~~~

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

.. note::

    For a Dag containing stub tasks, the **Code** view in the Airflow UI shows only the Python Dag file,
    including the stub declarations, as the Dag's source. The non-Python implementation source is not
    displayed anywhere in the UI; consult your project repository or the build artifact shipped in the bundle
    to inspect it. This is an intentional architecture decision, not a bug.

.. _language-sdks/coordinator-config:

Coordinator configuration
-------------------------

A coordinator is a Python object registered in the ``[sdk] coordinators`` configuration, and runs as part of an
Airflow worker. When the worker picks up a task, it looks up the coordinator mapped to the task's ``queue``,
and uses it to run the task. The coordinator starts one short-lived runtime per task instance, usually a
subprocess of an executable written in the target language, and relays messages between that runtime and the
worker. All coordinators extend :class:`task-sdk:airflow.sdk.execution_time.coordinator.BaseCoordinator`.

Coordinators are registered in ``airflow.cfg`` (or via environment variables) under ``[sdk]``.

``coordinators``
    A JSON object mapping a logical coordinator name to its class and keyword arguments:

    .. code-block:: ini

        [sdk]
        coordinators = {
            "my-coordinator": {
                "classpath": "path.to.CoordinatorClass",
                "kwargs": {},
                "extra": {}
            }
        }

    The ``classpath`` value must be importable by the worker and the Dag processor.  The ``kwargs``
    are passed directly to the coordinator's constructor.  See the language-specific guide for the
    accepted kwargs of each coordinator (e.g. :ref:`java-sdk/coordinator-config` for
    :class:`~airflow.sdk.coordinators.java.JavaCoordinator`).

    ``extra`` is an optional object for any additional information you want to associate with a
    coordinator without coupling it to the coordinator instance. The coordinator itself never
    receives it; other components read it as needed. For example, KubernetesExecutor reads
    ``extra.pod_template_file`` to launch a queue's worker pod from a specific pod template, and
    ``extra.worker_container_repository`` + ``extra.worker_container_tag`` to override that queue's
    worker base image (both keys are required), e.g. an image that bundles the JVM for a Java
    coordinator.

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
