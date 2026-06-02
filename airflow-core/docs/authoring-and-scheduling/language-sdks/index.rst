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

.. toctree::
   :hidden:

   java

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
