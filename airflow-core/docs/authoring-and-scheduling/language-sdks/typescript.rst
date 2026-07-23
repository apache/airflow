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

.. _typescript-sdk:

TypeScript SDK
==============

|experimental|

The TypeScript SDK lets you implement Airflow task logic in TypeScript (or plain JavaScript), running on
Node.js. The Dag and its scheduling remain in Python; individual tasks delegate to a Node.js subprocess that
is spawned by :class:`~airflow.sdk.coordinators.node.NodeCoordinator` for each task instance.

The SDK is the ``@apache-airflow/ts-sdk`` package (ESM-only). It is currently in **alpha** and its API may change.

.. warning::

  The SDK is not yet published to npm. To try it today, build it from source in the
  `ts-sdk/ <https://github.com/apache/airflow/tree/main/ts-sdk>`__ directory of the Airflow repository and
  depend on it locally (see ``ts-sdk/example/`` for a working setup).

.. contents:: Contents
   :local:
   :depth: 2

Prerequisites
-------------

* Node.js 22 or later must be available on the Airflow worker nodes.
* The packed bundle (a single ``bundle.mjs`` file, see :ref:`typescript-sdk/build`) must be accessible from
  the worker, under a directory the coordinator scans.
* The ``apache-airflow-task-sdk`` package (installed with Airflow) provides the coordinator; no additional
  Python packages are needed.

Quick start
-----------

The following example shows the minimal moving parts: a Python Dag with a stub task, and a TypeScript
implementation of that task.

Python Dag (the scheduling side)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def typescript_example():
        @task
        def python_start():
            return "hello from Python"

        @task.stub(queue="typescript")
        def build_message(): ...

        python_start() >> build_message()


    typescript_example()

``@task.stub`` declares the *shape* of the TypeScript task without any Python implementation. The ``queue``
value routes the task to the Node.js coordinator.

TypeScript implementation
~~~~~~~~~~~~~~~~~~~~~~~~~

A task is an ordinary (usually ``async``) function receiving ``TaskHandlerArgs``. Register it with the
``dag_id`` and ``task_id`` it implements, then start the coordinator runtime; the registrations and the
top-level ``await startCoordinator()`` make the module a runnable bundle entry point.

.. code-block:: typescript

    import { registerTask, startCoordinator, type TaskHandlerArgs } from "@apache-airflow/ts-sdk";

    export async function buildMessage({ ctx, client }: TaskHandlerArgs) {
      const upstream = await client.getXCom<string>({
        key: "return_value",
        taskId: "python_start",
      });
      const greeting = await client.getVariable("typescript_example_greeting");
      return `${greeting ?? "hello from TypeScript"}; upstream=${upstream ?? "missing"}`;
    }

    registerTask({ dagId: "typescript_example", taskId: "build_message" }, buildMessage);

    await startCoordinator();

The ``dagId`` passed to ``registerTask`` must match the ``dag_id`` of the Python Dag, and each ``taskId``
must match a ``@task.stub`` function in that Dag.

.. note::

  As with the other language SDKs, XCom *dependencies* are declared in the Python stub Dag (they define task
  order). The value must still be read explicitly in TypeScript via ``client.getXCom``, and produced either
  by the task's return value or by ``client.setXCom``.

Coordinator configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Register the coordinator and route the queue to it under ``[sdk]`` in ``airflow.cfg`` (or the equivalent
``AIRFLOW__SDK__*`` environment variables):

.. code-block:: ini

    [sdk]
    coordinators = {
      "ts": {
        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
        "kwargs": {"bundles_root": ["/opt/airflow/ts-bundles"]}
      }
    }
    queue_to_coordinator = {"typescript": "ts"}

``bundles_root`` is one or more directories the coordinator scans for bundles; ``queue_to_coordinator``
routes stub tasks with ``queue="typescript"`` to this coordinator. See
:ref:`typescript-sdk/coordinator-config` for the full list of accepted ``kwargs``.

There is no separate Node.js worker to run: the Airflow worker launches the bundle with ``node`` once per
task instance.

Writing tasks
-------------

Every task handler receives a single ``TaskHandlerArgs`` object:

.. list-table::
   :header-rows: 1
   :widths: 15 85

   * - Field
     - Value
   * - ``ctx``
     - The task's execution context: ``dagId``, ``taskId`` (including any TaskGroup prefix), ``runId``,
       ``tryNumber``, ``mapIndex`` (``-1`` for an unmapped task), and ``signal`` — an ``AbortSignal`` that
       fires when Airflow terminates the task. Pass ``signal`` to ``fetch()``, timers, or other APIs that
       accept an ``AbortSignal`` for cooperative cancellation.
   * - ``client``
     - A ``TaskClient`` for Airflow Variables, Connections, and XCom.

A non-``undefined`` return value becomes the task's ``return_value`` XCom, matching Python ``@task``
behavior. An uncaught exception (or rejected promise) marks the task instance failed in Airflow, triggering
retries if configured on the stub.

The ``TaskClient`` surface
~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``getVariable(key)`` — returns the Variable as a string, or ``null`` when it is missing;
  ``getVariableOrThrow(key)`` throws ``VariableNotFoundError`` instead, matching Python ``Variable.get``
  with no default.
* ``getConnection(connId)`` — returns a ``ConnectionResult`` with fields ``id`` and ``type``, plus the
  optional fields ``host``, ``schema``, ``login``, ``password``, ``port``, and ``extra`` (each may be
  missing or ``null``), or ``null`` when the connection does not exist.
* ``getXCom<T>({key, ...})`` — reads an XCom value, or ``null`` when it is missing. The locator fields
  (``dagId``, ``runId``, ``taskId``, ``mapIndex``) default to the current task; pass ``taskId`` to read an
  upstream task's XCom. See :ref:`typescript-sdk/types` for how the stored JSON maps to JavaScript types.
* ``setXCom({key, value, ...})`` — publishes an XCom value.

Logging
-------

Anything the task writes to stdout or stderr (``console.log``, ``console.error``) is captured by the worker
and shown in the Airflow task log (stdout at ``INFO`` level, stderr at ``ERROR`` level). The SDK does not
yet expose a dedicated structured-logging API.

.. _typescript-sdk/types:

XCom type mapping
-----------------

XCom values are stored as JSON in Airflow's metadata database. The table below shows how those JSON types
surface as JavaScript values when read back via ``getXCom``.

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Python type
     - JSON
     - JavaScript type (from ``getXCom``)
   * - ``int``
     - number (integer)
     - ``number`` (see note)
   * - ``float``
     - number (decimal)
     - ``number``
   * - ``str``
     - string
     - ``string``
   * - ``bool``
     - boolean
     - ``boolean``
   * - ``None``
     - null
     - ``null``
   * - ``list``
     - array
     - ``Array``
   * - ``dict``
     - object
     - ``object``

.. note::

  JavaScript has a single ``number`` type (an IEEE 754 double), so integers and decimals arrive as the same
  type, and integers larger than ``Number.MAX_SAFE_INTEGER`` (2\ :sup:`53` − 1) may lose precision.

.. _typescript-sdk/build:

Building and packaging
----------------------

``airflow-ts-pack`` (shipped with the SDK) bundles the entry module and all of its imports with esbuild into
a single self-contained ESM file, ``bundle.mjs``, and embeds the manifest (the ``dag_id`` and ``task_id``
map plus the supervisor schema version) as a leading ``//# airflowMetadata=<base64>`` comment — one file to
deploy, with no separate manifest or ``node_modules``.

.. code-block:: bash

    npx airflow-ts-pack src/main.ts --outdir dist

Use ``--outdir <dir>`` to choose the output directory (default ``dist``) and ``--source <name>`` to set the
source name displayed in the Airflow UI (default: the entry file's basename).

Deploying
~~~~~~~~~

Copy or mount ``bundle.mjs`` into a directory listed in the coordinator's ``bundles_root``.
:class:`~airflow.sdk.coordinators.node.NodeCoordinator` searches the configured directories in order and
launches the first usable bundle with ``node``.

.. _typescript-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.node.NodeCoordinator` configuration
---------------------------------------------------------------------

All ``kwargs`` in the ``coordinators`` config entry are passed to the
:class:`~airflow.sdk.coordinators.node.NodeCoordinator` constructor:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``bundles_root``
     - *(required)*
     - One or more directories searched, in order, for a ``bundle.mjs`` (with embedded metadata, or with an
       ``airflow-metadata.yaml`` sidecar). Accepts a string, a path, or a list of strings/paths.
   * - ``node_executable``
     - ``"node"``
     - Path to the ``node`` binary. Defaults to ``node`` on ``$PATH``.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for the Node.js subprocess to connect after launch. Increase this if your bundle
       startup is slow (e.g. on constrained hardware).

Limitations
-----------

* **A Python stub Dag is still required.** The Execution API does not yet carry Dag structure for non-Python
  languages, so task names and dependencies are declared in Python with
  :func:`@task.stub <airflow.sdk.task.stub>`.
* **Alpha status.** The SDK API may change in incompatible ways between releases.
* **One bundle per coordinator.** :class:`~airflow.sdk.coordinators.node.NodeCoordinator` launches the first
  usable bundle found in ``bundles_root``; it does not yet route different Dags or tasks to different
  bundles. To serve multiple bundles, register multiple coordinators on separate queues.
* **One Node.js subprocess per task instance.** Tasks that need to share in-process state between instances
  should use XCom or an external store instead.
