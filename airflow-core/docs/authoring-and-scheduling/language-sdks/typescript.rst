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

The TypeScript SDK lets you register task handlers on a ``Bundle`` and implement their logic in TypeScript (or
plain JavaScript), running on Node.js. A matching Python stub Dag still declares the scheduling shape and
dependencies; individual tasks delegate to a Node.js subprocess that is spawned by
:class:`~airflow.sdk.coordinators.node.NodeCoordinator` for each task instance.

The SDK is the ``apache-airflow-ts-sdk`` package (ESM-only). It is currently in **beta** and its API may change.

.. warning::

  Install an available release from npm. To try an unreleased change, build it from source in the
  `ts-sdk/ <https://github.com/apache/airflow/tree/main/ts-sdk>`__ directory of the Airflow repository
  and depend on it locally (see ``ts-sdk/example/`` for a working setup).

.. seealso::

  For the full TypeScript API reference (``Bundle``, ``TaskHandler``, ``Dag``, the task handler getters,
  ``TaskClient``, supporting types, and exceptions),
  see the `TypeScript SDK API reference <https://airflow.apache.org/docs/ts-sdk/stable/>`__.

.. contents:: Contents
   :local:
   :depth: 2

Prerequisites
-------------

* Node.js 22 or later must be available on the Airflow worker nodes.
* The packed bundle (a single ``bundle.min.mjs`` file, see :ref:`typescript-sdk/build`) must be accessible
  from the worker, under a directory the coordinator scans.
* The ``apache-airflow-task-sdk`` package (installed with Airflow) provides the coordinator; no additional
  Python packages are needed.
* In the TypeScript project, install the ``apache-airflow-ts-sdk`` npm package to author task handlers:

  .. code-block:: bash

      npm install apache-airflow-ts-sdk

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

A task is an ordinary (usually ``async``) function taking no arguments:
``getContext()`` and ``getClient()`` reach the runtime from inside the call, so nothing the SDK supplies is a parameter.

Create a ``TaskHandler`` per task, binding the function to the ``dag_id`` and ``task_id`` it implements,
register them on a ``Bundle``, then serve it to Airflow with ``bundle.serve()``.
That top-level ``await`` makes the module a runnable bundle entry point.

.. code-block:: typescript

    import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

    export async function buildMessage() {
      const client = getClient();
      const upstream = await client.getXCom<string>({
        key: "return_value",
        taskId: "python_start",
      });
      const greeting = await client.getVariable("typescript_example_greeting");
      return `${greeting ?? "hello from TypeScript"}; upstream=${upstream ?? "missing"}`;
    }

    const bundle = new Bundle();
    bundle.register(new TaskHandler("typescript_example", "build_message", buildMessage));
    await bundle.serve();

The ``dagId`` a handler binds must match the ``dag_id`` of the Python Dag, and the ``taskId`` a
``@task.stub`` function in that Dag, including any TaskGroup prefix.

``register`` takes any number of task handlers and ``bundle.serve()`` serves exactly what is registered,
so a task left out is not part of the packed bundle and is marked removed at runtime.
A second ``bundle.serve()`` call is rejected.
Registering holds no sockets and starts nothing, so a unit test can build a bundle and dispatch a handler
through ``bundle.getTaskHandler(dagId, taskId)`` without a coordinator runtime.

``Dag`` is another interface, for a Dag declared in TypeScript rather than in Python, and is still a work
in progress. ``new Dag`` and ``dag.task`` take a trailing options object (``spec`` on both, plus
``inputs`` on a task) that is not used yet; do not set them.

TaskFlow arguments
~~~~~~~~~~~~~~~~~~

A Python Dag that calls a stub task TaskFlow-style passes those arguments straight to the handler, which
destructures them by name:

.. code-block:: python

    @task.stub(queue="typescript")
    def transform(region_code: str, threshold: float, dry_run: bool = False): ...


    transform("uk", 0.75)

.. code-block:: typescript

    interface TransformArgs {
      regionCode: string;
      threshold: number;
      dryRun: boolean;
    }

    export async function transform({ regionCode, threshold, dryRun }: TransformArgs) {
      // ...
    }

Names bind by **folding on both sides**, lowercased with underscores removed, so ``region_code`` reaches
``regionCode`` and ``s3_uri`` reaches ``s3Uri`` with nothing declared on either side.
The Go SDK folds identically, so one Python signature binds the same way in either SDK.
An argument the call leaves at its default arrives carrying the default's value.

A name nothing folds to is **logged, not thrown**, naming what the handler asked for and what the call
delivered. Two Python names that fold to the same token fail the task.

``Object.keys`` and rest destructuring (``{ ...rest }``) yield Python's names, and ``in`` folds like a read.

An argument the call fills from another task, as in ``transform(extract(), "uk")``,
arrives as that task's value rather than a reference to it.
An upstream that pushed no output fails the task, naming both the argument and the task it came from;
one that pushed ``null`` binds ``null``.

A Python ``int`` beyond the ±9007199254740991 a JavaScript number holds exactly is refused rather than
bound, so carry such a value across the language boundary as a string.

.. note::

  Being upstream is not the same as being passed. As with the other language SDKs, an XCom *dependency*
  declared with ``>>`` in the Python stub Dag defines task order only. Read a value the call did not pass
  explicitly via ``getClient().getXCom``, and produce one either by the task's return value or by
  ``getClient().setXCom``.

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

.. note::

  The coordinator runs inside the Airflow worker, so the ``[sdk]`` config (and the packed ``*.min.mjs``
  bundles in ``bundles_root``) only need to be present wherever tasks actually execute. With
  ``CeleryExecutor``, setting them on the Celery workers is sufficient. With ``LocalExecutor``, tasks run
  inside the scheduler process, so they must be present where the scheduler can read them. The API server
  and Dag processor do not need them.

Writing tasks
-------------

A task handler takes no SDK-supplied argument. Two getters, valid for as long as the handler runs, supply what it needs:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Getter
     - Value
   * - ``getContext()``
     - The task's execution context (a ``TaskContext``): ``dagId``, ``taskId`` (including any TaskGroup
       prefix), ``runId``, ``tryNumber``, ``mapIndex`` (``-1`` for an unmapped task), and ``signal``, an
       ``AbortSignal`` that fires when Airflow terminates the task. Pass ``signal`` to ``fetch()``, timers,
       or other APIs that accept an ``AbortSignal`` for cooperative cancellation.
   * - ``getClient()``
     - A ``TaskClient`` for Airflow Variables, Connections, and XCom.

Both read a store the runtime installs around the handler call,
which follows the handler across every ``await`` and into every promise it creates,
so a helper several frames deep reads them without being passed anything. Both throw outside a handler.

Work that outlives the handler is the one gap.
A promise the handler never awaits still resolves, but it runs after Airflow has been told the task's terminal state,
so await everything a handler starts.

A non-``undefined`` return value becomes the task's ``return_value`` XCom, matching Python ``@task``
behavior. An uncaught exception (or rejected promise) marks the task instance failed in Airflow, triggering
retries if configured on the stub.

The ``TaskClient`` surface
~~~~~~~~~~~~~~~~~~~~~~~~~~

* ``getVariable(key)`` returns the Variable as a string, or ``null`` when it is missing;
  ``getVariableOrThrow(key)`` throws ``VariableNotFoundError`` instead, matching Python ``Variable.get``
  with no default.
* ``getConnection(connId)`` returns a ``ConnectionResult`` with fields ``id`` and ``type``, plus the
  optional fields ``host``, ``schema``, ``login``, ``password``, ``port``, and ``extra`` (each may be
  missing or ``null``), or ``null`` when the connection does not exist;
  ``getConnectionOrThrow(connId)`` throws ``ConnectionNotFoundError`` instead, matching Python
  ``BaseHook.get_connection``.
* ``getXCom<T>({key, ...})`` reads an XCom value, or ``null`` when it is missing. The locator fields
  (``dagId``, ``runId``, ``taskId``, ``mapIndex``) default to the current task; pass ``taskId`` to read an
  upstream task's XCom. See :ref:`typescript-sdk/types` for how the stored JSON maps to JavaScript types.
* ``setXCom({key, value, ...})`` publishes an XCom value.

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
a single self-contained, minified ESM file, ``bundle.min.mjs``, and embeds the manifest (the ``dag_id`` and
``task_id`` map plus the supervisor schema version) after a leading compact JSON ``//# airflowBundle=...``
layout header. The layout records the byte ranges and SHA-256 digests of the manifest and executable code,
so there is one file to deploy, with no separate manifest or ``node_modules``.

The code is minified because an integrity digest is only worth taking over an artifact nobody is expected to
read or edit in place. The ``/*! */`` license banners of bundled dependencies are kept. Nothing is identified by
a function name, so minified names are safe: a Dag and a task are named by the string ids their registration
states, and a handler is dispatched by reference.

``esbuild`` is an optional peer dependency: packing is build-time only, so the runtime install of
``apache-airflow-ts-sdk`` skips it, and it must be installed separately before running ``airflow-ts-pack``.

.. code-block:: bash

    npm install --save-dev esbuild
    npx airflow-ts-pack src/main.ts --outdir dist

Use ``--outdir <dir>`` to choose the output directory (default ``dist``), ``--outfile <path>`` to name the
artifact exactly, which helps when one ``bundles_root`` holds several bundles, and ``--source <name>`` to set
the source name displayed in the Airflow UI (default: the entry file's basename). ``--outdir`` and
``--outfile`` are mutually exclusive, and an ``--outfile`` name must end in ``.min.mjs`` so the coordinator
can find it.

Deploying
~~~~~~~~~

Copy or mount the bundle into a directory listed in the coordinator's ``bundles_root``.
:class:`~airflow.sdk.coordinators.node.NodeCoordinator` searches the configured directories in order,
recursively, and launches the first integrity-verified ``*.min.mjs`` bundle whose metadata declares the task
instance's Dag. The artifact's name does not matter beyond that suffix, so one root can hold several bundles
and a Dag is routed to whichever declares it. If multiple bundles declare the same Dag, the first configured
root wins, and within a root the first in sorted path order.

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
     - One or more directories searched recursively, in order, for an integrity-verified ``*.min.mjs``
       bundle that declares the requested Dag. Accepts a string, a path, or a list of strings/paths.
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
* **Beta status.** The SDK API may change in incompatible ways between releases.
* **One Node.js subprocess per task instance.** Tasks that need to share in-process state between instances
  should use XCom or an external store instead.
