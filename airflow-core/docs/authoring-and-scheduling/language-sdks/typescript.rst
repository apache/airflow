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

* Node.js 22 or later must be available on the Airflow worker nodes and the Dag processor.
  Once a ``NodeCoordinator`` is configured, the Dag processor runs ``node`` on every packed ``*.min.mjs`` bundle
  in every Dag bundle, including ones that only register ``TaskHandler`` objects,
  and reports an import error for each when ``node`` is missing.
* The packed bundle (a single ``bundle.min.mjs`` file, see :ref:`typescript-sdk/build`) must be accessible
  from the worker and the Dag processor, in the Dag bundle the coordinator scans.
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

``register`` takes any number of task handlers and Dags, and ``bundle.serve()`` serves exactly what is
registered, so a task left out is not part of the packed bundle and is marked removed at runtime.
A second ``bundle.serve()`` call is rejected.
Registering holds no sockets and starts nothing, so a unit test can build a bundle and dispatch a handler
through ``bundle.getTaskHandler(dagId, taskId)`` without a coordinator runtime.

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

Explicit renames
~~~~~~~~~~~~~~~~

``withArgNames`` states a binding when folding cannot reach it, for a name the Python side never used:
a clearer word than the Dag chose, or a TypeScript reserved word like ``enum``.
The mapping comes first, the handler second:

.. code-block:: typescript

    interface ReportArgs {
      summary: Summary;
      label: string; // Python calls this `run_label`
    }

    const report = withArgNames({ label: "run_label" }, async ({ summary, label }: ReportArgs) => {
      // `label` is the call's `run_label`; `summary` folded as usual.
    });

    bundle.register(new TaskHandler("etl", "report", report));

An entry beats folding, and everything the map does not mention still folds,
so ``withArgNames`` should be rare in a real Dag.
A mapped name the call did not pass misses rather than falling back to folding.

The map's keys are checked against the handler's own parameter type, so ``{ labl: "run_label" }`` is a
compile error naming the right key. Its values are Python names, which ``tsc`` cannot see and does not
check.

.. note::

  Being upstream is not the same as being passed. As with the other language SDKs, an XCom *dependency*
  declared with ``>>`` in the Python stub Dag defines task order only. Read a value the call did not pass
  explicitly via ``getClient().getXCom``, and produce one either by the task's return value or by
  ``getClient().setXCom``.

Coordinator configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Register a Dag bundle for the packed bundles, register the coordinator, and route the queue to it in
``airflow.cfg`` (or the equivalent ``AIRFLOW__*`` environment variables):

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
        {
          "name": "ts-task-handlers",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {"path": "/opt/airflow/ts-bundles"}
        }
      ]

    [sdk]
    coordinators = {
      "ts": {
        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
        "kwargs": {"task_handler_bundle_name": "ts-task-handlers"}
      }
    }
    queue_to_coordinator = {"typescript": "ts"}

``task_handler_bundle_name`` names the Dag bundle the coordinator scans for packed bundles;
``queue_to_coordinator`` routes stub tasks with ``queue="typescript"`` to this coordinator. See
:ref:`typescript-sdk/coordinator-config` for the full list of accepted ``kwargs`` and how the bundle is
located.

There is no separate Node.js worker to run: the Airflow worker launches the bundle with ``node`` once per
task instance.

A Dag processor with this :ref:`[sdk] <config:sdk>` configuration also parses the ``*.min.mjs`` bundles of every Dag bundle,
and needs Node.js to do so (see :ref:`typescript-sdk/native-parsing`).
The ``ts-task-handlers`` Dag bundle only holds the bundles that the Python stub Dag's tasks run,
so keep the Dag processor from parsing it by listing ``*`` in its ``.airflowignore``:

.. code-block:: bash

    echo '*' > /opt/airflow/ts-bundles/.airflowignore

.. note::

  The :ref:`[sdk] <config:sdk>` config, the packed ``*.min.mjs`` bundles and Node.js must be present wherever tasks execute
  and on the Dag processor. With ``CeleryExecutor``, tasks execute on the Celery workers; with
  ``LocalExecutor``, they run inside the scheduler process. The Dag processor checks the stub tasks of each
  Python Dag against the task handlers the packed bundles register, so it runs them too.
  It also runs them to parse the bundles of every Dag bundle, see :ref:`typescript-sdk/native-parsing`.
  The API server does not need any of it.
  Register the Dag bundle in :ref:`[dag_processor] dag_bundle_config_list <config:dag_processor__dag_bundle_config_list>` on every
  component, like your other Dag bundles: the worker and the Dag processor resolve
  ``task_handler_bundle_name`` through it, and wherever the :ref:`[sdk] <config:sdk>` config is read it is rejected if the
  name is missing there.

.. _typescript-sdk/native-dag:

Declaring a Dag in TypeScript
-----------------------------

A ``Dag`` is declared on this side rather than in Python: its schedule, its tasks, their options and
the edges between them are all written in TypeScript. Airflow parses such a Dag from its packed bundle,
see :ref:`typescript-sdk/native-parsing`.

``dag.task(taskId, handler)`` returns a *factory*. A handler takes one object of named arguments, and
calling the factory names each input, so the call graph is the task graph:

.. code-block:: typescript

    import { Dag } from "apache-airflow-ts-sdk";

    const dag = new Dag("ts_etl");

    const extract = dag.task("extract", async (): Promise<number> => 42);
    const transform = dag.task(
      "transform",
      async ({ rows, region }: { rows: number; region: string }) => rows * 2,
    );
    const load = dag.task("load", async ({ total }: { total: number }) => {});

    const extracted = extract();
    const total = transform({ rows: extracted, region: "us" });
    load({ total });

Naming the inputs is how a task is called. A handler that takes no arguments is called with none, and
a single argument is named like any other, ``load({ total })``. The compiler checks the call: it reports
an argument left out, a misspelled one, and a literal of the wrong type.

Each argument takes either an upstream reference or a literal JSON value. A reference has to be the
argument itself: one buried inside an array or an object is a literal, and draws no edge.

Every task has to be called exactly once. An uncalled task fails when the Dag is read, so none can be
left out of the graph by accident.

The task id may be omitted, in which case it is the handler's function name:

.. code-block:: typescript

    const extract = dag.task(async function extract(): Promise<number> {
      return 42;
    });

``airflow-ts-pack`` keeps function names intact, so bundling cannot rename a task. A handler with no
name of its own, such as an arrow function passed inline, has nothing to take an id from and needs
one: either positionally or as ``taskId`` in its spec. Give it in one place only, not both.

Order-only edges
~~~~~~~~~~~~~~~~

An edge that carries no value has no argument name to travel under, so it is drawn between the
references themselves with ``before`` and ``after``, the TypeScript pair for Python's ``>>`` and
``<<``:

.. code-block:: typescript

    const loaded = load({ transformed });
    const cleaned = cleanup();

    loaded.before(cleaned);                  // loaded >> cleaned
    cleaned.after(loaded, transformed);      // [loaded, transformed] >> cleaned

Both take any number of references, so one call draws several edges, and drawing an edge that
already exists changes nothing. Each returns the reference it was called on, so
``loaded.before(cleaned).before(notified)`` draws both edges from ``loaded``.

Pass a value as an argument when the downstream task needs it, and use ``before`` or ``after`` when
it only needs to run in order.

Task groups
~~~~~~~~~~~

``dag.taskGroup(groupId)`` opens a scope with the same ``task`` and ``taskGroup`` methods as the Dag,
prefixing the id of everything declared in it, as Python's ``prefix_group_id`` does:

.. code-block:: typescript

    const staging = dag.taskGroup("staging");
    staging.task("stage_rows", stageRows)();        // task id "staging.stage_rows"
    staging.taskGroup("checks").task("nulls", checkNulls)();  // "staging.checks.nulls"

    staging.before(loaded);                          // staging >> loaded

A group is an edge endpoint in its own right, so ``before`` and ``after`` order a whole group against
a task or against another group.

Tasks and groups share one id namespace, as they do in Python, so a Dag cannot hold both a task and a
group called ``staging``. A ``.`` is what separates a group from what it holds, so it cannot appear in
an id of either.

Pass ``{ prefixGroupId: false }`` to keep the ids declared in a group as written, as ``prefix_group_id=False``
does in Python; they then have to be unique across the Dag. A group id is made of letters, digits, dashes and
underscores, and is at most 200 characters.

Serialization
~~~~~~~~~~~~~

A native Dag serializes into the same Dag JSON a Python Dag produces, so the scheduler reads it
without knowing which language declared it.

``schedule`` accepts what maps to a stock timetable: unset, ``@once``, ``@continuous``, or a cron
expression. A cron preset such as ``@daily`` is recorded as the expression it stands for. Anything
else names a Python object a TypeScript bundle cannot point at, and is rejected.

A cron schedule is read in UTC. Python takes the Dag's timezone from ``start_date``, but
``startDate`` here is a ``Date``, which is an instant and carries no timezone, so there is none to
take.

Every task of a native Dag runs on the Node coordinator, so it needs the queue the deployment routes
there. Set it once on the Dag and each task inherits it:

.. code-block:: typescript

    const dag = new Dag("ts_etl", { schedule: "@daily", queue: "typescript" });

    // ...and one task that needs its own.
    dag.task("heavy", heavyHandler, { queue: "typescript_large" })();

``queue`` on a task wins over the Dag's. See :ref:`typescript-sdk/coordinator-config` for the
``queue_to_coordinator`` entry that sends that queue to the coordinator.

Conditional branching
~~~~~~~~~~~~~~~~~~~~~

``dag.if`` takes a handler that returns a boolean, and names the task each outcome runs:

.. code-block:: typescript

    async function hasRows({ rows }: { rows: number }): Promise<boolean> {
      return rows > 0;
    }

    const gate = dag.if(hasRows, { rows: extracted });
    gate.then(loaded).else(reportedEmpty);

``dag.if`` declares the condition as a task: its id is the function's name, and a trailing spec sets
its id or other options, as in ``dag.if(hasRows, { rows }, { taskId: "has_rows" })``. The compiler
checks that the handler returns a boolean. ``else`` is optional: a one-sided condition skips its own
branch when the condition fails and follows nothing. The condition is also a node, so
``notified.after(gate)`` orders a task after it.

A guarded task takes no argument for the control edge, because a condition's boolean decides whether
the task runs rather than what it runs on. Read a value from the condition with
``getClient().getXCom``.

The side not taken is skipped when the run reaches it, and stays skipped if you clear it later. Only
the branches named here are skipped, so a task that several branches converge on still runs — unlike
Python's ``@task.branch``, which skips every immediate downstream it did not follow.

Multi-way branching
~~~~~~~~~~~~~~~~~~~

``dag.switch`` is the multi-way form: a handler that returns one of the cases it is given.

.. code-block:: typescript

    async function pickPath({ rows }: { rows: number }): Promise<TaskRef> {
      return rows > 1000 ? handleLong : handleShort;
    }

    dag.switch(pickPath, { rows: extracted }).case(handleLong).case(handleShort);

``dag.switch`` declares the decider the way ``dag.if`` does. A case is the task reference itself, so
the compiler checks the candidate exists and renaming a handler cannot silently rewire a Dag. The
task's own value is the chosen task's id, which a downstream task can read from its XCom.

There is no default case. A decider that returns anything outside its cases fails the task, naming
what it chose and what it could have chosen.

Exactly one case is selected. Python's branch callable may return a list of task ids, and no language
SDK offers that yet: put the paths that run together behind one task, or gate each with its own
condition.

Triggering another Dag
~~~~~~~~~~~~~~~~~~~~~~

``triggerDagRun`` starts another Dag's run. Pass it to ``dag.task`` in place of a handler:

.. code-block:: typescript

    import { triggerDagRun } from "apache-airflow-ts-sdk";

    const trigger = dag.task(triggerDagRun({ dagId: "downstream_etl", waitForCompletion: true }), {
      taskId: "trigger_downstream",
    })();

    trigger.after(loaded);

The task has no handler to take an id from, so ``taskId`` is required, and the factory takes no
inputs. The trailing spec carries the task's other options, as for any task.

The task runs in the TypeScript runtime, like the Dag's other tasks, and inherits the Dag's queue. It
behaves as ``TriggerDagRunOperator`` does. ``waitForCompletion`` polls the run every ``pokeInterval``
seconds until it reaches one of ``allowedStates`` or ``failedStates``. With ``deferrable`` as well, the
task defers to ``DagStateTrigger`` instead of holding a worker slot, and resumes in this runtime when
the run finishes. ``DagStateTrigger`` runs in the Python triggerer, so the triggerer needs the standard
provider installed.

The task pushes the ``trigger_run_id`` XCom, and the task's "Triggered DAG" link opens the run it
started.

The task follows these config options, with Python's fallback when one is unset:

- ``[api] base_url`` is the base of the "Triggered DAG" link. It falls back to ``/``.
- ``[operators] default_deferrable`` is the default of ``deferrable``. It falls back to ``false``.
- ``[triggerer] queues_enabled`` decides whether the deferred trigger gets the task's queue. It falls
  back to ``false``, so the trigger gets none.

Some of what ``TriggerDagRunOperator`` does is not offered:

- ``logical_date`` and ``run_after``. The triggered run's logical date is the time the task triggers
  it, as in Python when neither is set.
- Jinja. Values are sent as written, so ``{{ ds }}`` in ``conf`` reaches the triggered run as that
  literal string.
- OpenLineage parent injection (``openlineage_inject_parent_info``). The runtime does not add the
  parent task's OpenLineage details to ``conf``.

``new Dag`` and ``dag.task`` both take a trailing spec of Airflow options:
``{ schedule: "@daily", tags: ["etl"] }`` for the Dag, ``{ retries: 2, retryDelay: 30 }`` for a task.

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
* ``setVariable(key, value, description?)`` stores a Variable, replacing any existing value, and
  ``deleteVariable(key)`` removes one. Values are stored as strings, so serialize structured data (for
  example with ``JSON.stringify``) before storing it.
* ``getConnection(connId)`` returns a ``ConnectionResult`` with fields ``id`` and ``type``, plus the
  optional fields ``host``, ``schema``, ``login``, ``password``, ``port``, and ``extra`` (each may be
  missing or ``null``), or ``null`` when the connection does not exist;
  ``getConnectionOrThrow(connId)`` throws ``ConnectionNotFoundError`` instead, matching Python
  ``BaseHook.get_connection``.
* ``getXCom<T>({key, ...})`` reads an XCom value, or ``null`` when it is missing. The locator fields
  (``dagId``, ``runId``, ``taskId``, ``mapIndex``) default to the current task; pass ``taskId`` to read an
  upstream task's XCom. See :ref:`typescript-sdk/types` for how the stored JSON maps to JavaScript types.
* ``setXCom({key, value, ...})`` publishes an XCom value.

.. note::

   A value supplied by a secrets backend (for example an ``AIRFLOW_VAR_*`` environment variable) still
   takes precedence over the stored value when the Variable is read back. Calling ``setVariable`` without
   a description clears the description the Variable had, and ``deleteVariable`` resolves even when the
   key does not exist.

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
read or edit in place. Function names are kept through minification, since a task id defaults to its
handler's name. The ``/*! */`` license banners of bundled dependencies are kept.

Because the shipped code is not the code anyone wrote, the packer also embeds source files verbatim, each in
its own ``/*# airflowSource:<path> ... #*/`` block comment verified by its own digest: the entry module, and
each native Dag's file (the file its ``new Dag(...)`` constructor ran in). The ``dag_source_paths`` field in
the manifest maps each ``dag_id`` to its file. Files that only supply utilities or types are not embedded.
The Code view currently shows the embedded entry module for each Dag in the bundle.

``esbuild`` is an optional peer dependency: packing is build-time only, so the runtime install of
``apache-airflow-ts-sdk`` skips it, and it must be installed separately before running ``airflow-ts-pack``.

.. code-block:: bash

    npm install --save-dev esbuild
    npx airflow-ts-pack src/main.ts --outdir dist

Use ``--outdir <dir>`` to choose the output directory (default ``dist``), ``--outfile <path>`` to name the
artifact exactly, which helps when one Dag bundle holds several bundles, and ``--source <name>`` to set
the source name displayed in the Airflow UI (default: the entry file's basename). ``--outdir`` and
``--outfile`` are mutually exclusive, and an ``--outfile`` name must end in ``.min.mjs`` so the coordinator
can find it.

Deploying
~~~~~~~~~

Copy or mount the bundle into the Dag bundle named by the coordinator's ``task_handler_bundle_name``.
For a task of a Python stub Dag,
:class:`~airflow.sdk.coordinators.node.NodeCoordinator` searches that Dag bundle recursively and launches the
first integrity-verified ``*.min.mjs`` bundle whose metadata declares the task instance's Dag. The artifact's
name does not matter beyond that suffix, so one Dag bundle can hold several bundles and a Dag is routed to
whichever declares it. If multiple bundles declare the same Dag, the first in sorted path order wins.
A task of a native TypeScript Dag runs the bundle its Dag was parsed from, see
:ref:`typescript-sdk/native-parsing`.

.. _typescript-sdk/native-parsing:

Parsing native Dags
-------------------

A Dag declared in TypeScript ships in its packed bundle, so the bundle goes into a Dag bundle,
next to any Python Dag files.
To have Airflow parse it, configure a :class:`~airflow.sdk.coordinators.node.NodeCoordinator`.
The Dag processor runs ``node`` on the bundle to list its Dags, so it needs Node.js, as the workers do:

.. code-block:: ini

    [sdk]
    coordinators = {
      "ts": {
        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
        "kwargs": {"node_executable": "/usr/local/bin/node"}
      }
    }
    queue_to_coordinator = {"typescript": "ts"}

Once a ``NodeCoordinator`` is configured, the Dag processor parses the packed bundles of every Dag bundle,
so it needs this :ref:`[sdk] <config:sdk>` configuration and Node.js. With one ``NodeCoordinator``, it parses them all. With several,
map each Dag bundle that holds native TypeScript Dags to one of them in :ref:`[sdk] dag_bundle_to_coordinator <config:sdk__dag_bundle_to_coordinator>`.
Each packed bundle of a Dag bundle that has no entry, or whose entry names no ``NodeCoordinator``,
fails to parse with an import error:

.. code-block:: ini

    [sdk]
    dag_bundle_to_coordinator = {"dags-folder": "ts"}

A Dag bundle that holds only the bundles that Python stub Dags' tasks run should list ``*`` in its ``.airflowignore``.
Otherwise, with several Node coordinators, its bundles fail to parse.

A task of a native Dag runs the bundle file its Dag was parsed from, on the ``NodeCoordinator`` its queue routes to,
which need not be the one that parsed it. For example, a queue can route to a coordinator that uses another Node.js.
A task whose queue routes to another kind of coordinator fails without retries. A task whose bundle is missing,
or that the coordinator cannot run (for example, because the bundle fails its integrity check),
fails and retries while it has retries left.
A task of a Python stub Dag runs the first bundle, in sorted path order, that declares its Dag.

The Dag processor parses only files that end in ``.min.mjs`` and start with the header ``airflow-ts-pack`` writes.
A bundle that fails its integrity check, or whose Dags cannot be serialized or fail validation,
such as a cycle drawn with ``before`` and ``after``, is reported as an import error.

Do not declare a Dag in TypeScript that a Python file in the same bundle also defines. When you move a Dag
such as the Quick start's ``typescript_example`` to ``new Dag(...)``, remove its Python stub, otherwise the
two files overwrite each other's Dag on every parse.

The Code view currently shows the bundle's entry module for each of its Dags, as ``airflow-ts-pack``
embeds it (see :ref:`typescript-sdk/build`). If the source cannot be read from the bundle, the view shows a
short notice instead.

A bundle that only registers ``TaskHandler`` objects is parsed too,
since its metadata does not say whether it declares Dags. Each parse then launches ``node`` once and finds no Dags.
To avoid that cost, list such bundles in ``.airflowignore``; the coordinator still finds them to run tasks.

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
   * - ``task_handler_bundle_name``
     - *(task's own Dag bundle)*
     - Name of the Dag bundle searched recursively for an integrity-verified ``*.min.mjs`` bundle that
       declares the requested Dag. It is used only by mixed-language Dags, to locate the task handlers for
       the ``@task.stub`` tasks of a Python Dag; Dags defined natively in a language SDK do not use it. It
       must be registered in :ref:`[dag_processor] dag_bundle_config_list <config:dag_processor__dag_bundle_config_list>`. It is checked when the :ref:`[sdk] <config:sdk>`
       configuration is loaded, so a typo fails there rather than on the first task.
   * - ``node_executable``
     - ``"node"``
     - Path to the ``node`` binary. Defaults to ``node`` on ``$PATH``.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for the Node.js subprocess to connect after launch. Increase this if your bundle
       startup is slow (e.g. on constrained hardware).

.. note::

  **Locating the bundle.** The packed bundles for the ``@task.stub`` tasks of a Python Dag are read from a
  Dag bundle, so they are delivered, refreshed and versioned by the same machinery as your Dags.

  * The expected layout is a separate Dag bundle for the packed bundles, named by
    ``task_handler_bundle_name``, rather than the Dag bundle that holds your ``.py`` files. The task uses
    the version that Dag bundle is on when it starts, pinned for the whole task.
  * If ``task_handler_bundle_name`` is unset, the bundle is read from the **task's own** Dag bundle, pinned
    to the version the run was created with.

  A task of a native TypeScript Dag ignores ``task_handler_bundle_name``:
  it runs the bundle of its Dag from the Dag's own bundle, at the version the run was created with.
  See :ref:`typescript-sdk/native-parsing`.

Limitations
-----------

* **A Python stub Dag is still required.** The Execution API does not yet carry Dag structure for non-Python
  languages, so task names and dependencies are declared in Python with
  :func:`@task.stub <airflow.sdk.task.stub>`.
* **Cluster policies do not apply to a native Dag.** ``dag_policy`` and ``task_policy`` are not run on
  it, so they cannot change or reject it.
* **Some CLI commands do not take a native Dag.** ``airflow dags test``, ``tasks test`` and ``tasks render`` refuse it,
  and ``airflow dags reserialize`` does not store the Dags of ``*.min.mjs`` bundles,
  which only the Dag processor stores.
  ``airflow tasks list`` lists its tasks by running the bundle with ``node``, so it needs Node.js.
* **Beta status.** The SDK API may change in incompatible ways between releases.
* **One Node.js subprocess per task instance.** Tasks that need to share in-process state between instances
  should use XCom or an external store instead.
