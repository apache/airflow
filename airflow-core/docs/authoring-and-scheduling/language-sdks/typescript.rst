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

The TypeScript SDK lets you write Airflow Dags and tasks in TypeScript, or plain JavaScript, and run them on
Node.js. There are two ways to use it:

* **Dag definition.** The schedule, the tasks, their options and the dependencies between them are all written
  in TypeScript, with no Python file involved. See :ref:`typescript-sdk/dag-definition`.
* **Python Dag with stub TaskHandler.** A Python Dag declares the tasks with ``@task.stub`` and wires them
  together, and a TypeScript ``TaskHandler`` implements each one. See :ref:`typescript-sdk/stub-tasks`.

Both use the same task API and the same build tool, and one bundle can serve both.

The SDK is the ``apache-airflow-ts-sdk`` npm package (ESM-only). It is in **beta**, and its API may change.

.. warning::

  Install an available release from npm. To try an unreleased change, build it from source in the
  `ts-sdk/ <https://github.com/apache/airflow/tree/main/ts-sdk>`__ directory of the Airflow repository
  and depend on it locally (see ``ts-sdk/example/`` for a working setup).

.. seealso::

  For the full API reference (``Dag``, ``Bundle``, ``TaskHandler``, ``withArgNames``, the task getters,
  ``TaskClient``, supporting types, and exceptions), see the
  `TypeScript SDK API reference <https://airflow.apache.org/docs/ts-sdk/stable/>`__.

.. contents:: Contents
   :local:
   :depth: 2

Prerequisites
-------------

* Node.js 22 or later on the Airflow workers. A Dag declared in TypeScript also needs Node.js on the Dag
  processor, which runs the bundle to read the Dag. To add Node.js to the Airflow image, extend it as
  described in :doc:`docker-stack:build`. If ``node`` is not on the ``PATH``, set the coordinator's
  ``node_executable`` (see :ref:`typescript-sdk/coordinator-config`).
* The ``apache-airflow-task-sdk`` package, installed with Airflow, provides
  :class:`~airflow.sdk.coordinators.node.NodeCoordinator`, which runs the TypeScript code. No other Python
  package is needed.
* In your TypeScript project, install the SDK, and ``esbuild`` to build the bundle:

  .. code-block:: bash

      npm install apache-airflow-ts-sdk
      npm install --save-dev esbuild

.. _typescript-sdk/quick-start:

Quick start
-----------

This example declares a whole Dag in TypeScript, builds it into a bundle, and deploys it, with no Python
file involved.

Declare a Dag in ``src/main.ts``:

.. code-block:: typescript

    import { Bundle, Dag } from "apache-airflow-ts-sdk";

    const dag = new Dag("ts_hello", { schedule: "@daily", queue: "typescript" });

    const extract = dag.task("extract", async (): Promise<number> => 42);
    const report = dag.task("report", async ({ rows }: { rows: number }) => {
      console.log(`extracted ${rows} rows`);
    });

    report({ rows: extract() });

    await new Bundle(dag).serve();

Build it into a single file:

.. code-block:: bash

    npx airflow-ts-pack src/main.ts --outdir dist

Make sure ``dist/bundle.min.mjs`` is in a Dag bundle, for example the default ``dags-folder`` bundle, which
reads the ``[core] dags_folder`` directory (see :doc:`Dag bundles </administration-and-deployment/dag-bundles>`).
Then send the ``typescript`` queue to the Node.js coordinator in ``airflow.cfg``:

.. code-block:: ini

    [sdk]
    coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
    queue_to_coordinator = {"typescript": "ts"}

Airflow reads the bundle like any other Dag file: ``ts_hello`` shows up in the UI, runs on its schedule,
and ``report`` receives the value ``extract`` returned.

.. _typescript-sdk/dag-definition:

Declaring a Dag in TypeScript
-----------------------------

Tasks and their inputs
~~~~~~~~~~~~~~~~~~~~~~

``dag.task(taskId, handler)`` declares a task and returns a *factory*. Calling the factory places the task in
the Dag and gives it its inputs, so the calls you write are the dependencies:

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

A handler takes one object of named arguments, and a call names each input. An input is either the
reference another task's call returned, which makes this task wait for that task and receive its value, or a
literal JSON value such as ``"us"``. A task with no arguments is called with none, and a single argument is
named like any other: ``load({ total })``.

The compiler checks every call: an argument left out, a misspelled one, and a literal of the wrong type are
all compile errors.

The argument type can be a named interface, and the handler a function declared anywhere, including in another
module. This is also how a task passes data to the next one:

.. code-block:: typescript

    interface Summary {
      total: number;
      regions: number;
    }

    interface ReportArgs {
      summary: Summary;
      label: string;
    }

    export async function report({ summary, label }: ReportArgs) {
      console.log(`${label}: ${summary.total} rows in ${summary.regions} regions`);
    }

    const summarize = dag.task("summarize", async (): Promise<Summary> => ({ total: 42, regions: 2 }));
    const reportTask = dag.task("report", report);

    reportTask({ summary: summarize(), label: "nightly" });

``report`` receives the object ``summarize`` returned as ``summary``, and the literal ``"nightly"`` as
``label``. Values move between tasks as XComs, so a task returns data JSON can hold (see
:ref:`typescript-sdk/types`).

A few rules keep the graph honest:

* Every task is called exactly once. A task that is never called fails when the Dag is read, so none is left
  out of the graph by accident.
* A reference has to be the input itself. One placed inside an array or an object is treated as a literal and
  draws no dependency.

Task ids
~~~~~~~~

The task id may be left out, in which case it is the handler's function name:

.. code-block:: typescript

    const extract = dag.task(async function extract(): Promise<number> {
      return 42;
    });

``airflow-ts-pack`` keeps function names, so building the bundle never renames a task. A handler with no name
of its own, such as an arrow function written inline, needs an id: pass it first, or set ``taskId`` in the
task's options. Give it in one place only.

A task id you write is made of letters, digits, dashes and underscores.

Dag and task options
~~~~~~~~~~~~~~~~~~~~

``new Dag`` and ``dag.task`` both take a trailing object of Airflow options. The fields are Airflow's own,
spelled in camelCase, and a misspelled one is a compile error:

.. code-block:: typescript

    const dag = new Dag("ts_etl", {
      schedule: "@daily",
      catchup: false,
      tags: ["etl"],
      queue: "typescript",
    });

    const load = dag.task("load", loadRows, { retries: 2, retryDelay: 30 });

``schedule`` accepts ``@once``, ``@continuous``, a cron expression, or a cron preset such as ``@daily``. Leave
it out for a Dag that only runs when triggered. A cron schedule runs in UTC.

Every task of the Dag runs on the Node.js coordinator, so it needs a queue that
:ref:`queue_to_coordinator <typescript-sdk/coordinator-config>` sends there. Set ``queue`` once on the Dag and
each task inherits it. A task's own ``queue`` wins over the Dag's.

Order-only dependencies
~~~~~~~~~~~~~~~~~~~~~~~

When a task only has to run after another, without receiving its value, draw the dependency between the
references with ``before`` and ``after``, the TypeScript spelling of Python's ``>>`` and ``<<``:

.. code-block:: typescript

    const loaded = load({ total });
    const cleaned = cleanup();

    loaded.before(cleaned);                // loaded >> cleaned
    cleaned.after(loaded, extracted);      // [loaded, extracted] >> cleaned

Both take any number of references, and drawing a dependency that already exists changes nothing. Each returns
the reference it was called on, so ``loaded.before(cleaned).before(notified)`` draws both from ``loaded``.

Pass a value as an input when the downstream task needs it, and use ``before`` or ``after`` when it only needs
to run in order.

Wrap a reference in ``label`` to name the edge drawn to it, as Python's ``Label`` does:

.. code-block:: typescript

    import { label } from "apache-airflow-ts-sdk";

    checked.before(label(processed, "rows found"), label(notified, "no rows"));

Redrawing an existing edge, including one an input drew, labels it.

Task groups
~~~~~~~~~~~

``dag.taskGroup(groupId)`` opens a group with the same ``task`` and ``taskGroup`` methods as the Dag. Every id
declared in it is prefixed with the group's, as in Python:

.. code-block:: typescript

    const staging = dag.taskGroup("staging");
    const staged = staging.task("stage_rows", stageRows)();          // task id "staging.stage_rows"
    staging.taskGroup("checks").task("nulls", checkNulls)();         // task id "staging.checks.nulls"

    staging.before(loaded);                                          // staging >> loaded

A group takes ``before`` and ``after`` too, which orders the whole group against a task or another group.

Tasks and groups share one id namespace, so a Dag cannot hold both a task and a group called ``staging``. The
``.`` is added by the group, so an id you write cannot contain one.

Pass ``{ prefixGroupId: false }`` to keep the ids declared in a group as written, as ``prefix_group_id=False``
does in Python; they then have to be unique across the Dag. A group id is made of letters, digits, dashes and
underscores, and is at most 200 characters.

Conditional branching
~~~~~~~~~~~~~~~~~~~~~

``dag.if`` declares a task from a handler that returns a boolean, and names the task each outcome runs:

.. code-block:: typescript

    async function hasRows({ total }: { total: number }): Promise<boolean> {
      return total > 0;
    }

    dag.if(hasRows, { total }).then(loaded).else(reportedEmpty);

Its inputs come second, and an optional third argument takes task options, such as ``taskId``; the id is
otherwise the handler's name. The compiler checks that the handler returns a boolean. ``else`` is optional:
without it, the ``then`` task is skipped when the condition is false.

The tasks a condition guards take no input from it. To use a value the condition computed, read it with
``getClient().getXCom``.

The side not taken is skipped, and stays skipped if it is cleared later. Only the tasks named here are
skipped, so a task that both sides lead to still runs. This differs from Python's ``@task.branch``, which
skips every direct downstream task it did not choose.

Multi-way branching
~~~~~~~~~~~~~~~~~~~

``dag.switch`` declares a task from a handler that returns the reference of the task to run, and lists the
candidates:

.. code-block:: typescript

    const daily = publishDaily();
    const weekly = publishWeekly();

    async function pickCadence() {
      const cadence = await getClient().getVariable("cadence");
      return cadence === "weekly" ? weekly : daily;
    }

    dag.switch(pickCadence).case(daily).case(weekly);

It takes inputs and options as ``dag.if`` does. A case is a task reference, so the compiler checks it exists,
and renaming a handler cannot silently change which task runs. The deciding task's value is the id of the
task it chose.

Exactly one case runs, and there is no default: a decider that returns anything else fails, naming what it
returned and what it could have returned. To run several tasks together on one outcome, put them behind a
single task, or give each its own ``dag.if``.

Triggering another Dag
~~~~~~~~~~~~~~~~~~~~~~

``triggerDagRun`` makes a task that starts a run of another Dag. Pass it to ``dag.task`` in place of a handler:

.. code-block:: typescript

    import { triggerDagRun } from "apache-airflow-ts-sdk";

    const triggered = dag.task(
      "trigger_downstream",
      triggerDagRun({ dagId: "downstream_etl", conf: { source: "ts_etl" }, waitForCompletion: true }),
    )();

    triggered.after(loaded);

The task has no handler to take an id from, so it needs one, and it takes no inputs. ``triggerDagRun`` takes
the options of Python's ``TriggerDagRunOperator``, such as ``conf``, ``waitForCompletion``, ``deferrable``,
``pokeInterval``, ``allowedStates`` and ``failedStates``. Values are sent as written, so Jinja in ``conf`` is
not rendered.

The task runs on Node.js like the Dag's other tasks, and inherits the Dag's ``queue``. With ``deferrable``, it
waits in the triggerer, which needs the standard provider installed.

A complete example
~~~~~~~~~~~~~~~~~~

``ts-sdk/example/src/native.ts`` uses all of the above in one Dag: a task group, a fan-in, a condition, a
multi-way branch, order-only dependencies and a triggered Dag run.

.. _typescript-sdk/stub-tasks:

Implementing stub tasks of a Python Dag
---------------------------------------

Here a Python Dag declares the tasks and their dependencies, and TypeScript implements some of them. Use it
when the Dag needs something a TypeScript Dag cannot express yet (see :ref:`typescript-sdk/limitations`), or to
mix Python and TypeScript tasks in one Dag.

The Python Dag
~~~~~~~~~~~~~~

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

``@task.stub`` declares a task that is implemented elsewhere, and ``queue`` sends it to the Node.js
coordinator.

The TypeScript side
~~~~~~~~~~~~~~~~~~~

Create a ``TaskHandler`` per stub task, naming the ``dag_id`` and ``task_id`` it implements, and register them
on a ``Bundle``:

.. code-block:: typescript

    import { Bundle, getClient, TaskHandler } from "apache-airflow-ts-sdk";

    export async function buildMessage() {
      const client = getClient();
      const upstream = await client.getXCom<string>({ key: "return_value", taskId: "python_start" });
      const greeting = await client.getVariable("typescript_example_greeting");
      return `${greeting ?? "hello from TypeScript"}; upstream=${upstream ?? "missing"}`;
    }

    const bundle = new Bundle();
    bundle.register(new TaskHandler("typescript_example", "build_message", buildMessage));
    await bundle.serve();

The ``taskId`` must match the stub's task id, including any task group prefix.

``register`` takes any number of task handlers and Dags, so one bundle can implement the tasks of several
Python Dags and declare Dags of its own. A task left out of the bundle is marked removed when it runs.

Nothing connects to Airflow until ``bundle.serve()``, so a unit test can build a bundle and call a handler
through ``bundle.getTaskHandler(dagId, taskId)`` without running Airflow.

TaskFlow arguments
~~~~~~~~~~~~~~~~~~

A Python Dag that calls a stub task TaskFlow-style passes those arguments to the handler, which destructures
them by name:

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

Names match ignoring case and underscores, so ``region_code`` reaches ``regionCode`` and ``s3_uri`` reaches
``s3Uri`` with nothing declared on either side. The Go SDK matches names the same way. An argument the call
leaves at its default arrives with the default's value.

A name the handler reads that the call did not pass is logged rather than raised, naming what the handler
asked for and what the call delivered. Two Python names that match the same TypeScript name fail the task.

An argument filled from another task, as in ``transform(extract(), "uk")``, arrives as that task's value. An
upstream task that produced no value fails the task, naming the argument and the task; one that returned
``None`` delivers ``null``.

A Python ``int`` larger than a JavaScript number holds exactly (beyond ±9007199254740991) is refused, so pass
such a value as a string.

Renaming an argument
~~~~~~~~~~~~~~~~~~~~

``withArgNames`` maps a handler's name to a different Python name, for when the two cannot match on their own:
a clearer word than the Dag chose, or a TypeScript reserved word such as ``enum``. The map comes first, the
handler second:

.. code-block:: typescript

    interface ReportArgs {
      summary: Summary;
      label: string; // the Python call passes this as `run_label`
    }

    const report = withArgNames({ label: "run_label" }, async ({ summary, label }: ReportArgs) => {
      // ...
    });

    bundle.register(new TaskHandler("etl", "report", report));

Every name the map does not mention still matches as usual, so ``withArgNames`` should be rare. The map's keys
are checked against the handler's argument type, so ``{ labl: "run_label" }`` is a compile error.

.. note::

  A dependency drawn with ``>>`` in the Python Dag orders the tasks and passes nothing. To read a value the
  call did not pass, use ``getClient().getXCom``.

Writing tasks
-------------

A handler is an ordinary, usually ``async``, function. It works the same in both ways of using the SDK.

Two functions give it what it needs while it runs:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Function
     - Returns
   * - ``getContext()``
     - The task's context (a ``TaskContext``): ``dagId``, ``taskId`` (including any task group prefix),
       ``runId``, ``tryNumber``, ``mapIndex`` (``-1`` for an unmapped task), and ``signal``, an ``AbortSignal``
       that fires when Airflow stops the task. Pass ``signal`` to ``fetch()``, timers, or any other API that
       accepts one, so the task can stop cleanly.
   * - ``getClient()``
     - A ``TaskClient`` for Airflow Variables, Connections, XComs and the task state store.

Both work anywhere inside the handler's call, including helper functions and code after an ``await``, and
throw outside it. Await everything the handler starts: work still running after the handler returns runs
after Airflow has recorded the task's result.

A value the handler returns becomes the task's ``return_value`` XCom, as with Python's ``@task``, and is what a
downstream task receives. An uncaught exception or a rejected promise fails the task, and retries it if the task
has retries.

The ``TaskClient``
~~~~~~~~~~~~~~~~~~

* ``getVariable(key)`` returns the Variable as a string, or ``null`` when it does not exist.
  ``getVariableOrThrow(key)`` throws ``VariableNotFoundError`` instead, like Python's ``Variable.get`` without
  a default.
* ``setVariable(key, value, description?)`` stores a Variable, replacing any existing value, and
  ``deleteVariable(key)`` removes one. Values are strings, so serialize structured data, for example with
  ``JSON.stringify``, before storing it.
* ``getConnection(connId)`` returns a ``ConnectionResult`` with ``id`` and ``type``, plus ``host``, ``schema``,
  ``login``, ``password``, ``port`` and ``extra``, each of which may be missing or ``null``. It returns ``null``
  when the connection does not exist, and ``getConnectionOrThrow(connId)`` throws ``ConnectionNotFoundError``
  instead, like Python's ``BaseHook.get_connection``.
* ``getXCom<T>({ key, ... })`` reads an XCom value, or ``null`` when it does not exist. ``dagId``, ``runId``,
  ``taskId`` and ``mapIndex`` default to the current task; pass ``taskId`` to read an upstream task's XCom.
  See :ref:`typescript-sdk/types` for how values map to JavaScript types.
* ``setXCom({ key, value, ... })`` publishes an XCom value.
* ``taskStateStore`` is this task instance's key/value store, with ``get<T>(key)``,
  ``set(key, value, { retentionMs? })``, ``delete(key)`` and ``clear()``.

.. note::

   A value from a secrets backend, such as an ``AIRFLOW_VAR_*`` environment variable, still wins over the
   stored value when the Variable is read back. ``setVariable`` without a description clears the existing
   description, and ``deleteVariable`` succeeds even when the Variable does not exist.

Task state store
----------------

The task state store is a per-task-instance key/value store, scoped to ``dagId``, ``runId``, ``taskId``,
and ``mapIndex`` (but not ``tryNumber``), so a value written by one attempt is still readable by the next.
It survives worker crashes and task retries within the same Dag run, which makes it a good place to record
external job IDs, checkpoints within a task, and progress metadata. See :doc:`/core-concepts/task-state-store`
for the full concept, including the equivalent Python API.

.. code-block:: typescript

    import { getClient, NEVER_EXPIRE } from "apache-airflow-ts-sdk";

    export async function runSparkJob() {
      const store = getClient().taskStateStore;

      let jobId = await store.get<string>("job_id");
      if (jobId == null) {
        jobId = await submitSparkJob();
        await store.set("job_id", jobId, { retentionMs: NEVER_EXPIRE });
      }

      const result = await waitForSparkJob(jobId);
      await store.delete("job_id");
      return result;
    }

**Retention.** ``retentionMs`` is milliseconds to retain the key, counted from the time of the write:

* A number retains the key for that many milliseconds from now; ``retentionMs: 0`` expires the key
  immediately.
* ``NEVER_EXPIRE`` (imported from ``apache-airflow-ts-sdk``) stores the key with no expiry, regardless of
  the deployment default.
* When ``retentionMs`` is omitted the runtime reads ``AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS``, which
  the coordinator sets from ``[state_store] default_retention_days``. A value of ``0`` there means the key
  never expires. This is not the same as ``retentionMs: 0`` above, which expires the key immediately.

**Keys** must be non-empty strings of at most 512 characters; they may contain slashes. **Values** must be
JSON-compatible (see :ref:`typescript-sdk/types`), and can never be ``null`` — ``set`` rejects a ``null``
value. ``get`` returns ``null`` only to mean "key not found."

Two behaviors to note:

* ``[workers] state_store_backend`` is not used from TypeScript tasks, so values are always stored inline
  through the Execution API. A key written by a Python task through a custom worker-side backend reads back
  from TypeScript as that backend's reference marker string, not the original value.
* ``[state_store] clear_on_success`` removes all of a task instance's state store keys when the task
  succeeds, the same as for Python tasks.

Logging
-------

Anything a task writes to stdout or stderr, such as ``console.log`` or ``console.error``, appears in the
Airflow task log: stdout at ``INFO`` level, stderr at ``ERROR``. There is no dedicated structured-logging API
yet.

.. _typescript-sdk/types:

XCom type mapping
-----------------

XComs are stored as JSON, so a value read with ``getXCom`` arrives as the matching JavaScript type:

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Python type
     - JSON
     - JavaScript type
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

  JavaScript has a single ``number`` type, so integers and decimals arrive as the same type, and an integer
  larger than ``Number.MAX_SAFE_INTEGER`` (2\ :sup:`53` − 1) may lose precision.

.. _typescript-sdk/build:

Building a bundle
-----------------

``airflow-ts-pack``, shipped with the SDK, builds your entry module and everything it imports into a single
file, ``bundle.min.mjs``. That file is all you deploy: it needs no ``node_modules`` and no separate metadata
file, and Airflow refuses to run a bundle whose content was changed after it was built.

.. code-block:: bash

    npx airflow-ts-pack src/main.ts --outdir dist

* ``--outdir <dir>`` sets the output directory (default ``dist``).
* ``--outfile <path>`` names the output file exactly, which helps when one directory holds several bundles.
  The name must end in ``.min.mjs``.

``--outdir`` and ``--outfile`` cannot be used together. ``esbuild`` must be installed before you build; the
runtime install of ``apache-airflow-ts-sdk`` does not need it.

The bundle's code is minified, with license banners of bundled dependencies kept. The Airflow UI's **Code**
view shows the file each Dag is declared in, as you wrote it.

Deploying
---------

Where the bundle goes depends on what it contains.

Dag definition
~~~~~~~~~~~~~~

Put the bundle in a Dag bundle, such as the default ``dags-folder`` bundle. The Dag processor runs each
``*.min.mjs`` bundle it finds with ``node`` to read its Dags, as it reads any other Dag file, so the Dag processor
needs Node.js and the same ``[sdk]`` configuration as the workers:

.. code-block:: ini

    [sdk]
    coordinators = {"ts": {"classpath": "airflow.sdk.coordinators.node.NodeCoordinator"}}
    queue_to_coordinator = {"typescript": "ts"}

With more than one ``NodeCoordinator``, map each Dag bundle that holds ``*.min.mjs`` bundles to one of them in
``[sdk] dag_bundle_to_coordinator``, such as ``{"dags-folder": "ts"}``, or its bundles fail to parse.

Their tasks run from the same Dag bundle, at the version their Dag run was created with, so a Dag declared in
TypeScript needs no coordinator option to find its bundle.

A bundle that fails its integrity check, or whose Dags cannot be read or have a cycle, shows up as an import
error, and the other Dags keep working. Do not declare the same ``dag_id`` in a Python file of that Dag bundle
too: the two overwrite each other on every parse.

Python Dag with stub TaskHandler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the coordinator loads the bundle that implements a stub task from the stub task's own Dag bundle,
at the version its Dag run was created with, so the bundle goes next to the Python Dag that declares the stub.

To keep such bundles in a Dag bundle of their own, name it with ``task_handler_bundle_name``. A task then uses
the version that bundle is on when the task starts:

.. code-block:: ini

    [sdk]
    coordinators = {
      "ts": {
        "classpath": "airflow.sdk.coordinators.node.NodeCoordinator",
        "kwargs": {"task_handler_bundle_name": "ts-handlers"}
      }
    }
    queue_to_coordinator = {"typescript": "ts"}

File names do not matter beyond the ``.min.mjs`` suffix, so one Dag bundle can hold several bundles.

The Dag processor also reads a bundle that only implements stub tasks, which starts ``node`` once per parse
and finds no Dags. List such bundles in ``.airflowignore`` to skip that; the coordinator still finds them.

Where the configuration is needed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The coordinator runs where tasks run, so the ``[sdk]`` configuration and Node.js are needed on the workers.
With ``CeleryExecutor``, set them on the Celery workers. With ``LocalExecutor``, tasks run in the scheduler's
process, so set them there. The Dag processor needs them to read Dags declared in TypeScript. If it has the
``[sdk]`` configuration, it needs Node.js even without such Dags, since it runs every ``*.min.mjs`` bundle it
finds. The API server never needs them.

There is no separate Node.js service to run: the worker starts the bundle with ``node`` once per task
instance.

.. _typescript-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.node.NodeCoordinator` configuration
---------------------------------------------------------------------

The ``kwargs`` of a ``coordinators`` entry are passed to
:class:`~airflow.sdk.coordinators.node.NodeCoordinator`, and all of them are optional:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``task_handler_bundle_name``
     - *(unset)*
     - The Dag bundle to load the bundles that implement stub tasks from. When unset, a stub task loads its
       bundle from its own Dag bundle. It must be registered in ``[dag_processor] dag_bundle_config_list``.
   * - ``node_executable``
     - ``"node"``
     - Path to the ``node`` binary.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for a task's bundle to start. Increase it if your bundle starts slowly, for example on
       constrained hardware.

``task_handler_bundle_name`` only concerns stub tasks: a Dag declared in TypeScript always runs from
the Dag bundle it was read from.

``queue_to_coordinator`` maps a queue to a coordinator entry, so every task on that queue runs on Node.js.

.. _typescript-sdk/limitations:

Limitations
-----------

* **A Dag declared in TypeScript cannot express everything yet.** Assets, custom timetables, dynamic task
  mapping and setup and teardown tasks have no TypeScript form. Declare such a Dag in Python, and implement
  its tasks in TypeScript with :func:`@task.stub <airflow.sdk.task.stub>`.
* **Cluster policies do not apply to Dags declared in TypeScript.** ``dag_policy`` and ``task_policy`` do not
  run when the Dag processor reads them.
* **Some CLI commands do not take a Dag declared in TypeScript.** ``airflow dags test``, ``tasks test`` and
  ``tasks render`` refuse it, and ``airflow dags reserialize`` skips it.
* **Beta.** The API may change in incompatible ways between releases.
* **One Node.js process per task instance.** Tasks cannot share in-process state; use XComs or an external
  store.
