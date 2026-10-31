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

.. _go-sdk:

Go SDK
======

The Go SDK lets you write Airflow Dags and tasks in Go. There are two ways to use it:

* **Dag definition.** The schedule, the tasks, their options and the dependencies between them are all written
  in Go, with no Python file involved. See :ref:`go-sdk/dag-definition`.
* **Python Dag with stub TaskHandler.** A Python Dag declares the tasks with ``@task.stub`` and wires them
  together, and a Go ``TaskHandler`` implements each one. See :ref:`go-sdk/stub-tasks`.

Both use the same task API and the same build tool, and one bundle can serve both.

Go is compiled, so your Dags and tasks are built into a single self-contained executable, the **bundle**, with
the :ref:`airflow-go-pack <go-sdk/build>` tool. The bundle is the only file you deploy.

The SDK is the ``github.com/apache/airflow/go-sdk`` Go module.

.. seealso::

  For the full API reference and the released versions, see
  `pkg.go.dev <https://pkg.go.dev/github.com/apache/airflow/go-sdk>`__.

.. contents:: Contents
   :local:
   :depth: 2

Prerequisites
-------------

* Go 1.25 or later to build bundles. Airflow needs no Go toolchain to run them.
* A bundle runs on the Airflow workers and on the Dag processor, so build it for their operating system and CPU
  architecture (see :ref:`go-sdk/cross-platform`).
* The ``apache-airflow-task-sdk`` package, installed with Airflow, provides
  :class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`, which runs the bundle. No other Python
  package is needed.
* In your Go module, add the SDK, and ``airflow-go-pack`` as a tool:

  .. code-block:: bash

      go get github.com/apache/airflow/go-sdk/airflow
      go get -tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack

.. _go-sdk/quick-start:

Quick start
-----------

This example declares a whole Dag in Go, builds it into a bundle, and deploys it, with no Python file involved.

Declare a Dag in ``main.go``:

.. code-block:: go

    package main

    import (
        "log"

        "github.com/apache/airflow/go-sdk/airflow"
    )

    func extract(airflow.Context) (int, error) {
        return 42, nil
    }

    func report(actx airflow.Context, rows int) error {
        actx.Logger().InfoContext(actx, "extracted rows", "rows", rows)
        return nil
    }

    func main() {
        dag := airflow.Dag("go_hello", airflow.DagSpec{Schedule: "@daily", Queue: "golang"})

        extracted := dag.Task(extract)
        dag.Task(report, airflow.Inputs(extracted))

        bundle := airflow.Bundle()
        bundle.Register(dag)
        if err := bundle.Serve(); err != nil {
            log.Fatal(err)
        }
    }

Build it into a bundle:

.. code-block:: bash

    go tool airflow-go-pack --output dist/go_hello .

Put ``dist/go_hello`` in a Dag bundle, for example the default ``dags-folder`` bundle, which reads the
``[core] dags_folder`` directory (see :doc:`Dag bundles </administration-and-deployment/dag-bundles>`). Then send
the ``golang`` queue to the Go coordinator in ``airflow.cfg``:

.. code-block:: ini

    [sdk]
    coordinators = {"go": {"classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator"}}
    queue_to_coordinator = {"golang": "go"}

Airflow reads the bundle like any other Dag file: ``go_hello`` shows up in the UI, runs on its schedule, and
``report`` receives the value ``extract`` returned.

.. _go-sdk/dag-definition:

Declaring a Dag in Go
---------------------

Tasks and their inputs
~~~~~~~~~~~~~~~~~~~~~~

``airflow.Dag(dagID, spec)`` creates a Dag, and ``dag.Task(fn, opts...)`` adds a task to it and returns a
reference to the task. ``airflow.Inputs`` gives each parameter after the ``airflow.Context`` one input:

* A task reference passes that task's return value, and makes this task wait for it.
* ``airflow.Literal(value)`` passes a fixed JSON value, and draws no dependency.

.. code-block:: go

    type Summary struct {
        Total   int `json:"total"`
        Regions int `json:"regions"`
    }

    func summarize(airflow.Context) (Summary, error) {
        return Summary{Total: 42, Regions: 2}, nil
    }

    func publish(actx airflow.Context, summary Summary, label string) error {
        actx.Logger().InfoContext(actx, label, "total", summary.Total)
        return nil
    }

    summarized := dag.Task(summarize)
    dag.Task(publish, airflow.Inputs(summarized, airflow.Literal("nightly")))

``publish`` receives the ``Summary`` that ``summarize`` returned, and the literal ``"nightly"`` as ``label``.

A task function takes an ``airflow.Context`` first, and returns ``error`` or ``(T, error)``. The ``T`` becomes the
task's ``return_value`` XCom and what a downstream task receives. Values move between tasks as JSON (see
:ref:`go-sdk/types`), so return types JSON can hold.

The inputs are checked when the Dag is built. Each parameter needs exactly one input, an upstream task's
result type must be assignable to the parameter type, and a literal must decode into it. A mismatch panics,
which the Dag processor reports as an import error.

Task ids
~~~~~~~~

The task id is the function's name, so ``dag.Task(summarize)`` adds the task ``summarize``. A method value such
as ``svc.Extract`` takes the method's name, ``Extract``. A function literal has no usable name, so give it one
with ``airflow.TaskSpec{TaskID: "..."}``.

A task id is made of letters, digits, dashes, underscores and dots, and is at most 250 characters with the
prefixes of its task groups. Two tasks of one Dag cannot share an id.

Dag and task options
~~~~~~~~~~~~~~~~~~~~

``airflow.DagSpec`` and ``airflow.TaskSpec`` hold Airflow's own Dag and task arguments, spelled in Go:

.. code-block:: go

    dag := airflow.Dag("go_etl", airflow.DagSpec{
        Schedule: "0 6 * * *",
        Tags:     []string{"etl"},
        Queue:    "golang",
    })

    loaded := dag.Task(loadRows, airflow.Inputs(summarized), airflow.TaskSpec{Retries: 2})

``Schedule`` accepts ``@once``, ``@continuous``, a cron preset such as ``@daily``, or a cron expression. Leave it
empty for a Dag that only runs when triggered. A cron schedule runs in UTC.

Every task of the Dag runs in the Go bundle, so it needs a queue that
:ref:`queue_to_coordinator <go-sdk/coordinator-config>` sends to the Go coordinator. Set ``Queue`` once on the
``DagSpec`` and each task inherits it. A task's own ``Queue`` wins over the Dag's.

Order-only dependencies
~~~~~~~~~~~~~~~~~~~~~~~

When a task only has to run after another, without receiving its value, draw the dependency with ``Before`` and
``After``, the Go spelling of Python's ``>>`` and ``<<``:

.. code-block:: go

    cleaned := dag.Task(cleanup)

    loaded.Before(cleaned)                           // loaded >> cleaned
    cleaned.After(loaded, summarized)                // [loaded, summarized] >> cleaned
    loaded.Before(airflow.Label("done").On(cleaned)) // a labelled edge

Both take any number of tasks or task groups, and drawing a dependency that already exists changes nothing.
``airflow.Label`` takes the text, and ``On`` names the tasks the label's edges reach, which is Python's
``loaded >> Label("done") >> cleaned``. The Airflow UI shows the label on the graph.

Pass a value through ``airflow.Inputs`` when the downstream task needs it, and use ``Before`` or ``After`` when it
only needs to run in order.

Task groups
~~~~~~~~~~~

``dag.TaskGroup(groupID)`` opens a group with the same ``Task``, ``TaskGroup``, ``If`` and ``Switch`` methods as
the Dag. Every id declared in it is prefixed with the group's, as in Python:

.. code-block:: go

    staging := dag.TaskGroup("staging")
    staging.Task(stageRows)                    // task id "staging.stageRows"
    staging.TaskGroup("checks").Task(checkNulls) // task id "staging.checks.checkNulls"

    staging.Before(loaded)                     // staging >> loaded

A group takes ``Before`` and ``After`` too, which orders the whole group against a task or another group.

Tasks and groups share one id namespace, so a Dag cannot hold both a task and a group called ``staging``. Set
``PrefixGroupID`` in an ``airflow.TaskGroupSpec`` to a pointer to ``false`` to keep the ids declared in a group
as written. A group id is made of letters, digits, dashes and underscores, and is at most 200 characters.

Conditional branching
~~~~~~~~~~~~~~~~~~~~~

``dag.If`` declares a task from a function that returns ``(bool, error)``, and names the task each outcome runs:

.. code-block:: go

    func hasRows(_ airflow.Context, summary Summary) (bool, error) {
        return summary.Total > 0, nil
    }

    dag.If(hasRows, airflow.Inputs(summarized)).Then(loaded).Else(reportedEmpty)

``If`` takes inputs and task options as ``Task`` does. ``Then`` is required, and ``Else`` is optional: without it,
the ``Then`` task is skipped when the condition is false.

The condition skips only the side not taken, which stays skipped if it is cleared later. Tasks after it follow their
own trigger rule, so with the default ``all_success``, a task after both sides is skipped too. To run such a task when
at least one side succeeds and none fails, give it ``airflow.TriggerRuleNoneFailedMinOneSuccess``:

.. code-block:: go

    dag.Task(finish, airflow.TaskSpec{
        TriggerRule: airflow.TriggerRuleNoneFailedMinOneSuccess,
    }).After(loaded, reportedEmpty)

Multi-way branching
~~~~~~~~~~~~~~~~~~~

``dag.Switch`` declares a task from a function that returns the reference of the task to run, and lists the
candidates with ``Case``:

.. code-block:: go

    daily := dag.Task(publishDaily)
    weekly := dag.Task(publishWeekly)

    pickCadence := func(actx airflow.Context) (*airflow.TaskRef, error) {
        cadence, err := actx.Client().GetVariable(actx, "cadence")
        if err != nil {
            return nil, err
        }
        if cadence == "weekly" {
            return weekly, nil
        }
        return daily, nil
    }

    dag.Switch(pickCadence, airflow.TaskSpec{TaskID: "pick_cadence"}).Case(daily).Case(weekly)

Exactly one case runs, and the others are skipped. There is no default: a decider that returns anything other
than a case fails, naming what it returned and the cases it could have returned. The deciding task's value is
the id of the task it chose.

Triggering another Dag
~~~~~~~~~~~~~~~~~~~~~~

``airflow.TriggerDagRun`` makes a task that starts a run of another Dag. Pass it to ``dag.Task`` in place of a
function:

.. code-block:: go

    triggered := dag.Task(
        airflow.TriggerDagRun(airflow.TriggerDagRunSpec{
            DagID:             "downstream_etl",
            Conf:              map[string]any{"source": "go_etl"},
            WaitForCompletion: true,
        }),
        airflow.TaskSpec{TaskID: "trigger_downstream"},
    )
    triggered.After(loaded)

The task has no function to take an id from, so it needs a ``TaskID``, and it takes no inputs.
``TriggerDagRunSpec`` takes the options of Python's ``TriggerDagRunOperator``, such as ``Conf``,
``WaitForCompletion``, ``Deferrable``, ``PokeInterval``, ``AllowedStates``, ``FailedStates`` and
``SkipWhenAlreadyExists``. Values are sent as written, so Jinja in ``Conf`` is not rendered.

When ``Deferrable`` is unset, it follows ``[operators] default_deferrable``. A deferred task waits in the
triggerer, which needs the standard provider installed. Without it, ``WaitForCompletion`` polls from the task
and holds its worker slot.

Generating Dags from data
~~~~~~~~~~~~~~~~~~~~~~~~~

A Dag is an ordinary Go value, so ``main`` can build any number of them, for example from a loop over a
configuration file. ``go-sdk/example/dynamic`` builds one Dag per entry of a ``dags.yaml`` file deployed next to
the bundle, so changing the file changes the Dags with no rebuild.

Registering Dags
~~~~~~~~~~~~~~~~

``bundle.Register`` takes the Dags, and ``Serve`` runs the bundle, so call it last in ``main``. ``Register``
checks each Dag as a whole: a cycle, an ``If`` with no ``Then``, a ``Switch`` with no ``Case``, or two Dags with
the same ``dag_id`` panic, and the Dag processor reports an import error.

A complete example
~~~~~~~~~~~~~~~~~~

``go-sdk/example/native`` uses all of the above: a task group, a fan-in, a condition, a multi-way branch,
order-only dependencies and a triggered Dag run.

.. _go-sdk/stub-tasks:

Implementing stub tasks of a Python Dag
---------------------------------------

Here a Python Dag declares the tasks and their dependencies, and Go implements some of them. Use it when the Dag
needs something a Go Dag cannot express yet (see :ref:`go-sdk/limitations`), or to mix Python and Go tasks in
one Dag.

The Python Dag
~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def simple_dag():
        @task.stub(queue="golang")
        def extract(): ...

        @task.stub(queue="golang")
        def transform(country: str, extracted: dict): ...

        transform("uk", extract())


    simple_dag()

``@task.stub`` declares a task that is implemented elsewhere, and ``queue`` sends it to the Go coordinator.

The Go side
~~~~~~~~~~~

Create a ``TaskHandler`` per stub task, naming the ``dag_id`` and ``task_id`` it implements, and register them on
the bundle:

.. code-block:: go

    func extract(actx airflow.Context) (any, error) {
        return map[string]any{"rows": 42}, nil
    }

    func transform(actx airflow.Context, country string, extracted map[string]any) error {
        actx.Logger().InfoContext(actx, "transforming", "country", country, "rows", extracted["rows"])
        return nil
    }

    func main() {
        bundle := airflow.Bundle()
        bundle.Register(
            airflow.TaskHandler("simple_dag", "extract", extract),
            airflow.TaskHandler("simple_dag", "transform", transform),
        )
        if err := bundle.Serve(); err != nil {
            log.Fatal(err)
        }
    }

The ``task_id`` must match the stub's task id, including any task group prefix. Neither id comes from the Go
function's name, so a handler can be named whatever reads best in Go.

``TaskHandler`` checks the function's signature and panics on a bad one, so a mistake stops the bundle as soon
as it starts rather than when the task first runs.

``Register`` takes any number of task handlers and Dags, so one bundle can implement the tasks of several Python
Dags and declare Dags of its own. A package can export its handlers as a ``[]airflow.Registerable`` for
``main`` to pass on with ``bundle.Register(reports.Handlers()...)``.

.. _go-sdk/arguments:

TaskFlow arguments
~~~~~~~~~~~~~~~~~~

A Python Dag that calls a stub task TaskFlow-style passes those arguments to the handler. They bind in one of two
ways.

**By position.** Each parameter after the ``airflow.Context`` takes the argument in the same position, as
``transform("uk", extract())`` fills ``country`` and ``extracted`` above.

**By name.** When the handler's only parameter after the Context is a struct, its fields take the arguments by
name:

.. code-block:: go

    type CombineInput struct {
        Region    string `arg:"region_code"`
        Threshold float64
    }

    // The Python Dag calls combine(region_code="uk", threshold=0.5).
    func combine(actx airflow.Context, input CombineInput) (any, error) {
        return nil, nil
    }

A field takes the argument whose name matches its own, ignoring case and underscores, so ``Threshold`` takes
``threshold``. An ``arg:"<name>"`` tag maps a field to a different name. A field the call does not pass keeps its
zero value, and an argument no field takes is ignored. Both are logged as a warning.

Either way, an argument filled from another task, as ``extract()`` above, arrives as that task's value. An
argument the call leaves at its default needs no Go parameter, so adding a defaulted parameter to a stub does
not break the handler. An argument that cannot fill its Go type fails the task before the handler runs.

.. note::

  A dependency drawn with ``>>`` in the Python Dag orders the tasks and passes nothing. To read a value the
  call did not pass, use ``actx.Client().GetXCom``.

Writing tasks
-------------

A task is an ordinary Go function. It works the same in both ways of using the SDK.

Its first parameter is an ``airflow.Context``, which gives it what Airflow provides:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Method
     - Returns
   * - ``actx.Logger()``
     - An ``*slog.Logger`` that writes to the Airflow task log. See :ref:`go-sdk/logging`.
   * - ``actx.Client()``
     - An ``sdk.Client`` for Airflow Variables, Connections, XComs and the task state store. See
       :ref:`go-sdk/client`.
   * - ``actx.TaskInstance()``
     - ``DagID``, ``RunID``, ``TaskID`` (including any task group prefix), ``MapIndex`` (``nil`` for an unmapped
       task) and ``TryNumber``.
   * - ``actx.DagRun()``
     - ``DagID``, ``RunID``, the ``*time.Time`` fields ``LogicalDate``, ``DataIntervalStart`` and
       ``DataIntervalEnd`` (``nil`` when the run has no such value, such as a manual run), and ``Conf``. See
       :ref:`go-sdk/dag-run-conf`.

``airflow.Context`` is a ``context.Context``, so pass it to a client call or to ``http.NewRequestWithContext``. Its
``Done()`` fires when Airflow stops the task, so long-running work can stop cleanly. Cleanup that must outlive
that runs under ``context.WithoutCancel(actx)``. A helper that takes a plain ``context.Context`` gets the
``airflow.Context`` back with ``airflow.FromContext``.

A value the task returns becomes its ``return_value`` XCom. A non-nil ``error``, or a panic, fails the task, and
retries it if the task has retries.

``airflow.NewContext`` builds a Context, so a unit test calls a task as an ordinary function:

.. code-block:: go

    actx := airflow.NewContext(
        t.Context(), slog.Default(), fakeClient,
        airflow.TaskInstance{DagID: "simple_dag", TaskID: "transform", TryNumber: 1},
        airflow.DagRun{DagID: "simple_dag", RunID: "run1"},
    )
    require.NoError(t, transform(actx, "uk", map[string]any{"rows": 42}))

.. _go-sdk/dag-run-conf:

The Dag run conf
~~~~~~~~~~~~~~~~

``actx.DagRun().Conf`` is the conf the run was triggered with, a ``map[string]any`` whose values have the Go
types listed in :ref:`the XCom type mapping <go-sdk/types>`. It is ``nil`` for a run triggered without a conf.

``UnmarshalConf`` reads the conf into a struct with typed fields, so a task does not assert a Go type per key:

.. code-block:: go

    type etlConf struct {
        TargetTable string
        Limit       int
        Since       time.Time
    }

    func load(actx airflow.Context) error {
        etl := etlConf{Limit: 100}
        if err := actx.DagRun().UnmarshalConf(&etl); err != nil {
            return err
        }
        ...
    }

For a run triggered with the conf ``{"target_table": "sales", "since": "2024-01-31T00:00:00Z"}``,
``etl.TargetTable`` is ``"sales"``, ``etl.Since`` is that date, and ``etl.Limit`` stays ``100``.

* Each key goes to the field whose name matches it when case and underscores are ignored, so ``target_table``
  reaches ``TargetTable``. A ``json:`` tag renames the field, and the tag name matches the same way. Objects
  nested in the conf follow the same rule, including objects in slices and objects that are map values. The
  keys of a map field stay as written, because they are data rather than field names.
* The ``arg:`` tag that binds a TaskFlow argument has no effect here. Read a key that no field name can match,
  such as ``target-table``, from ``Conf``, or name it in a ``json:`` tag.
* Keys that match no field are ignored, because other tasks in the run may read them. A field with no matching
  key keeps the value the struct already holds, so set the defaults before the call, as ``Limit`` does above.
* The conf holds only the values the run was triggered with. In a Python task, ``params`` combines the conf
  with the defaults of the Dag's params. ``UnmarshalConf`` does not add those defaults, so in a scheduled run
  every field keeps the value it came in with.
* It returns an error when a value does not fit the type of its field, when two keys such as ``target_table``
  and ``TARGET_TABLE`` match one field and neither names it exactly, or when two fields of the struct differ
  only in case or underscores.

Reading ``Conf`` directly gives the value of a key as an ``any``. An integer then arrives as a Go integer type
whose width depends on the value, so assert the type of a string or a bool, and use ``UnmarshalConf`` for a
number or for an object nested in the conf.

.. _go-sdk/client:

The ``sdk.Client``
~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Method
     - Description
   * - ``GetVariable(ctx, key)``
     - The Variable as a string.
   * - ``UnmarshalJSONVariable(ctx, key, pointer)``
     - Decodes a JSON Variable into the pointer.
   * - ``SetVariable(ctx, key, value, description)``
     - Stores a Variable, replacing any existing value. Values are strings, so encode structured data, for
       example with ``json.Marshal``.
   * - ``DeleteVariable(ctx, key)``
     - Removes a Variable.
   * - ``GetConnection(ctx, connID)``
     - A ``Connection`` with ``ID``, ``Type``, ``Host``, ``Port``, ``Login``, ``Password``, ``Path`` and ``Extra``,
       and a ``GetURI()`` helper.
   * - ``GetXCom(ctx, dagID, runID, taskID, mapIndex, key, nil)``
     - An XCom value, as an ``any``. See :ref:`go-sdk/types`.
   * - ``PushXCom(ctx, ti, key, value)``
     - Publishes an XCom value.
   * - ``TaskStateStore()``
     - This task instance's key/value store. See :ref:`go-sdk/task-state-store`.

A missing value returns an error that wraps ``sdk.VariableNotFound``, ``sdk.ConnectionNotFound``,
``sdk.XComNotFound`` or ``sdk.TaskStateNotFound``, so test for it with ``errors.Is``.

``sdk.Client`` is made of ``VariableClient``, ``ConnectionClient``, ``XComClient`` and ``TaskStateStoreClient``,
so a helper can take only the one it needs, and a test can pass a fake.

.. note::

   A value from a secrets backend, such as an ``AIRFLOW_VAR_*`` environment variable, still wins over the
   stored value when the Variable is read back. ``SetVariable`` with an empty description clears the existing
   description.

.. _go-sdk/task-state-store:

Task state store
~~~~~~~~~~~~~~~~

The task state store is a per-task-instance key/value store, scoped to the Dag, run, task and map index, but not
the try number, so a value written by one attempt is still readable by the next. It survives worker crashes and
retries, which makes it a good place to record an external job id or a checkpoint. See
:doc:`/core-concepts/task-state-store` for the concept.

.. code-block:: go

    func runSparkJob(actx airflow.Context) error {
        store := actx.Client().TaskStateStore()

        var jobID string
        stored, err := store.Get(actx, "job_id")
        switch {
        case errors.Is(err, sdk.TaskStateNotFound):
            if jobID, err = submitSparkJob(actx); err != nil {
                return err
            }
            if err := store.Set(actx, "job_id", jobID, sdk.WithRetention(sdk.NeverExpire)); err != nil {
                return err
            }
        case err != nil:
            return err
        default:
            jobID = stored.(string)
        }

        return waitForSparkJob(actx, jobID)
    }

The store has ``Get``, ``UnmarshalJSONValue`` (decodes an object or array into a pointer), ``Set``, ``Delete``
and ``Clear``.

* A value must be JSON, and not ``nil``. A ``time.Time``, a non-finite float or a ``[]byte`` is refused before it
  is sent, so store a time as ``value.Format(time.RFC3339)``.
* ``Set`` without options keeps the key for ``[state_store] default_retention_days``.
  ``sdk.WithRetention(d)`` keeps it for a positive ``time.Duration``, and ``sdk.NeverExpire`` keeps it forever.
* ``[workers] state_store_backend`` is not used from Go tasks, so values are always stored in the database. A
  key a Python task wrote through such a backend reads back as the backend's reference marker.

.. _go-sdk/logging:

Logging
~~~~~~~

``actx.Logger()`` writes to the Airflow task log, at the level ``[logging] logging_level`` sets.
``[logging] namespace_levels`` applies to a logger named with ``WithGroup``:

.. code-block:: go

    databaseLog := actx.Logger().WithGroup("example.database")
    databaseLog.Debug("query complete", "rows", 42)

A namespace level applies to the longest matching dotted prefix, so ``example=DEBUG`` also applies to
``example.database``.

.. _go-sdk/types:

XCom type mapping
-----------------

XComs are stored as JSON, so a value read with ``GetXCom`` arrives as the matching Go type:

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Python type
     - JSON
     - Go type
   * - ``int``
     - number (integer)
     - an integer type (see note)
   * - ``float``
     - number (decimal)
     - ``float64``
   * - ``str``
     - string
     - ``string``
   * - ``bool``
     - boolean
     - ``bool``
   * - ``None``
     - null
     - ``nil``
   * - ``list``
     - array
     - ``[]any``
   * - ``dict``
     - object
     - ``map[string]any``

.. note::

  An integer arrives as a Go integer type whose width depends on the value. Type-switch over the integer types
  you expect, or round-trip the value through ``json.Marshal`` and ``json.Unmarshal`` into a typed Go value. A
  task input from ``airflow.Inputs`` or a TaskFlow argument is already decoded into its parameter type.

.. _go-sdk/build:

Building a bundle
-----------------

``airflow-go-pack`` builds your ``main`` package into a single executable, the bundle. It also embeds the
entrypoint file and the file that declares each Dag, which the Airflow UI's **Code** view shows, and a manifest
of the bundle's Dags and task handlers. That file is all you deploy, and Airflow refuses to run a bundle whose
content was changed after it was built.

.. code-block:: bash

    go tool airflow-go-pack --output dist/my_dags ./cmd/my_dags -- -trimpath

* ``--output <path>`` sets the output file. By default, the bundle is written to the current directory, named
  after the package's directory.
* Flags after ``--`` are passed to ``go build``.
* ``go tool airflow-go-pack inspect <bundle>`` prints a bundle's manifest.

The Dag processor reads a bundle only when its file name has no extension, such as ``dags/orders``, not
``dags/orders.bin``.

.. _go-sdk/cross-platform:

Cross-platform builds
~~~~~~~~~~~~~~~~~~~~~

The workers and the Dag processor often run a different operating system or CPU architecture than your build
machine. Pass ``--goos`` and ``--goarch`` to build for them:

.. code-block:: bash

    go tool airflow-go-pack --goos linux --goarch amd64 --output dist/my_dags ./cmd/my_dags

``airflow-go-pack`` runs the built binary to read its manifest. When it cannot do that for a cross-compiled
binary, build and read the manifest on a machine of the target platform, then pack the binary with
``--executable``:

.. code-block:: bash

    # On a linux/amd64 machine:
    go build -o my_dags ./cmd/my_dags
    ./my_dags --airflow-metadata > airflow-metadata.yaml

    # On the build machine:
    go tool airflow-go-pack --executable ./my_dags --source cmd/my_dags/main.go \
      --airflow-metadata airflow-metadata.yaml --output dist/my_dags

Deploying
---------

Where the bundle goes depends on what it contains.

Dag definition
~~~~~~~~~~~~~~

Put the bundle in a Dag bundle, such as the default ``dags-folder`` bundle. The Dag processor runs each bundle it
finds to read its Dags, as it reads any other Dag file, so it needs the same ``[sdk]`` configuration as the workers,
shown in the :ref:`quick start <go-sdk/quick-start>`, and a bundle built for its platform.

With more than one ``ExecutableCoordinator``, map each Dag bundle that holds Go Dags to one of them in
``[sdk] dag_bundle_to_coordinator``, such as ``{"dags-folder": "go"}``, or its bundles fail to parse.

Their tasks run from the same Dag bundle, at the version their Dag run was created with, so a Dag declared in Go
needs no coordinator option to find its bundle.

A bundle that cannot be read, or whose Dags fail a check, shows up as an import error, and the other Dags keep
working.

Python Dag with stub TaskHandler
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the coordinator loads the bundle that implements a stub task from the stub task's own Dag bundle, at
the version its Dag run was created with, so the bundle goes next to the Python Dag that declares the stub.

To keep such bundles in a Dag bundle of their own, name it with ``task_handler_bundle_name``. A task then uses the
version that bundle is on when the task starts:

.. code-block:: ini

    [dag_processor]
    dag_bundle_config_list = [
        {"name": "dags-folder", "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle", "kwargs": {}},
        {
          "name": "go-task-handlers",
          "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
          "kwargs": {"path": "/opt/airflow/go-task-handlers"}
        }
      ]

    [sdk]
    coordinators = {
      "go": {
        "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
        "kwargs": {"task_handler_bundle_name": "go-task-handlers"}
      }
    }
    queue_to_coordinator = {"golang": "go"}

The coordinator finds the bundle for a task by the ``dag_id`` in each bundle's manifest, not by file name, so one
Dag bundle can hold several bundles. Any Dag bundle type works, including an object-store one such as
``S3DagBundle``.

The Dag processor checks the stub tasks of each Python Dag against the task handlers the bundles register, so
it needs the bundles too, built for its platform.

The Dag processor also parses a bundle that only implements stub tasks, which runs it once per parse and finds
no Dags. Skip that with the Dag bundle's ``.airflowignore``; the coordinator still finds the bundles. For a Dag
bundle that holds only such bundles, as ``go-task-handlers`` above:

.. code-block:: bash

    # [core] dag_ignore_file_syntax = glob (the default)
    echo '*' > /opt/airflow/go-task-handlers/.airflowignore

    # [core] dag_ignore_file_syntax = regexp
    echo '.' > /opt/airflow/go-task-handlers/.airflowignore

For bundles next to a Python Dag, put them in a folder such as ``bin/`` and ignore it with ``bin/*`` (glob) or
``^bin/`` (regexp).

Where the configuration is needed
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The coordinator runs where tasks run, so the ``[sdk]`` configuration and the bundles are needed on the workers.
With ``CeleryExecutor``, set them on the Celery workers. With ``LocalExecutor``, tasks run in subprocesses on the
scheduler's host, so set them there. The Dag processor needs them too, as described above. Register every Dag bundle in
``[dag_processor] dag_bundle_config_list`` on each of these components. The API server never needs them.

There is no separate Go service to run: the worker starts the bundle once per task instance.

.. _go-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` configuration
---------------------------------------------------------------------------------

The ``kwargs`` of a ``coordinators`` entry are passed to
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator`, and all of them are optional:

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
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for a task's bundle to start. Increase it if your bundle starts slowly, for example on
       constrained hardware.

``task_handler_bundle_name`` only concerns stub tasks: a Dag declared in Go always runs from the Dag bundle it was
read from.

``queue_to_coordinator`` maps a queue to a coordinator entry, so every task on that queue runs in a Go bundle.

.. _go-sdk/limitations:

Limitations
-----------

* **A Dag declared in Go cannot express everything yet.** Assets, Dag params, custom timetables, dynamic task
  mapping, setup and teardown tasks, and human-in-the-loop tasks have no Go form. Declare such a Dag in Python,
  and implement its tasks in Go with :func:`@task.stub <airflow.sdk.task.stub>`.
* **Cluster policies do not apply to Dags declared in Go.** ``dag_policy`` and ``task_policy`` do not run when the
  Dag processor reads them.
* **Some CLI commands do not take a Dag declared in Go.** ``airflow dags test``, ``tasks test`` and
  ``tasks render`` refuse it, and ``airflow dags reserialize`` skips it.
* **One process per task instance.** Tasks cannot share in-process state; use XComs or an external store.
