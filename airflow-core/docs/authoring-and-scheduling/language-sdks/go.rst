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

|experimental|

The Go SDK lets you implement Airflow task logic in Go, with native access to the Airflow "model"
(Variables, Connections, and XCom). The Dag and its scheduling remain in Python; individual tasks delegate
to a compiled Go *bundle* that is launched by
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` for each task instance.

Because Go is a compiled language, every task must be compiled ahead of time and registered inside a single,
self-contained native executable called a **bundle**. The bundle also embeds its Dag source and a metadata
manifest (the ``dag_id`` and ``task_id`` map) in a footer appended to the executable, so the executable *is*
the bundle: one runnable file to ship, with no separate manifest or archive. The
:ref:`airflow-go-pack <go-sdk/build>` tool builds and packs that bundle.

.. contents:: Contents
   :local:
   :depth: 2

API reference
-------------

The generated API reference for the Go SDK module, and the list of its released versions, is available on
`pkg.go.dev <https://pkg.go.dev/github.com/apache/airflow/go-sdk>`__.

Prerequisites
-------------

* Go 1.24 or later to build and pack bundles. This is a build-time requirement only; the worker that runs a
  packed bundle needs no Go toolchain, because the bundle is a self-contained native executable.
* The packed bundle must be accessible from the Airflow worker, under a directory the coordinator scans.
* The ``apache-airflow-task-sdk`` package (installed with Airflow) provides the coordinator; no additional
  Python packages are needed.

Execution architecture
----------------------

A Python task runner launches the Go bundle directly, with no separate Go worker process on the host. This
is the same coordinator mechanism the Java SDK uses. Because the mature Python supervisor handles the
Airflow-facing concerns, Go tasks inherit remote task logs (S3/GCS), the full range of task states, and
alternate XCom backends rather than implementing them again in Go.

Quick start
-----------

The following example shows the minimal moving parts: a Python Dag with two stub tasks, and a Go
implementation of those tasks.

Python Dag (the scheduling side)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from airflow.sdk import dag, task


    @dag
    def simple_dag():
        @task.stub(queue="golang")
        def extract(): ...

        @task.stub(queue="golang")
        def transform(): ...

        extract() >> transform()


    simple_dag()

``@task.stub`` declares the *shape* of the Go tasks (their names and dependencies) without any Python
implementation. The ``queue`` value routes the task to the Go coordinator.

Go implementation
~~~~~~~~~~~~~~~~~~

A task is an ordinary Go function whose first parameter is an ``airflow.Context``. Everything Airflow gives
the task is a method on it, so the signature stays the same whatever the task uses.

.. code-block:: go

    import (
        "runtime"

        "github.com/apache/airflow/go-sdk/airflow"
    )

    func extract(actx airflow.Context) (any, error) {
        conn, err := actx.Client().GetConnection(actx, "test_http")
        if err != nil {
            return nil, err
        }
        actx.Logger().InfoContext(actx, "fetched connection", "host", conn.Host)
        // ... do work, honour actx cancellation ...
        return map[string]any{"go_version": runtime.Version()}, nil
    }

    func transform(actx airflow.Context) error {
        val, err := actx.Client().GetVariable(actx, "my_variable")
        if err != nil {
            return err
        }
        actx.Logger().InfoContext(actx, "obtained variable", "my_variable", val)
        return nil
    }

.. note::

  As with the other language SDKs, XCom *dependencies* are declared in the Python stub Dag (they define task
  order). An upstream task's value reaches a downstream task either through a parameter, when the stub Task
  passes it in the TaskFlow call (see :ref:`go-sdk/arguments`), or by reading it explicitly with
  ``actx.Client().GetXCom``.

Go entry point
~~~~~~~~~~~~~~~

Build a bundle with ``airflow.Bundle()``, register a handler for each task, and call ``Serve`` as the last
statement of ``main``. The ``Register`` calls are the single source of truth for which ``dag_id`` and task
names this bundle can run, so the generated manifest can never drift from what the binary actually executes.

.. code-block:: go

    import (
        "log"

        "github.com/apache/airflow/go-sdk/airflow"
    )

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

``TaskHandler`` names the ``dag_id`` and the ``task_id`` explicitly: the ``dag_id`` must match the Python
Dag, and the ``task_id`` must match a ``@task.stub`` function in that Dag. Neither is derived from the Go
function name, so a handler can be named whatever reads best in Go.

``TaskHandler`` also checks the signature of the function it is given and panics if the check fails -- for
instance when the function does not take an ``airflow.Context`` first, or does not return an ``error``.
Because ``main`` registers every handler before ``Serve``, a mistake stops the executable as soon as it
starts rather than when the task first runs.

A package that defines task handlers of its own can export them as a ``[]airflow.Registerable`` for ``main``
to pass on with ``bundle.Register(reports.Handlers()...)``.

Coordinator configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Register the coordinator and route the queue to it under ``[sdk]`` in ``airflow.cfg`` (or the equivalent
``AIRFLOW__SDK__*`` environment variables):

.. code-block:: ini

    [sdk]
    coordinators = {
      "go": {
        "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
        "kwargs": {"executables_root": ["~/airflow/executable-bundles"]}
      }
    }
    queue_to_coordinator = {"golang": "go"}

``executables_root`` is one or more directories the coordinator scans for bundles; ``queue_to_coordinator``
routes stub tasks with ``queue="golang"`` to this Go coordinator. See :ref:`go-sdk/coordinator-config` for
the full list of accepted ``kwargs``.

There is no separate Go worker to run: the Airflow worker forks the bundle binary once per task instance.

.. note::

  The coordinator is part of the Airflow worker, so the ``[sdk]`` config (and the bundle files in
  ``executables_root``) only need to be present wherever tasks actually execute. With ``CeleryExecutor``,
  setting it on the Celery workers is sufficient. With ``LocalExecutor``, tasks run inside the scheduler
  process, so it must be set where the scheduler can read it. The API server and Dag processor do not need
  it.

Writing tasks
-------------

Every task function takes an ``airflow.Context`` as its first parameter, and reaches what Airflow provides
through its methods:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Method
     - What it returns
   * - ``actx.Logger()``
     - An ``*slog.Logger`` whose output is routed back to the Airflow task log.
   * - ``actx.Client()``
     - A client for Airflow Variables, Connections, and XCom. See :ref:`go-sdk/client`.
   * - ``actx.TaskInstance()``
     - The identifiers of the running task instance. See :ref:`go-sdk/runtime-context`.
   * - ``actx.DagRun()``
     - The identifiers and scheduling timestamps of its Dag run. See :ref:`go-sdk/runtime-context`.

``airflow.Context`` is itself a ``context.Context``, so pass it straight to a client call or to
``http.NewRequestWithContext``, and select on ``actx.Done()``, which fires when the supervisor asks the task
to stop. Respect it for long-running work. Cleanup that must outlive that cancellation runs under ``context.WithoutCancel(actx)``.
A helper typed as a plain ``context.Context`` recovers the same surface with ``airflow.FromContext``.

Every parameter after the Context is data, filled from the stub Task's TaskFlow call; see
:ref:`go-sdk/arguments`.

An optional ``(any, error)`` return value becomes the task's ``return_value`` XCom. A non-nil ``error`` (or a
panic, which the runtime recovers) marks the task instance failed in Airflow, triggering retries if
configured on the stub.

``airflow.NewContext`` builds a Context, so a task is an ordinary function call in a unit test:

.. code-block:: go

    actx := airflow.NewContext(
        t.Context(), slog.Default(), fakeClient,
        airflow.TaskInstance{DagID: "simple_dag", TaskID: "transform", TryNumber: 1},
        airflow.DagRun{DagID: "simple_dag", RunID: "run1"},
    )
    require.NoError(t, transform(actx))

A helper the task calls can still ask for the narrowest interface it needs (for example
``sdk.VariableClient`` instead of the full ``sdk.Client``), which documents the Airflow features it touches
and lets a test pass a fake.

.. _go-sdk/client:

The ``sdk.Client`` surface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``actx.Client()`` returns an ``sdk.Client``, which composes three smaller interfaces, so a helper can depend
on just one:

* ``VariableClient`` - ``GetVariable`` (returns the Variable as a string), ``UnmarshalJSONVariable``
  (decodes a JSON Variable into a pointer you provide), ``SetVariable``, and ``DeleteVariable``.
* ``ConnectionClient`` - ``GetConnection``, returning a ``Connection`` with fields ``ID``, ``Type``,
  ``Host``, ``Port``, ``Login``, ``Password``, ``Path``, ``Extra`` (a ``map[string]any``), plus a
  ``GetURI()`` helper.
* ``XComClient`` - ``GetXCom`` to read an upstream task's XCom and ``PushXCom`` to publish one.

``GetXCom`` returns the stored value as an ``any``; see :ref:`go-sdk/types` for how the stored JSON maps to
Go types.

``SetVariable`` stores the value as a string, so encode structured data (for example with ``json.Marshal``)
before storing it.

.. code-block:: go

    client := actx.Client()
    if err := client.SetVariable(actx, "process_threshold", "42", "Rows above this count take the slow path"); err != nil {
        return err
    }
    if err := client.DeleteVariable(actx, "legacy_threshold"); err != nil {
        return err
    }

.. note::

  A value supplied by a secrets backend (for example an ``AIRFLOW_VAR_*`` environment variable) still takes
  precedence over the stored value when the Variable is read back. Calling ``SetVariable`` with an empty
  description clears any existing description.

Not-found lookups return sentinel errors - ``VariableNotFound``, ``ConnectionNotFound``, ``XComNotFound`` -
so you can branch on a missing value with ``errors.Is`` rather than parsing an error string.

.. _go-sdk/runtime-context:

Reading the task runtime context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``airflow.Context`` carries the identifiers and scheduling timestamps of the running task instance and its
Dag run -- the Go equivalent of the execution context the Python and Java SDKs expose:

.. code-block:: go

    func extract(actx airflow.Context) (any, error) {
        ti := actx.TaskInstance()
        actx.Logger().InfoContext(actx, "running",
            "dag_id", ti.DagID,
            "run_id", ti.RunID,
            "task_id", ti.TaskID,
            "try_number", ti.TryNumber,
            "logical_date", actx.DagRun().LogicalDate,
        )
        return nil, nil
    }

``actx.TaskInstance()`` returns ``DagID``, ``RunID``, ``TaskID``, ``MapIndex`` (nil for an unmapped task),
and ``TryNumber``; ``actx.DagRun()`` returns ``DagID``, ``RunID``, and the ``*time.Time`` fields
``LogicalDate``, ``DataIntervalStart``, and ``DataIntervalEnd`` (nil when the run has no such value, e.g. a
manual trigger).

.. _go-sdk/arguments:

Receiving arguments from the stub Task
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A stub Task's arguments reach a Go handler in one of two ways:

1. **Positional binding** -- each data parameter takes the argument in the same position.
2. **Struct-based (keyword) binding** -- a sole struct parameter takes the arguments by field name.

Every parameter after the ``airflow.Context`` is a **data parameter**, filled in declaration order from the
arguments of the Python stub Task's TaskFlow call. A literal in the Dag file (``transform("uk", ...)``)
decodes straight into the parameter; an upstream task's output (``transform(..., extract())``) is pulled
from that task's XCom in the current Dag run. If the argument count does not match, or an argument's
declared type cannot fill the Go type, the task fails before its body runs.

.. code-block:: go

    // The Python stub Task calls transform("uk", extract()).
    func transform(actx airflow.Context, country string, extracted map[string]any) error {
        actx.Logger().InfoContext(actx, "transforming", "country", country)
        return nil
    }

When a task's **sole** data parameter is a struct, its fields bind **by name** instead of by position --
keyword arguments rather than positional ones. Being the only data parameter is the opt-in; there is no
marker to add.

.. code-block:: go

    type CombineInput struct {
        Region    string `arg:"region_code"` // renamed
        Threshold float64
    }

    // The Python stub Task calls combine(region_code="uk", threshold=0.5).
    func Combine(actx airflow.Context, input CombineInput) (any, error) {
        return nil, nil
    }

An exported field binds the argument matching its own Go name, folding case and underscores, so
``Threshold`` takes ``threshold``; reach for an ``arg:"<name>"`` tag when the names genuinely differ, as
``Region`` does above. The `Go SDK README
<https://github.com/apache/airflow/blob/main/go-sdk/README.md>`__ has the full binding rules, including
how unmatched fields and arguments are treated and when an untagged struct is decoded whole from a single
argument instead.

Stub parameters the Dag author left at their Python defaults are the exception to both shapes: they reach
the wire but need no Go parameter, so adding a defaulted parameter to a stub does not break the Go
functions already bound to it.

.. _go-sdk/types:

XCom type mapping
-----------------

XCom values are stored as JSON in Airflow's metadata database. The table below shows how those JSON types
surface as Go values when read back via ``GetXCom``.

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Python type
     - JSON
     - Go type (from ``GetXCom``)
   * - ``int``
     - number (integer)
     - numeric (see note)
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

  ``GetXCom`` returns the value exactly as decoded from the transport; there is no typed XCom
  deserialization layer yet. The Python supervisor encodes values as ``msgpack``, so a whole number arrives
  as a Go integer type (whose width depends on the value) and only a non-integer arrives as ``float64``. Do
  not assume a fixed integer width: type-switch over the numeric types you expect, or round-trip the value
  through ``json.Marshal`` / ``json.Unmarshal`` into a typed Go value.

.. _go-sdk/build:

Building and packaging
----------------------

A plain ``go build`` produces a runnable binary, but a *deployable* bundle (binary + embedded source +
manifest) must be produced with ``airflow-go-pack``. The packer compiles the bundle and appends the embedded
metadata footer, so the coordinator can read its ``dag_id``\ s without executing the binary, producing a
single runnable file. The on-disk format the packer emits (the ``AFBNDL01`` footer and the
``airflow-metadata.yaml`` manifest) is the bundle format shared by all native-executable SDKs, specified in
:doc:`task-sdk:executable-bundle-spec`.

``airflow-go-pack`` ships via the Go 1.24 ``tool`` directive, so there is no global install: add

.. code-block:: text

    tool github.com/apache/airflow/go-sdk/cmd/airflow-go-pack

to your bundle module's ``go.mod`` and run it with ``go tool airflow-go-pack``. This pins the packer version
per project.

Build and pack in one step; any flags after ``--`` are forwarded verbatim to ``go build``:

.. code-block:: bash

    go tool airflow-go-pack ./example/bundle -- -trimpath -tags=prod

Use ``--output <path>`` to write the packed bundle straight into a directory the coordinator scans
(``executables_root``):

.. code-block:: bash

    go tool airflow-go-pack --output ~/airflow/executable-bundles/sample-dag-bundle ./example/bundle

Cross-platform builds
~~~~~~~~~~~~~~~~~~~~~~~

The worker that runs a bundle often uses a different operating system or CPU architecture than your build
machine (for example, deploying to a Linux host from an Apple-silicon ``darwin/arm64`` laptop). Pass
``--goos`` / ``--goarch`` and the packer cross-builds for you:

.. code-block:: bash

    go tool airflow-go-pack --goos linux --goarch amd64 \
      --output ~/airflow/executable-bundles/sample-dag-bundle \
      ./example/bundle

Alternatively, pack a pre-built binary with ``--executable`` / ``--source``. The packer normally execs the
binary with ``--airflow-metadata`` to read its manifest, but a cross-compiled binary cannot run on the build
host. In that case, generate the manifest on a machine that *can* run the binary and feed it to the packer
with ``--airflow-metadata``:

.. code-block:: bash

    # On a linux/amd64 machine:
    go build -o my-bundle ./example/bundle
    ./my-bundle --airflow-metadata > airflow-metadata.yaml

    # Back on the darwin/arm64 machine:
    go tool airflow-go-pack --executable ./my-bundle --source main.go \
      --airflow-metadata airflow-metadata.yaml

(``--executable`` is mutually exclusive with ``--goos`` / ``--goarch`` and with ``go build`` flags after
``--``, since it packs an already-built binary instead of building one.)

Deploying
~~~~~~~~~

Copy or mount the packed bundle into a directory listed in the coordinator's ``executables_root``. The
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` scans those directories recursively,
matches the incoming ``dag_id`` against each bundle's manifest, verifies the bundle's integrity hash, and
launches the matching bundle. Bundles are identified by the trailer magic, not by filename (no extension on
Linux/macOS, ``.exe`` on Windows), so the file name on the worker is irrelevant.

.. _go-sdk/coordinator-config:

:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` configuration
---------------------------------------------------------------------------------------------

All ``kwargs`` in the ``coordinators`` config entry are passed to the
:class:`~airflow.sdk.coordinators.executable.ExecutableCoordinator` constructor:

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

   * - Parameter
     - Default
     - Description
   * - ``executables_root``
     - *(required)*
     - One or more directories scanned recursively for executable bundles. Accepts a string,
       a path, or a list of strings/paths.
   * - ``task_startup_timeout``
     - ``10.0``
     - Seconds to wait for the bundle subprocess to connect after launch. Increase this if your
       bundle startup is slow (e.g. on constrained hardware).

.. _go-sdk/limitations:

Limitations
-----------

* **A Python stub Dag is still required.** The Execution API does not yet carry Dag structure for non-Python
  languages, so task names and dependencies are declared in Python with
  :func:`@task.stub <airflow.sdk.task.stub>`. This is a documented known limitation.
