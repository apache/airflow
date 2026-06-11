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

Prerequisites
-------------

* Go 1.24 or later to build and pack bundles. This is a build-time requirement only; the worker that runs a
  packed bundle needs no Go toolchain, because the bundle is a self-contained native executable.
* The packed bundle must be accessible from the Airflow worker, under a directory the coordinator scans.
* The ``apache-airflow-task-sdk`` package (installed with Airflow) provides the coordinator; no additional
  Python packages are needed.

Deployment modes
----------------

A packed bundle can run in two ways. The same binary works in both, and you pick one per deployment:

* **Coordinator (recommended).** A Python task runner launches the Go bundle directly, with no separate Go
  worker process on the host. This is the same coordinator mechanism the Java SDK uses. Because the mature
  Python supervisor handles the Airflow-facing concerns, this path inherits remote task logs (S3/GCS), the
  full range of task states, and alternate XCom backends, rather than implementing them again in Go. Those are
  exactly the features the Edge Worker path is still missing.
* **Edge Worker.** A long-running Go process (``airflow-go-edge-worker``) polls Airflow for work and runs
  your bundle, with no Python in the data path. It runs end-to-end today but is missing the features listed
  under :ref:`go-sdk/limitations`.

The rest of this guide covers the recommended coordinator path; see :ref:`go-sdk/edge-worker` for a summary
of the Edge Worker.

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

A task is an ordinary Go function. The runtime inspects its signature and injects arguments by type, so each
task declares only the parameters it needs.

.. code-block:: go

    import (
        "log/slog"
        "runtime"

        "github.com/apache/airflow/go-sdk/sdk"
    )

    func extract(ctx sdk.TIRunContext, client sdk.Client, log *slog.Logger) (any, error) {
        conn, err := client.GetConnection(ctx, "test_http")
        if err != nil {
            return nil, err
        }
        log.Info("fetched connection", "host", conn.Host)
        // ... do work, honour ctx cancellation ...
        return map[string]any{"go_version": runtime.Version()}, nil
    }

    func transform(ctx sdk.TIRunContext, client sdk.VariableClient, log *slog.Logger) error {
        val, err := client.GetVariable(ctx, "my_variable")
        if err != nil {
            return err
        }
        log.Info("obtained variable", "my_variable", val)
        return nil
    }

.. note::

  As with the other language SDKs, XCom *dependencies* are declared in the Python stub Dag (they define task
  order). The value must still be read explicitly in Go via ``client.GetXCom``, and produced either by the
  task's ``(any, error)`` return value or by ``client.PushXCom``.

Go entry point
~~~~~~~~~~~~~~~

Implement ``bundlev1.BundleProvider`` to register your Dags and tasks; ``main`` is one line. ``RegisterDags``
is the single source of truth for which ``dag_id`` and task names this bundle can run, so the generated
manifest can never drift from what the binary actually executes.

.. code-block:: go

    import (
        "log"

        v1 "github.com/apache/airflow/go-sdk/bundle/bundlev1"
        "github.com/apache/airflow/go-sdk/bundle/bundlev1/bundlev1server"
    )

    type myBundle struct{}

    var _ v1.BundleProvider = (*myBundle)(nil)

    func (m *myBundle) GetBundleVersion() v1.BundleInfo {
        return v1.BundleInfo{Name: bundleName, Version: &bundleVersion}
    }

    func (m *myBundle) RegisterDags(dagbag v1.Registry) error {
        simpleDag := dagbag.AddDag("simple_dag") // must match the Python dag_id
        simpleDag.AddTask(extract)               // task_id is taken from the function name
        simpleDag.AddTask(transform)
        return nil
    }

    func main() {
        if err := bundlev1server.Serve(&myBundle{}); err != nil {
            log.Fatal(err)
        }
    }

The ``dag_id`` passed to ``AddDag`` must match the ``dag_id`` of the Python Dag, and each registered task's
name must match a ``@task.stub`` function in that Dag.

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

The runtime inspects a task function's signature and injects arguments by type, so you only declare the
parameters your task actually needs:

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Parameter type
     - Injected value
   * - ``sdk.TIRunContext``
     - The task's execution context: the cancellation/deadline signal plus the task instance identifiers and
       Dag run timestamps. Respect it for long-running work. See :ref:`go-sdk/runtime-context`.
   * - ``*slog.Logger``
     - A logger whose output is routed back to the Airflow task log.
   * - ``sdk.Client`` (or a narrower interface)
     - A client for Airflow Variables, Connections, and XCom.

An optional ``(any, error)`` return value becomes the task's ``return_value`` XCom. A non-nil ``error`` (or a
panic, which the runtime recovers) marks the task instance failed in Airflow, triggering retries if
configured on the stub.

Requesting the narrowest interface you need (for example ``sdk.VariableClient`` instead of the full
``sdk.Client``) documents which Airflow features the task touches and makes unit testing easier, because you
can pass a fake in tests.

.. _go-sdk/client:

The ``sdk.Client`` surface
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``sdk.Client`` composes three smaller interfaces, so a task can depend on just one:

* ``VariableClient`` - ``GetVariable`` (returns the Variable as a string) and ``UnmarshalJSONVariable``
  (decodes a JSON Variable into a pointer you provide).
* ``ConnectionClient`` - ``GetConnection``, returning a ``Connection`` with fields ``ID``, ``Type``,
  ``Host``, ``Port``, ``Login``, ``Password``, ``Path``, ``Extra`` (a ``map[string]any``), plus a
  ``GetURI()`` helper.
* ``XComClient`` - ``GetXCom`` to read an upstream task's XCom and ``PushXCom`` to publish one.

``GetXCom`` returns the stored value as an ``any``; see :ref:`go-sdk/types` for how the stored JSON maps to
Go types.

Not-found lookups return sentinel errors - ``VariableNotFound``, ``ConnectionNotFound``, ``XComNotFound`` -
so you can branch on a missing value with ``errors.Is`` rather than parsing an error string.

.. _go-sdk/runtime-context:

Reading the task runtime context
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declare an ``sdk.TIRunContext`` parameter on a task to read the identifiers and scheduling timestamps of the
running task instance and its Dag run -- the Go equivalent of the execution context the Python and Java SDKs
expose. It is an interface that embeds ``context.Context``, so the same ``ctx`` drives cancellation and
client calls. The runtime binds it by type, just like the other injected parameters:

.. code-block:: go

    func extract(ctx sdk.TIRunContext, log *slog.Logger) (any, error) {
        ti := ctx.TaskInstance()
        log.Info("running",
            "dag_id", ti.DagID,
            "run_id", ti.RunID,
            "task_id", ti.TaskID,
            "try_number", ti.TryNumber,
            "logical_date", ctx.DagRun().LogicalDate,
        )
        return nil, nil
    }

``ctx.TaskInstance()`` returns ``DagID``, ``RunID``, ``TaskID``, ``MapIndex`` (nil for an unmapped task),
and ``TryNumber``; ``ctx.DagRun()`` returns ``DagID``, ``RunID``, and the ``*time.Time`` fields
``LogicalDate``, ``DataIntervalStart``, and ``DataIntervalEnd`` (nil when the run has no such value, e.g. a
manual trigger).

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
  deserialization layer yet. The concrete type of a *numeric* value therefore depends on the deployment
  mode. Over the Execution API (the Edge Worker path) numbers are decoded with ``encoding/json``, so every
  number - integer or not - arrives as ``float64``. In coordinator mode the Python supervisor re-encodes the
  value as ``msgpack``, so a whole number arrives as a Go integer type (whose width depends on the value) and
  only a non-integer as ``float64``. Do not assume a fixed numeric type: type-switch over the numeric types
  you expect, or round-trip the value through ``json.Marshal`` / ``json.Unmarshal`` into a typed Go value.

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

.. _go-sdk/edge-worker:

Alternative: the Go Edge Worker
-------------------------------

The same bundle binary can also run **without a Python coordinator**, under the standalone Go Edge Worker.
Rather than the worker launching the bundle once per task, ``airflow-go-edge-worker`` is a long-running Go
process that registers with the scheduler, polls the Edge Executor API for workloads, and runs the bundle
directly over HashiCorp ``go-plugin`` (gRPC), with no Python in the data path. The bundle source and
``RegisterDags`` registration are identical; only the deployment differs, and the mode is selected
automatically at launch from the CLI flags, so you do not change any task code.

This path does not use the ``[sdk] coordinators`` configuration and is currently missing the features listed
under :ref:`go-sdk/limitations`. See the Go SDK's own repository documentation for Edge Worker setup
(``airflow-go-edge-worker`` configuration and ``go install``).

.. _go-sdk/limitations:

Limitations
-----------

* **A Python stub Dag is still required.** The Execution API does not yet carry Dag structure for non-Python
  languages, so task names and dependencies are declared in Python with
  :func:`@task.stub <airflow.sdk.task.stub>`. This applies to both deployment modes and is a documented
  known limitation.

The following are a non-exhaustive list of features the **Edge Worker** path has yet to implement. They are
the main reason the coordinator path is recommended: in coordinator mode the Python supervisor handles these
concerns, so they are **not** limitations there.

* Putting tasks into states other than success or failed/up-for-retry (deferred, failed-without-retries,
  etc.).
* Remote task logs (e.g. S3/GCS).
* XCom reading/writing through non-default XCom backends.
