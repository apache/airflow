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

Each tab below declares the same Dag, ``orders_daily``:

* the ``extract`` task group counts orders for two regions,
* ``summarize`` adds them up,
* ``has_orders`` runs ``load`` when there are orders, and ``alert`` when there are none,
* ``pick_report`` picks a full or a brief report,
* ``trigger_finance`` starts a run of the ``finance_close`` Dag after ``load``, on a labeled edge.

.. tab-set::
    :sync-group: sdk

    .. tab-item:: TypeScript
        :sync: typescript

        A handler takes one object of named arguments, and calling a task names its inputs. The calls you
        write are the dependencies, and the compiler checks each one.

        .. code-block:: typescript

            import { Bundle, Dag, Label, triggerDagRun } from "@apache-airflow/ts-sdk";

            const dag = new Dag("orders_daily", { schedule: "@daily", tags: ["orders"], queue: "typescript" });

            // A task group: its task ids are "extract.eu" and "extract.us".
            const extract = dag.taskGroup("extract");
            const eu = extract.task("eu", async (): Promise<number> => 120)();
            const us = extract.task("us", async (): Promise<number> => 80)();

            // Passing a task's result draws the dependency and delivers the value.
            const summarize = dag.task("summarize", async ({ eu, us }: { eu: number; us: number }) => eu + us);
            const total = summarize({ eu, us });

            const load = dag.task("load", async ({ total, table }: { total: number; table: string }) => {
              console.log(`Loading ${total} orders into ${table}`);
            });
            const loaded = load({ total, table: "warehouse.orders" });
            const alerted = dag.task("alert", async () => console.log("No orders today"))();

            // if / else: the side not taken is skipped.
            const hasOrders = async ({ total }: { total: number }) => total > 0;
            dag.if(hasOrders, { total }, { taskId: "has_orders" }).then(loaded).else(alerted);

            // switch / case: the decider returns the task to run.
            const full = dag.task("report_full", async () => console.log("Full report"))();
            const brief = dag.task("report_brief", async () => console.log("Brief report"))();
            const pickReport = async ({ total }: { total: number }) => (total > 1000 ? full : brief);
            dag.switch(pickReport, { total }, { taskId: "pick_report" }).case(full).case(brief);

            // Trigger another Dag after the load, on a labeled edge.
            const triggered = dag.task("trigger_finance", triggerDagRun({ dagId: "finance_close" }))();
            loaded.before(Label.of("hand off").on(triggered));

            await new Bundle(dag).serve();

        See :ref:`typescript-sdk/dag-definition`.

    .. tab-item:: Java
        :sync: java

        Annotations carry the configuration. An ``@Airflow.Deps`` class wires the graph through a view that the
        annotation processor generates, so ``javac`` type-checks every edge. An interface-based API is also
        available.

        .. code-block:: java

            package com.example;

            import static java.lang.System.Logger.Level.INFO;

            import org.apache.airflow.sdk.*;

            @Airflow.Dag(id = "orders_daily", schedule = "@daily", tags = {"orders"}, queue = "java")
            public class OrdersDaily {
              private static final System.Logger log = System.getLogger(OrdersDaily.class.getName());

              // A task group: its task ids are "extract.eu" and "extract.us".
              @Airflow.TaskGroup(id = "extract")
              static class Extract {
                @Airflow.Task
                public long eu() { return 120L; }

                @Airflow.Task
                public long us() { return 80L; }
              }

              @Airflow.Task
              public long summarize(long eu, long us) { return eu + us; }

              @Airflow.Task
              public void load(long total, String table) {
                log.log(INFO, "Loading {0} orders into {1}", total, table);
              }

              @Airflow.Task
              public void alert() { log.log(INFO, "No orders today"); }

              // if / else: the side not taken is skipped.
              @Airflow.If(id = "has_orders")
              public boolean hasOrders(long total) { return total > 0; }

              @Airflow.Task(id = "report_full")
              public void reportFull() { log.log(INFO, "Full report"); }

              @Airflow.Task(id = "report_brief")
              public void reportBrief() { log.log(INFO, "Brief report"); }

              // switch / case: the decider returns the class generated for the task to run.
              @Airflow.Switch(id = "pick_report")
              public Class<? extends Task> pickReport(long total) {
                return total > 1000 ? OrdersDailyBuilder.ReportFull.class : OrdersDailyBuilder.ReportBrief.class;
              }

              // Trigger another Dag. This method runs when the Dag is built, not when the task runs.
              @Airflow.Task(id = "trigger_finance")
              public TriggerDagRun triggerFinance() { return new TriggerDagRun("finance_close"); }

              // Each call is a task, and passing its result to another call draws the dependency.
              @Airflow.Deps
              static class Wiring implements OrdersDailyDeps {
                void depends() {
                  var total = summarize(extract().eu(), extract().us());
                  var loaded = load(total, lit("warehouse.orders"));
                  hasOrders(total).Then(loaded).Else(alert());
                  pickReport(total).Case(reportFull()).Case(reportBrief());
                  loaded.before(Label.of("hand off").on(triggerFinance()));
                }
              }

              public static void main(String[] args) {
                Server.create(args).serve(new Bundle().register(OrdersDaily.class));
              }
            }

        See :ref:`java-sdk/dag-definition`.

    .. tab-item:: Go
        :sync: go

        ``airflow.Inputs`` passes task results and literals to a task function, in parameter order. A task's id
        is its function's name unless a ``TaskSpec`` sets one.

        .. code-block:: go

            package main

            import (
                "log"

                "github.com/apache/airflow/go-sdk/airflow"
            )

            func main() {
                bundle := airflow.Bundle()
                bundle.Register(ordersDaily())
                if err := bundle.Serve(); err != nil {
                    log.Fatal(err)
                }
            }

            func ordersDaily() *airflow.DagRef {
                dag := airflow.Dag("orders_daily", airflow.DagSpec{
                    Schedule: "@daily",
                    Tags:     []string{"orders"},
                    Queue:    "golang",
                })

                // A task group: its task ids are "extract.eu" and "extract.us".
                extract := dag.TaskGroup("extract")
                eu := extract.Task(countOrders, airflow.TaskSpec{TaskID: "eu"}, airflow.Inputs(airflow.Literal("eu")))
                us := extract.Task(countOrders, airflow.TaskSpec{TaskID: "us"}, airflow.Inputs(airflow.Literal("us")))

                // Passing a task's result draws the dependency and delivers the value.
                total := dag.Task(summarize, airflow.Inputs(eu, us))
                loaded := dag.Task(load, airflow.Inputs(total, airflow.Literal("warehouse.orders")))

                // if / else: the side not taken is skipped.
                dag.If(hasOrders, airflow.TaskSpec{TaskID: "has_orders"}, airflow.Inputs(total)).
                    Then(loaded).
                    Else(dag.Task(alert))

                // switch / case: the decider returns the task to run.
                r := &reports{
                    full:  dag.Task(reportFull, airflow.TaskSpec{TaskID: "report_full"}),
                    brief: dag.Task(reportBrief, airflow.TaskSpec{TaskID: "report_brief"}),
                }
                dag.Switch(r.pick, airflow.TaskSpec{TaskID: "pick_report"}, airflow.Inputs(total)).
                    Case(r.full).
                    Case(r.brief)

                // Trigger another Dag after the load, on a labeled edge.
                triggered := dag.Task(
                    airflow.TriggerDagRun(airflow.TriggerDagRunSpec{DagID: "finance_close"}),
                    airflow.TaskSpec{TaskID: "trigger_finance"},
                )
                loaded.Before(airflow.Label("hand off").On(triggered))

                return dag
            }

            var ordersByRegion = map[string]int{"eu": 120, "us": 80}

            func countOrders(_ airflow.Context, region string) (int, error) { return ordersByRegion[region], nil }

            func summarize(_ airflow.Context, eu, us int) (int, error) { return eu + us, nil }

            func load(actx airflow.Context, total int, table string) error {
                actx.Logger().InfoContext(actx, "loading orders", "total", total, "table", table)
                return nil
            }

            func alert(actx airflow.Context) error {
                actx.Logger().InfoContext(actx, "no orders today")
                return nil
            }

            func hasOrders(_ airflow.Context, total int) (bool, error) { return total > 0, nil }

            func reportFull(actx airflow.Context) error {
                actx.Logger().InfoContext(actx, "full report")
                return nil
            }

            func reportBrief(actx airflow.Context) error {
                actx.Logger().InfoContext(actx, "brief report")
                return nil
            }

            // reports holds the switch's cases, because a decider returns a *TaskRef.
            type reports struct{ full, brief *airflow.TaskRef }

            func (r *reports) pick(_ airflow.Context, total int) (*airflow.TaskRef, error) {
                if total > 1000 {
                    return r.full, nil
                }
                return r.brief, nil
            }

        See :ref:`go-sdk/dag-definition`.

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

Compatibility matrix
--------------------

Language SDKs mature independently, so they do not all support the same features yet. The tables below show
what each Language SDK supports: the features common to both authoring modes, the features of a
:ref:`Dag definition <language-sdks/dag-definition>`, and the features of a
:ref:`Python Dag with stub TaskHandler <language-sdks/stub-taskhandler>`. A supported feature shows the SDK
release that shipped it. See the legend below the tables for what each mark means.

.. BEGIN AUTO-GENERATED LANG-SDK COMPAT MATRIX

.. list-table:: Language SDK versions
   :header-rows: 1
   :widths: 30 15 15 15

   * - Version
     - Go
     - Java
     - TypeScript
   * - Min. Airflow version
     - 3.5.0
     - 3.5.0
     - 3.5.0
   * - Current Supervisor Schema Version
     - 2027-02-28
     - 2027-02-28
     - 2027-02-28

.. list-table:: Common to both authoring modes
   :header-rows: 1
   :widths: 30 15 15 15

   * - Dimension
     - Go
     - Java
     - TypeScript
   * - **TaskInstance states**
     -
     -
     -
   * - ``success`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``failed`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``up_for_retry`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``skipped`` (SHOULD)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``deferred`` (MAY)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``up_for_reschedule`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``awaiting_input`` (MAY)
     - ✗
     - ✗
     - ✓ 1.0.0
   * - ``removed`` (MAY)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - **Task runtime**
     -
     -
     -
   * - ``task-logging`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``xcom-read-write`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``custom-xcom-backend`` (MAY)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``connection-read`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``variable-read-write`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``self-contained-bundle`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``retry-policy`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``task-state-store`` (MAY)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``asset-state-store`` (MAY)
     - ✗
     - ✓ 1.0.0
     - ✗
   * - ``asset-event-emit`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``asset-event-read`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``object-store`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``listeners`` (MAY)
     - ✗
     - ✗
     - ✗

.. list-table:: Dag definition
   :header-rows: 1
   :widths: 30 15 15 15

   * - Dimension
     - Go
     - Java
     - TypeScript
   * - ``dag-definition-authoring`` (SHOULD)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``dag-args`` (MUST †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``task-args`` (MUST †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``dag-params`` (MUST †)
     - ✗
     - ✗
     - ✗
   * - ``dag-callbacks`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``explicit-dependencies`` (MUST †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``edge-labels`` (MAY †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``taskflow-dependencies`` (MUST †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``task-group`` (MAY †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``task-loops`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``if-else-branching`` (SHOULD †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``switch-case-branching`` (SHOULD †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``trigger-dag-run`` (MAY †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``dynamic-dag-generation`` (MAY †)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``human-in-the-loop`` (MAY †)
     - ✗
     - ✗
     - ✓ 1.0.0
   * - ``dag-test`` (SHOULD †)
     - ✗
     - ✗
     - ✗
   * - ``asset-definition`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``asset-scheduling`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``mapped-tasks`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``asset-inlets-outlets`` (MAY †)
     - ✗
     - ✗
     - ✗
   * - ``task-callbacks`` (MAY †)
     - ✗
     - ✗
     - ✗

.. list-table:: Python Dag with stub TaskHandler
   :header-rows: 1
   :widths: 30 15 15 15

   * - Dimension
     - Go
     - Java
     - TypeScript
   * - ``mixed-lang-stub-target`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``taskflow-binding`` (MUST)
     - ✓ 1.0.0
     - ✓ 1.0.0
     - ✓ 1.0.0
   * - ``mapped-tasks`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``asset-inlets-outlets`` (MAY)
     - ✗
     - ✗
     - ✗
   * - ``task-callbacks`` (MAY)
     - ✗
     - ✗
     - ✗

*Marks:* ``✓`` supported (with the SDK release that shipped it, when known) · ``✗`` not supported · ``n/a`` not applicable · ``n/p`` not published. A tier marked ``†`` applies only when ``dag-definition-authoring`` is supported.

.. END AUTO-GENERATED LANG-SDK COMPAT MATRIX

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
