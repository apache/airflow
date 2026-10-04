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


Language SDK Spec
=================

This document fixes the terms an Airflow Language SDK (Go, Java, TypeScript, ...) uses for
its user-facing authoring interface, and how they fit together. Every SDK spells them in its
own language's idiom; the terms and the flow are the same everywhere.

Spec version: ``1.0``.

There are two authoring features, and they differ in one thing: who owns the graph.

.. mermaid::

    flowchart TD
        subgraph F1["Feature 1 — Mixed Language Task Handler"]
            direction TB
            P1["Python @task.stub<br/>declares dag_id, task_id,<br/>arguments, and every edge"]
            H1["fn -&gt; TaskHandler(dagId, taskId, fn)"]
            H2[TaskHandlerRef]
            P1 -. "binds by dag_id + task_id" .-> H1
            H1 --> H2
        end

        subgraph F2["Feature 2 — Native Dag"]
            direction TB
            D1["Dag(spec)"]
            D2["dag<br/>(owns the schedule, the tasks, the edges)"]
            D3["fn -&gt; dag.Task(fn, options)"]
            D4a[TaskRef]
            D4b[TaskRef]
            D1 --> D2 --> D3 --> D4a
            D4a -- "Inputs(ref): data edge<br/>(carries a value)" --> D4b
            D4a -. "before / after: order edge<br/>(carries nothing)" .-> D4b
        end

        H2 --> R["bundle.register(Dag | TaskHandler)"]
        D4b --> R
        R --> SV["bundle.serve()<br/>the task subprocess entrypoint"]

A ``TaskHandler`` supplies a body for a task Python already declared, so it names the
``dagId``/``taskId`` pair it binds to and nothing else. A native ``Dag`` owns the schedule, the
tasks, and the edges, so ``dag.Task`` returns a ``TaskRef`` that edges attach to. Both features
land in the same ``bundle``, and one ``bundle.serve()`` call serves both, so a single process can carry
native Dags and mixed-language task handlers at once.

Terms
-----

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Term
     - Definition
   * - ``fn``
     - The function callable itself: the task body a user writes.
   * - ``TaskHandler``
     - Callable interface factory over ``(dagId, taskId, fn)``; returns a ``TaskHandlerRef``.
   * - ``Dag``
     - Callable interface factory over a Dag spec; returns a ``DagRef``.
   * - ``dag``
     - The instance a ``Dag`` call returns.
   * - ``dag.Task``
     - Callable interface factory over ``(fn, options)``; returns a ``TaskRef``.
   * - ``bundle``
     - Holds every ``Dag`` and ``TaskHandler``. One ``register`` takes either kind, in any
       mixture, and the bundle serves them.
   * - ``serve``
     - A method on the ``bundle``: the task subprocess's entrypoint calls ``bundle.serve()``,
       which serves it for the lifetime of the process. Nothing outside the ``bundle`` needs a
       handle on the serving machinery.

Per-SDK spelling
----------------

.. list-table::
   :header-rows: 1
   :widths: 16 28 28 28

   * - Term
     - Go
     - Java
     - TypeScript
   * - ``fn``
     - a ``func``
     - an annotated method, or an ``InputTask``
     - an ``async`` function
   * - ``TaskHandler``
     - ``airflow.TaskHandler(dagId, taskId, fn)``
     - ``@Builder.TaskHandler(dagId, taskId)``
     - ``new TaskHandler(dagId, taskId, fn)``
   * - ``Dag``
     - ``airflow.Dag(spec)``
     - ``@Builder.Dag(id, ...)``, or ``new Dag(id)``
     - ``new Dag(id)``
   * - ``dag``
     - ``*airflow.DagRef``
     - ``Dag``
     - ``Dag``
   * - ``dag.Task``
     - ``dag.Task(fn, opts...)``
     - ``@Builder.Task(id)``, or ``dag.task(id, cls)``
     - ``dag.task(id, fn)``
   * - data edge
     - ``airflow.Inputs(refs...)``
     - the ``@Wiring`` call expression
     - the task factory call
   * - order edge
     - ``Before`` / ``After``
     - ``before`` / ``after``
     - ``before`` / ``after``
   * - ``bundle``
     - ``airflow.Bundle()`` -> ``*airflow.BundleRef``
     - ``new Bundle()``
     - ``new Bundle()``
   * - ``register``
     - ``bundle.Register(items ...airflow.Registration)``
     - ``bundle.register(dag, handler, ...)``
     - ``bundle.register(dag, handler, ...)``
   * - ``serve``
     - ``bundle.Serve()`` in ``main``
     - ``bundle.serve(args)`` in ``main``
     - ``await bundle.serve()`` in the entry module

Task subprocess lifecycle
-------------------------

Authoring is only half the contract. Every SDK runs the same sequence inside the task
subprocess, once per task instance:

.. mermaid::

    sequenceDiagram
        participant S as Supervisor
        participant T as Task subprocess

        T->>T: 1. process start
        S->>T: 2. send StartupDetails
        T->>T: 3. build Context + Client,<br/>bind the cancellation signal
        T->>T: 4. look up the registered fn<br/>for (dag_id, task_id)
        T->>T: 5. bind arguments: TaskFlow data,<br/>plus ctx/client where injected
        rect rgb(230, 230, 250)
            note right of T: scope holding Context + Client<br/>(a getter reads this scope)
            T->>T: 6. invoke fn
        end
        T->>S: 7. push the return value to XCom,<br/>report the terminal state

Steps 2 through 5 and step 7 belong to the SDK; step 6 is the only one that runs code a user
wrote. How ``fn`` reaches ``Context`` and ``Client`` is each SDK's own choice — parameters, or
getters that read the scope opened at step 6 — and all this spec asks is that ``fn`` receives the
same pair the SDK built at step 3. The terminal state at step 7 is one of ``SucceedTask``,
``RetryTask``, or ``TaskState``, and it is reported exactly once.

Task handler parse lifecycle
----------------------------

To check a Python Dag's ``@task.stub`` tasks, the Dag processor starts the process that a task of the
Dag would run and asks it for its ``TaskHandler`` registrations:

.. mermaid::

    sequenceDiagram
        participant D as Dag processor
        participant R as SDK process

        R->>R: 1. process start
        D->>R: 2. send TaskHandlerParseRequest
        R->>R: 3. declare every registered TaskHandler,<br/>keyed by dag_id, in registration order
        R->>D: 4. send one TaskHandlerParsingResult
        D->>R: 5. reply to it
        R->>R: 6. exit

No ``fn`` runs: the answer comes from the registrations alone, and the tasks of a native ``Dag`` are
never declared. It must depend only on the artifact, never on the request. A declaration states whether
the stub task's arguments bind to the handler's parameters by position or by name, and which values each
parameter accepts. An SDK that cannot list a handler's parameters sends ``null`` for them, and then only
the handler's presence is checked. An artifact that registers no ``TaskHandler`` answers with an empty
mapping.
