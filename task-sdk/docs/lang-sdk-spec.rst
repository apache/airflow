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

.. code-block:: text

     FEATURE 1                                   FEATURE 2
     Mixed Language Stub Handler                 Native Dag
     Python owns the graph                       the SDK owns the graph

     Python @task.stub                           Dag(spec)
     declares dag_id, task_id,                          |
     arguments, and every edge                          v
           |                                           dag  <-- owns the schedule,
           | binds by dag_id + task_id                  |        the tasks, the edges
           v                                            v
     fn --> StubHandler(dagId, taskId, fn)       fn --> dag.Task(fn, options)
                    |                                           |
                    v                                           v
             StubHandlerRef                                  TaskRef
                    |                                           |
                    |                                           |  Inputs(ref)      data edge, carries a value
                    |                                           |  before / after   order edge, carries nothing
                    |                                           v
                    |                                        TaskRef
                    |                                           |
                    +---------------------+---------------------+
                                          |
                                          v
                         bundle.register(Dag | StubHandler)
                                          |
                                          v
                                   bundle.serve()  <-- the task subprocess entrypoint

A ``StubHandler`` supplies a body for a task Python already declared, so it names the
``dagId``/``taskId`` pair it binds to and nothing else. A native ``Dag`` owns the schedule, the
tasks, and the edges, so ``dag.Task`` returns a ``TaskRef`` that edges attach to. Both features
land in the same ``bundle``, and one ``bundle.serve()`` call serves both, so a single process can carry
native Dags and mixed-language stub handlers at once.

Terms
-----

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Term
     - Definition
   * - ``fn``
     - The function callable itself: the task body a user writes.
   * - ``StubHandler``
     - Callable interface factory over ``(dagId, taskId, fn)``; returns a ``StubHandlerRef``.
   * - ``Dag``
     - Callable interface factory over a Dag spec; returns a ``DagRef``.
   * - ``dag``
     - The instance a ``Dag`` call returns.
   * - ``dag.Task``
     - Callable interface factory over ``(fn, options)``; returns a ``TaskRef``.
   * - ``bundle``
     - Holds every ``Dag`` and ``StubHandler``. One ``register`` takes either kind, in any
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
   * - ``StubHandler``
     - ``airflow.StubHandler(dagId, taskId, fn)``
     - ``@Builder.StubHandler(dagId, taskId)``
     - ``new StubHandler(dagId, taskId, fn)``
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

.. code-block:: text

   1  process start
   2  receive StartupDetails from the supervisor
   3  build Context + Client, bind the cancellation signal
   4  look up the registered fn for (dag_id, task_id)
   5  bind arguments: TaskFlow data, plus ctx/client where they are injected
   6  +-- scope holding Context + Client --+
      |             invoke fn              |   <-- a getter reads this scope
      +------------------------------------+
   7  push the return value to XCom, report the terminal state

Steps 2 through 5 and step 7 belong to the SDK; step 6 is the only one that runs code a user
wrote. How ``fn`` reaches ``Context`` and ``Client`` is each SDK's own choice — parameters, or
getters that read the scope opened at step 6 — and all this spec asks is that ``fn`` receives the
same pair the SDK built at step 3. The terminal state at step 7 is one of ``SucceedTask``,
``RetryTask``, or ``TaskState``, and it is reported exactly once.
