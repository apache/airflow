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

.. _concepts:state-overview:

Task and Asset State Overview
========

.. versionadded:: 3.3

Airflow has always modeled tasks as stateless, idempotent units of work. A growing class of workloads, however, require a small amount of data to be persisted outside of a Task's return value, like a submitted job ID that must survive a worker crash, a watermark that advances run-by-run, or a row counter exposed for observability. Task state and Asset state fill that gap without touching the XCom or Variable systems.

Task and Asset State
--------------------

Airflow 3.3 ships two persistent key/value stores, differentiated by *what* they are scoped to:

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Store
     - Scope
     - Default lifetime
     - Primary use case
   * - **Task state**
     - A single task Instance (dag_id + run_id + task_id + map_index)
     - Configurable retention; cleared on task success when ``clear_on_success = True``
     - Survive retries, track in-flight jobs, checkpoint progress within a run
   * - **Asset state**
     - An asset (independent of any particular run)
     - Persists indefinitely; removed only when the asset is deactivated
     - Cross-run watermarks, incremental-load cursors, per-asset metadata

Both stores accept string keys and JSON values. Values up to 64 KB are supported through the default metastore backend; larger payloads can be offloaded via a :ref:`custom worker-side backend <state-store:worker-backends>`.

When to use Task and Asset State
--------------------------------

Use this table to choose the right mechanism for your use case.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Mechanism
     - When to use it
   * - **XCom**
     - Pass data *between tasks* within a single Dag run (e.g. the output of one task consumed by a downstream task). XComs are cleared on retry, and should NOT be used to persist state across task retries or across runs.
   * - **Variables**
     - Dag-wide or installation-wide configuration that changes infrequently and is set by operators rather than by tasks themselves.
   * - **Task state**
     - State that must survive a worker crash or a retry within the **same run**. An external job ID written before a long-running job completes is a perfect use case for task state.
   * - **Asset state**
     - State that must persist **across asset events** or while asset "watching" and is logically owned by an asset rather than a task. For example, a watermark that advances each time a file lands in an object store.

.. note::

   If your current implementation already leverages an XCom-based pattern successfully, there's **no need to migrate** to task state. Task state is meant to solve problems that XCom was never designed for.

Further reading
---------------

* :doc:`Task State <task-state>`: full API reference and use-case examples
* :doc:`Asset State </authoring-and-scheduling/asset-state>`: watermark pattern and API reference
* :doc:`State Store Configuration </administration-and-deployment/state-store>`: retention, GC, and custom backends
