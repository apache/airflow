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

.. _concepts:task-and-asset-state-store-overview:

Task and Asset State Store Overview
===================================

.. versionadded:: 3.3

Airflow has always modeled tasks as stateless, idempotent units of work. A growing class of workloads, however, require some amount of data to be persisted outside of a task's return value, like a submitted job ID that must survive a worker crash, a watermark that advances run-by-run, or a row counter exposed for observability. Task state store and Asset state store fill that gap without touching the XCom or Variable systems.

Task and Asset State Store
--------------------------

Task and Asset state store provide two key/value stores to persist data like a job ID, watermark, or row count. These two state stores are differentiated by *what* they are scoped to:

.. list-table::
   :header-rows: 1
   :widths: 20 25 25 30

   * - Store
     - Scope
     - Default lifetime
     - Primary use case
   * - **Task state store**
     - A single task Instance (``dag_id`` + ``run_id`` + ``task_id`` + ``map_index``)
     - Configurable retention; cleared on task success when ``clear_on_success = True``
     - Survive retries, track in-flight jobs, checkpoint progress within a run, resume progress from checkpoint set by a past run
   * - **Asset state store**
     - An asset (independent of any particular run)
     - Persists indefinitely; removed only when the asset is deactivated
     - Cross-run watermarks, incremental-load cursors, per-asset metadata

Both stores accept JSON-able values. Values can be stored using the default metastore backend, or be offloaded via a :ref:`custom worker-side backend <task-and-asset-state-store:worker-backends>`.

When to use Task and Asset State Store
--------------------------------------

Use this table to choose the right mechanism for your use case.

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Mechanism
     - When to use it
   * - **XCom**
     - Pass data *between tasks* within a single Dag run (e.g. the output of one task consumed by a downstream task) or across different multiple Dag runs (referencing the data persisted from another run). XComs are cleared on retry, and should NOT be used to persist data across task retries or across runs.
   * - **Variables**
     - Deployment-wide or installation-wide configuration that changes infrequently and is set by operators rather than by tasks themselves.
   * - **Task state store**
     - Data that must survive a worker crash or data that must survive across retries within the **same run**. An external job ID written before a long-running job completes is a perfect use case for task state store.
   * - **Asset state store**
     - Data that must persist **across asset events** or while asset "watching" and is logically owned by an asset rather than a task. For example, a watermark that advances each time a file lands in an object store.

.. note::

   If your current implementation already leverages an XCom-based pattern successfully, there's **no need to migrate** to task state store. Task state store is meant to solve problems that XCom was never designed for.

Further reading
---------------

* :doc:`Task State Store <task-state-store>`: full API reference and use-case examples
* :doc:`Asset State Store </core-concepts/asset-state-store>`: watermark pattern and API reference
* :doc:`Task and Asset State Store Configuration </administration-and-deployment/task-and-asset-state-store>`: retention, GC, and custom backends
