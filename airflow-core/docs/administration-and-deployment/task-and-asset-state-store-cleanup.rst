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

.. _task-and-asset-state-store-cleanup:

Task and Asset State Store Cleanup
==================================

.. versionadded:: 3.3

Airflow does not automatically purge task state store rows on a schedule. Cleanup (also known as "garbage collection") is the responsibility of the user (you) and must be triggered explicitly via the CLI. This page explains what gets cleaned up, how to run it, and how to integrate it into a recurring maintenance workflow.


What gets cleaned up
--------------------

The cleanup command operates only on **task state store** rows in the ``MetastoreBackend``. Asset store rows are never touched by this command. Asset store rows are removed only by the orphan sweep when an asset is deactivated (see :ref:`task-and-asset-state-store`).

A task state store row is eligible for deletion when its ``expires_at`` timestamp is in the past. ``expires_at`` is computed on the worker at write time:

* Keys written with an explicit ``retention=timedelta(...)`` expire after that duration from the time of the write.
* Keys written with ``retention=None`` (the default) pick up an expiry based on ``[state_store] default_retention_days``. If that value is ``> 0``, the key expires that many days after the write.
* Keys written with ``retention=NEVER_EXPIRE`` have ``expires_at = NULL`` and a flag that marks them as permanent. They are **never** deleted by this command regardless of configuration.

If ``[state_store] default_retention_days = 0``, keys written without an explicit retention have ``expires_at = NULL`` (no expiry) and are also skipped. Only keys with a non-null, past ``expires_at`` are removed.

.. note::

   Custom backends (``[state_store] backend`` set to anything other than the default) are **explicitly skipped**. The cleanup command prints a message and exits cleanly without deleting anything. If your custom backend needs its own retention logic, implement it in ``BaseStoreBackend.cleanup()`` and call it from your own maintenance process.


Running cleanup
---------------

The command is::

    airflow state-store cleanup-task-state-store

It reads ``[state_store] default_retention_days`` and ``[state_store] state_cleanup_batch_size`` from the ``airflow.cfg`` file, then deletes all eligible rows.

**Dry run**

Use ``--dry-run`` to preview what would be deleted without removing anything::

    airflow state-store cleanup-task-state-store --dry-run

The output lists every row that would be deleted, grouped by dag, run, task, map index, and key.

**Batching**

By default (``state_cleanup_batch_size = 0``) all eligible rows are deleted in a single statement. On deployments with large ``task_state_store`` tables, set a batch size to reduce lock duration per transaction::

    # airflow.cfg
    [state_store]
    state_cleanup_batch_size = 10000

The command then deletes rows in batches of 10,000, committing after each batch, until no eligible rows remain.

How often to run cleanup depends on your write volume and the value of ``default_retention_days``. A weekly cleanup may be sufficient for most environments. For high-throughput pipelines that write task state store entries on every task execution, consider running cleanup more frequently to keep the ``task_state_store`` table small.
