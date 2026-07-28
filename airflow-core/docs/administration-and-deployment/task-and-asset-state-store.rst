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

.. _task-and-asset-state-store:

Task and Asset State Store Configuration
========================================

.. versionadded:: 3.3

The task and asset state store is the persistence layer for :doc:`task state store </core-concepts/task-state-store>` and :doc:`asset state store </core-concepts/asset-state-store>`. By default, both are stored in the Airflow metadata database. This page describes the available configuration options, garbage-collection semantics, and how to provide a custom backend. Configuration (``[state_store]``), the CLI (``airflow state-store``), and the backend base class (:class:`~airflow.sdk.state.BaseStoreBackend`) all use the ``state_store`` name for this feature.

Configuration reference
-----------------------

All options live under the ``[state_store]`` section of ``airflow.cfg``.

.. note::

   The config section is ``[state_store]``, **not** ``[task_state_store]``.

``backend``
~~~~~~~~~~~

Full dotted path to a class that implements :class:`~airflow.sdk.state.BaseStoreBackend`. Defaults to the built-in metastore backend.

.. code-block:: ini

    [state_store]
    backend = mypackage.state.CustomStateBackend

``default_retention_days``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of days after which task state store rows expire. When a key is written with no explicit retention, expires_at is computed on the worker as now + default_retention_days. Changing this setting does not affect already-written rows.

* Set to ``0`` to disable time-based cleanup entirely.
* Default: ``30``.
* This setting does **not** apply to asset state store rows.

.. code-block:: ini

    [state_store]
    default_retention_days = 30

``clear_on_success``
~~~~~~~~~~~~~~~~~~~~

When ``True``, all task state store keys for a task instance are automatically deleted when that task instance moves to the ``success`` state. Defaults to ``False``, which preserves task state store entries after success for observability (e.g. the submitted job ID or the last row count is still readable from the UI or REST API after the run completes).

.. important::

   ``clear_on_success`` clears **task state store only**. It has no effect on asset state store. Asset store is scoped to the asset rather than the task instance and must be cleared explicitly.

.. code-block:: ini

    [state_store]
    clear_on_success = False

``state_cleanup_batch_size``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of rows deleted per batch during garbage collection cleanup. Set to ``0`` (default) to delete all matching rows in a single statement. Tune this on deployments with large ``task_state_store`` tables to reduce lock contention.

.. code-block:: ini

    [state_store]
    state_cleanup_batch_size = 10000

.. _task-and-asset-state-store:worker-backends:

Worker-side backend (``[workers] state_backend``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A separate, optional config key under ``[workers]`` lets you route task state store and asset state store values through a worker-side backend before they reach the API server.

.. code-block:: ini

    [workers]
    state_backend = mypackage.state.S3StateBackend

When this is set, ``TaskStateStoreAccessor.set()`` calls ``serialize_task_state_store_to_ref()`` on the worker-side backend before sending the returned value (a reference to the actual storage) to the Execution API, and ``get()`` calls ``deserialize_task_state_store_from_ref()`` after receiving the stored reference from the Execution API. See `Custom worker-side backends`_ below.


Garbage collection semantics
-----------------------------

The cleanup task, also known as "garbage collection" is triggered using the Airflow CLI. The command to trigger the cleanup task is ``airflow state-store clean``. This process removes store rows according to the following rules:

**Time-based expiry (task state store only)**
  Rows whose ``expires_at < now()`` are deleted. ``expires_at`` is computed on the *worker* at write time, not by the server.

**``default_retention_days`` fallback (task state store only)**
  Keys written with no explicit retention get an ``expires_at`` of now + default_retention_days computed at write time. Garbage collection deletes rows where ``expires_at < now()``."

**``NEVER_EXPIRE`` keys**
  Keys set with ``retention=NEVER_EXPIRE`` are stored with ``expires_at = NULL`` and a flag that tells the garbage collection to skip them unconditionally. They are never deleted by time-based cleanup, regardless of ``default_retention_days``.

**``on_delete=CASCADE`` (asset state store)**
  When an asset is deleted, all corresponding asset state store rows for that asset are deleted.

.. important::

   Garbage collection only works for the ``MetastoreBackend``. Custom backends are explicitly skipped.



Custom backends
---------------

A custom backend must subclass :class:`~airflow.sdk.state.BaseStoreBackend` and implement its abstract methods: ``get``, ``set``, ``delete``, and ``clear`` for synchronous callers and the ``aget``, ``aset``, ``adelete``, and ``aclear`` async equivalents. Refer to :class:`~airflow.sdk.state.BaseStoreBackend` for the full API.

Each method receives a ``scope`` argument that is either a :class:`~airflow.sdk.state.TaskScope` or an :class:`~airflow.sdk.state.AssetScope`. Use ``isinstance`` to dispatch:

.. code-block:: python

    from airflow.sdk.state import BaseStoreBackend, TaskScope, AssetScope


    class MyBackend(BaseStoreBackend):
        def get(self, scope, key, *, session=None):
            if isinstance(scope, TaskScope):
                return self._task_store.get(scope, key)
            elif isinstance(scope, AssetScope):
                return self._asset_store.get(scope, key)

:class:`~airflow.sdk.state.AssetScope` has three optional fields: ``asset_id`` (integer, server-side only), ``name``, and ``uri``. At least one must be set. Server-side operations (REST API calls) provide ``asset_id``. Worker-side operations provide ``name`` or ``uri`` (workers do not have access to the integer ``asset_id``).

Configure the class via ``[state_store] backend``:

.. code-block:: ini

    [state_store]
    backend = mypackage.state.MyBackend


Custom worker-side backends
----------------------------

Worker-side backends extend ``BaseStoreBackend`` with two pairs of serialization hooks. They are configured separately via ``[workers] state_backend`` and run *on the worker process*, not on the API server. This lets you store large payloads or credentialed data directly using worker infrastructure while only a compact reference string is kept in the database.

Override four serialization hooks from :class:`~airflow.sdk.state.BaseStoreBackend`:

* ``serialize_task_state_store_to_ref``: called by ``TaskStateStoreAccessor.set()`` before the value is sent to the Execution API; return a compact reference string (e.g. an S3 key) to be stored in the database instead of the raw value.
* ``deserialize_task_state_store_from_ref``: called by ``TaskStateStoreAccessor.get()`` after retrieving the reference from the backend; return the actual value.
* ``serialize_asset_state_store_to_ref``: same as the task variant but for asset state store; receives the asset scope as ``scope`` (an :class:`~airflow.sdk.state.AssetScope` with ``name`` and/or ``uri``).
* ``deserialize_asset_state_store_from_ref``: called by ``AssetStateStoreAccessor.get()`` to resolve the stored reference back to the actual value.

.. important::

   **References must be deterministic.**  Given the same inputs (``scope`` + ``key``), the serialization method must always return the same reference string. Do not embed timestamps, random UUIDs, or any other non-deterministic component in the reference path.

   When a key is deleted or cleared, Airflow clears the database reference *first*, then calls the backend's ``delete()`` or ``clear()`` method. If backend cleanup fails after the DB row is gone, the external object is orphaned. Because the reference is deterministic, a subsequent ``set()`` for the same key will overwrite the orphaned object, making the failure recoverable. A non-deterministic reference would leave the external object permanently orphaned with no way to locate it.

Example skeleton:

.. code-block:: python

    from airflow.sdk.state import BaseStoreBackend, TaskScope, AssetScope

    if TYPE_CHECKING:
        from pydantic import JsonValue


    class S3StateBackend(BaseStoreBackend):

        def _task_ref(self, scope: TaskScope, key: str) -> str:
            return f"airflow/task-store/{scope.dag_id}/{scope.run_id}/{scope.task_id}/{scope.map_index}/{key}"

        def _asset_ref(self, scope: AssetScope, key: str) -> str:
            import hashlib

            asset_identifier = scope.name or scope.uri or ""
            safe = hashlib.sha256(asset_identifier.encode()).hexdigest()[:16]
            return f"airflow/asset-store/{safe}/{key}"

        def serialize_task_state_store_to_ref(self, *, value: JsonValue, key: str, scope: TaskScope) -> str:
            s3_key = self._task_ref(scope, key)
            s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(value).encode())
            return s3_key

        def deserialize_task_state_store_from_ref(self, stored: str) -> JsonValue:
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=stored)
            return json.loads(s3_object["Body"].read().decode())

        def serialize_asset_state_store_to_ref(self, *, value: JsonValue, key: str, scope: AssetScope) -> str:
            s3_key = self._asset_ref(scope, key)
            s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(value).encode())
            return s3_key

        def deserialize_asset_state_store_from_ref(self, stored: str) -> JsonValue:
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=stored)
            return json.loads(s3_object["Body"].read().decode())

        # Implement the remaining abstract methods as pass-throughs or delegating to the
        # default MetastoreBackend for the DB side
        ...
