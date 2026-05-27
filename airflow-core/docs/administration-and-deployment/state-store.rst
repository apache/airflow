 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements. See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership. The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License. You may obtain a copy of the License at

 ..  http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied. See the License for the
    specific language governing permissions and limitations
    under the License.

.. _state-store:

State Store Configuration
==========================

.. versionadded:: 3.3

The state store is the persistence layer for :doc:`task state </core-concepts/task-state>` and :doc:`asset state </core-concepts/asset-state>`. By default, both are stored in the Airflow metadata database. This page describes the available configuration options, garbage-collection semantics, and how to provide a custom backend.

Configuration reference
-----------------------

All options live under the ``[state_store]`` section of ``airflow.cfg``.

.. note::

   The config section is ``[state_store]``, **not** ``[task_state]``.

``backend``
~~~~~~~~~~~

Full dotted path to a class that implements :class:`~airflow.sdk.state.BaseStateBackend`. Defaults to the built-in metastore backend.

.. code-block:: ini

    [state_store]
    backend = mypackage.state.CustomStateBackend

``default_retention_days``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of days to retain **task state** rows after their last update. Rows older than this are deleted during the next garbage collection pass.

* Set to ``0`` to disable time-based cleanup entirely.
* Default: ``30``.
* This setting does **not** apply to asset state rows.

.. code-block:: ini

    [state_store]
    default_retention_days = 30

``clear_on_success``
~~~~~~~~~~~~~~~~~~~~

When ``True``, all task state keys for a task instance are automatically deleted when that task instance moves to the ``success`` state. Defaults to ``False``, which preserves task state after success for observability (e.g.the submitted job ID or the last row count is still readable from the UI orREST API after the run completes).

.. important::

   ``clear_on_success`` clears **task state only**. It has no effect on asset state. Asset state is scoped to the asset rather than the task instance and must be cleared explicitly.

.. code-block:: ini

    [state_store]
    clear_on_success = False

``state_cleanup_batch_size``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of rows deleted per batch during garbage collection cleanup. Set to ``0`` (default) to delete all matching rows in a single statement. Tune this on deployments with large ``task_state`` tables to reduce lock contention.

.. code-block:: ini

    [state_store]
    state_cleanup_batch_size = 10000

Worker-side backend (``[workers] state_backend``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A separate, optional config key under ``[workers]`` lets you route task state and asset state values through a worker-side backend before they reach the API server.

.. code-block:: ini

    [workers]
    state_backend = mypackage.state.S3StateBackend

When this is set, ``TaskStateAccessor.set()`` calls ``serialize_task_state_to_ref()`` on the worker-side backend before sending the returned value (a reference to the actual storage) to the Execution API, and ``get()`` calls ``deserialize_task_state_from_ref()`` after receiving the stored reference from the Execution API. See `Custom worker-side backends`_ below.


Garbage collection semantics
-----------------------------

The cleanup task, also known as "garbage collection" is triggered using the Airflow CLI. The command to trigger the cleanup task is ``airflow state-store cleanup-task-states``. This process removes state rows according to the following rules:

**Time-based expiry (task state only)**
  Rows whose ``expires_at < now()`` are deleted. ``expires_at`` is computed on the *worker* at write time, not by the server.

**``default_retention_days`` fallback (task state only)**
  Keys written with no explicit ``retention`` (i.e. ``expires_at`` is ``NULL``) are governed by the global ``default_retention_days`` setting. When this setting is positive, the garbage collection job treats such rows as expiring ``default_retention_days`` days after their last update.

**``NEVER_EXPIRE`` keys**
  Keys set with ``retention=NEVER_EXPIRE`` are stored with ``expires_at = NULL`` and a flag that tells the garbage collection to skip them unconditionally. They are never deleted by time-based cleanup, regardless of ``default_retention_days``.

**Orphan sweep (asset state)**
  Asset state rows for assets that no longer have an ``asset_active`` record are deleted during the orphan-sweep pass. This cleans up state for deactivated or renamed assets.

.. important::

   Garbage collection only works for the ``MetastoreStateBackend``. Custom backends are explicitly skipped.



Custom backends
---------------

To replace the default metastore backend, subclass :class:`~airflow.sdk.state.BaseStateBackend` and implement all eight abstract methods.

Abstract methods
~~~~~~~~~~~~~~~~

There are four synchronous methods and four async equivalents:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Method
     - Description
   * - ``get(scope, key, *, session)``
     - Return the stored value, or ``None``.
   * - ``set(scope, key, value, *, expires_at, session)``
     - Write or overwrite a key. ``expires_at`` is a UTC datetime or ``None``
       for non-expiring keys.
   * - ``delete(scope, key, *, session)``
     - Delete a single key; no-op if absent.
   * - ``clear(scope, *, all_map_indices, session)``
     - Delete all keys under the scope.
   * - ``aget(scope, key, *, session)``
     - Async variant of ``get``.
   * - ``aset(scope, key, value, *, expires_at, session)``
     - Async variant of ``set``.
   * - ``adelete(scope, key, *, session)``
     - Async variant of ``delete``.
   * - ``aclear(scope, *, all_map_indices, session)``
     - Async variant of ``clear``.

Dispatching on scope type
~~~~~~~~~~~~~~~~~~~~~~~~~

Each method receives a ``scope`` argument that is either a :class:`~airflow.sdk.state.TaskScope` or an :class:`~airflow.sdk.state.AssetScope`. Use a ``match`` statement to dispatch:

.. code-block:: python

    from airflow.sdk.state import BaseStateBackend, TaskScope, AssetScope


    class MyBackend(BaseStateBackend):
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

Worker-side backends extend ``BaseStateBackend`` with two pairs of serialization hooks. They are configured separately via ``[workers] state_backend`` and run *on the worker process*, not on the API server. This lets you store large payloads or credentialed data directly using worker infrastructure while only a compact reference string is kept in the database.

Override these four methods:

``serialize_task_state_to_ref(*, value, key, ti_id)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Called by ``TaskStateAccessor.set()`` before sending the value to the Execution API. Return a reference string (e.g. an S3 key) that will be stored in the database instead of the raw value.

``deserialize_task_state_from_ref(stored)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Called by ``TaskStateAccessor.get()`` after retrieving the reference string from the Execution API. Return the actual value.

``serialize_asset_state_to_ref(*, value, key, asset_ref)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Same as the task variant, but for asset state. ``asset_ref`` is the asset name or URI, depending on how the accessor was constructed.

``deserialize_asset_state_from_ref(stored)``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Called by ``AssetStateAccessor.get()`` to resolve the stored reference back to the actual value.

.. important::

   **References must be deterministic.**  Given the same inputs (``ti_id`` + ``key`` for task state; ``asset_ref`` + ``key`` for asset state), the serialization method must always return the same reference string. Do not embed timestamps, random UUIDs, or any other non-deterministic component in the reference path.

   When a key is deleted or cleared, Airflow clears the database reference *first*, then calls the backend's ``delete()`` or ``clear()`` method. If backend cleanup fails after the DB row is gone, the external object is orphaned. Because the reference is deterministic, a subsequent ``set()`` for the same key will overwrite the orphaned object, making the failure recoverable. A non-deterministic reference would leave the external object permanently orphaned with no way to locate it.

Example skeleton:

.. code-block:: python

    from airflow.sdk.state import BaseStateBackend, TaskScope, AssetScope

    if TYPE_CHECKING:
        from pydantic import JsonValue


    class S3StateBackend(BaseStateBackend):

        def _task_ref(self, ti_id: str, key: str) -> str:
            return f"airflow/task-state/{ti_id}/{key}"

        def _asset_ref(self, asset_ref: str, key: str) -> str:
            import hashlib

            safe = hashlib.sha256(asset_ref.encode()).hexdigest()[:16]
            return f"airflow/asset-state/{safe}/{key}"

        def serialize_task_state_to_ref(self, *, value: JsonValue, key: str, ti_id: str) -> str:
            s3_key = self._task_ref(ti_id, key)
            s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(value).encode())
            return s3_key

        def deserialize_task_state_from_ref(self, stored: str) -> JsonValue:
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=stored)
            return json.loads(s3_object["Body"].read().decode())

        def serialize_asset_state_to_ref(self, *, value: JsonValue, key: str, asset_ref: str) -> str:
            s3_key = self._asset_ref(asset_ref, key)
            s3_client.put_object(Bucket=BUCKET, Key=s3_key, Body=json.dumps(value).encode())
            return s3_key

        def deserialize_asset_state_from_ref(self, stored: str) -> JsonValue:
            s3_object = s3_client.get_object(Bucket=BUCKET, Key=stored)
            return json.loads(s3_object["Body"].read().decode())

        # Implement the remaining abstract methods as pass-throughs or delegating to the
        # default MetastoreStateBackend for the DB side
        ...
