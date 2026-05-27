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

.. _state-api:

State REST API
==============

.. versionadded:: 3.3

The Airflow REST API exposes endpoints to read and manage task state and asset state from outside a running task. These endpoints are useful for administration tasks such as inspecting state written by a running task, clearing stuck keys, or seeding state before a run.

.. _state-api:task-state-endpoints:

Task state endpoints
--------------------

All task state endpoints share the prefix::

    /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states

**Notes:**

* Keys may contain slashes. The ``{key}`` path parameter uses FastAPI's ``{key:path}`` matcher, so ``/api/v2/.../states/my/nested/key`` works as expected.
* The optional ``?map_index`` query parameter (default ``-1``) selects a specific mapped instance. ``-1`` is used for non-mapped tasks.
* ``?all_map_indices=true`` on the Clear endpoint ignores ``map_index`` and clears state for every mapped instance of the task.
* Values are limited to **64 KB** for the default metastore backend.

List task state entries
~~~~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states``

Returns a paginated list of all state entries for the task instance.

Query parameters: ``?limit``, ``?offset``, ``?map_index``

Get a single task state entry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states/{key}``

Returns the value, ``updated_at``, and ``expires_at`` for the specified key.

Returns ``404`` if the key does not exist.

Set a task state entry
~~~~~~~~~~~~~~~~~~~~~~~

``PUT /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states/{key}``

Creates or overwrites the key. Request body:

.. code-block:: json

    {"value": "my_value"}

Returns ``204 No Content`` on success. Returns ``404`` if the task instance does not exist.

Delete a single task state key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DELETE /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states/{key}``

Deletes the key. No-op if the key does not exist. Returns ``204 No Content``.

Clear all task state keys
~~~~~~~~~~~~~~~~~~~~~~~~~~

``DELETE /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/states``

Deletes all state keys for the task instance. Pass ``?all_map_indices=true`` to clear state across every mapped instance of the task (the ``?map_index`` parameter is ignored when this flag is set).

Returns ``204 No Content``.

.. _state-api:asset-state-endpoints:

Asset state endpoints
----------------------

All asset state endpoints share the prefix::

    /api/v2/assets/{asset_id}/states

The ``{asset_id}`` is the integer primary key of the asset. All endpoints return ``404`` if the asset does not exist.

List asset state entries
~~~~~~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/assets/{asset_id}/states``

Returns a paginated list of all state entries for the asset.

Query parameters: ``?limit``, ``?offset``

Get a single asset state entry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``GET /api/v2/assets/{asset_id}/states/{key}``

Returns the value and ``updated_at`` for the specified key. Returns ``404``if the key does not exist.

Set an asset state entry
~~~~~~~~~~~~~~~~~~~~~~~~~

``PUT /api/v2/assets/{asset_id}/states/{key}``

Creates or overwrites the key. Request body:

.. code-block:: json

    {"value": "2024-06-01T00:00:00Z"}

Returns ``204 No Content`` on success.

Delete a single asset state key
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DELETE /api/v2/assets/{asset_id}/states/{key}``

Deletes the key. No-op if the key does not exist. Returns ``204 No Content``.

Clear all asset state keys
~~~~~~~~~~~~~~~~~~~~~~~~~~~

``DELETE /api/v2/assets/{asset_id}/states``

Deletes all state keys for the asset. Returns ``204 No Content``.
