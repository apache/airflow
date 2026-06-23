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

.. _concepts:asset-state-store:

.. spelling:word-list::

   subscripted
   subscripting

Asset State Store
=================

.. versionadded:: 3.3

Asset store is a persistent key/value store scoped to an *asset*, independent of any particular DAG run. Unlike :doc:`task state store </core-concepts/task-state-store>`, which is tied to a single task instance, asset state store persists across runs and is logically owned by the asset itself. It is the natural home for cross-run metadata such as watermarks, incremental-load cursors, and per-asset configuration.

Asset store is accessed through the task context via ``context["asset_state_store"]``.


When is ``asset_state_store`` available?
----------------------------------------

When using asset state store within a task, ``context["asset_state_store"]`` is populated for **concrete** :class:`~airflow.sdk.definitions.asset.Asset` inlets and outlets. A task must declare at least one concrete inlet or outlet for ``asset_state_store`` to contain any entries.

Accessing asset state store using ``context``
---------------------------------------------

An asset becomes available through context["asset_state_store"] when it is included in inlets or outlets. You can then retrieve its asset state store by subscripting context["asset_state_store"] with the asset object.

.. code-block:: python

    from airflow.sdk import Asset, DAG, task

    my_asset = Asset("my_data", uri="s3://bucket/my_data")

    with DAG("example_asset_store", schedule=None):

        @task(inlets=[my_asset], outlets=[my_asset])
        def process(**context):
            asset_state_store = context["asset_state_store"][my_asset]
            watermark = asset_state_store.get("watermark")
            asset_state_store.set("watermark", "2024-06-01")

To see asset state store in-action in a real DAG, checkout the DAG in `example_asset_state_store.py <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/example_dags/example_asset_state_store.py>`_.

Single-inlet shorthand
~~~~~~~~~~~~~~~~~~~~~~~

For tasks with exactly **one** concrete inlet or outlet, you can call ``get``, ``set``, ``delete``, and ``clear`` directly on ``context["asset_state_store"]`` without subscripting.

.. code-block:: python

    @task(inlets=[my_asset], outlets=[my_asset])
    def process_single(**context):
        asset_state_store = context["asset_state_store"]
        watermark = asset_state_store.get("watermark")
        asset_state_store.set("watermark", "2024-06-01")

If the task has more than one concrete inlet or outlet, calling the shorthand raises a ``ValueError``. Use the subscript form (``context["asset_state_store"][my_asset]``) whenever a task has multiple inlets.


API reference
-------------

The following methods are available on both the per-asset accessor (``context["asset_state_store"][my_asset]``) and the shorthand (``context["asset_state_store"]``) when the task has exactly one inlet.

``get(key, default)``
~~~~~~~~~~~~~~~~~~~~~

Returns the stored JSON value, or the ``default`` value if the key does not exist.

.. code-block:: python

    # Using context
    watermark = context["asset_state_store"][my_asset].get("watermark", default="initial_watermark")

``set(key, value)``
~~~~~~~~~~~~~~~~~~~

Writes or overwrites a key-value pair. Unlike task state store, asset state store has no ``retention`` parameter. Values persist until explicitly deleted or until the asset is deactivated. Like with task state store, ``value`` can be any JSON-compatible type, except for ``None``. This includes:

* ``str``
* ``int``
* ``float``
* ``bool``
* ``list``
* ``dict``

.. code-block:: python

    # Using context
    context["asset_state_store"][my_asset].set("watermark", "2024-06-01T00:00:00Z")

``delete(key)``
~~~~~~~~~~~~~~~

Deletes a single key. No-op if the key does not exist.

.. code-block:: python

    # Using context
    context["asset_state_store"][my_asset].delete("watermark")

``clear()``
~~~~~~~~~~~

Deletes *all* asset state store keys for the asset.

.. code-block:: python

    # Using context
    context["asset_state_store"][my_asset].clear()

Some Example Use cases
----------------------

Watermark pattern
~~~~~~~~~~~~~~~~~

The canonical use case for asset state store is an incremental-load task that advances a watermark on each run. The watermark is stored on the asset itself so any task that reads or writes that asset can access it. This use case is especially applicable when building things like asset "watchers" using ``BaseEventTrigger``'s.

.. code-block:: python

    from airflow.sdk import Asset, DAG, task

    orders = Asset("orders", uri="s3://data/orders/")

    with DAG("incremental_orders", schedule="@daily"):

        @task(inlets=[orders], outlets=[orders])
        def load_new_orders(**context):
            asset_state_store = context["asset_state_store"]  # single-inlet shorthand

            # Read the last watermark, default to epoch if first run.
            watermark = asset_state_store.get("watermark", default="1970-01-01T00:00:00Z")

            # Fetch only rows created after the watermark.
            rows = fetch_orders_since(watermark)
            if not rows:
                return

            upload_to_warehouse(rows)

            # Advance the watermark to the latest row seen.
            new_watermark = max(r["created_at"] for r in rows)
            asset_state_store.set("watermark", new_watermark)

On each run the task reads the watermark left by the previous run, fetches only the new data, and then advances the watermark. Because asset state store persists across runs, the next run starts exactly where this one left off, even across retries, manual re-runs, or Scheduler restarts.


Lifetime and garbage collection
--------------------------------

Asset store rows persist indefinitely. They are **not** subject to the ``[state_store] default_retention_days`` time-based expiry that applies to task state store.

The only automatic cleanup is an *orphan sweep*: when an asset is deactivated (no ``asset_active`` record exists), its store rows are removed during the next garbage-collection pass. Until that sweep runs, stale rows may exist in the database but cannot be written to. The Execution API resolver filters to active assets only.

To remove asset state store entries explicitly, call ``clear()`` from within a task, or use the REST API.

.. note::

   ``[state_store] clear_on_success`` does **not** clear asset state store. Asset store is cross-run by design, so automatic task-level cleanup would destroy information that the next run depends on. Always clear asset state store explicitly when it is no longer needed.
