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

.. _concepts:asset-state:

Asset State
===========

.. versionadded:: 3.3

Asset state is a persistent key/value store scoped to an *asset*, independent of any particular DAG run.  Unlike :doc:`task state </core-concepts/task-state>`, which is tied to a single task instance, asset state persists across runs and is logically owned by the asset itself.  It is the natural home for cross-run metadata such as watermarks, incremental-load cursors, and per-asset configuration.

Asset state is accessed through the task context via ``context["asset_state"]``.


When is ``asset_state`` available?
------------------------------------

When using asset state within a task, ``context["asset_state"]`` is populated for **concrete** :class:`~airflow.sdk.definitions.asset.Asset` inlets and outlets. A task must declare at least one concrete inlet or outlet for ``asset_state`` to contain any entries.

If using asset state in a ``BaseEventTrigger``, the ``self.asset_state`` parameter can be used within the ``BaseEventTrigger``. It can be subscripted in the same way that ``context["asset_state"]`` can be.

.. warning::

   **Outlets-only tasks**: if a task declares only ``outlets`` (no ``inlets``), ``context["asset_state"][my_asset]`` may raise a ``KeyError`` at runtime.  The workaround is to declare the asset in **both** ``inlets`` and ``outlets``.

   .. code-block:: python
       # my_asset defined above ...

       @task(inlets=[my_asset], outlets=[my_asset])
       def write_asset(**context):
           context["asset_state"][my_asset].set("watermark", "2024-01-01")

   This known issue will be resolved in a future release.


Accessing asset state using ``context``
---------------------------------------

An asset can be brought into "scope" (for lack of a better phrase) by including it in ``inlets`` (or both ``inlets`` and ``outlets``). Then subscript ``context["asset_state"]`` with the asset object to retrieve the asset state.

.. code-block:: python

    from airflow.sdk import Asset, DAG, task

    my_asset = Asset("my_data", uri="s3://bucket/my_data")

    with DAG("example_asset_state", schedule=None):

        @task(inlets=[my_asset], outlets=[my_asset])
        def process(**context):
            asset_state = context["asset_state"][my_asset]
            watermark = asset_state.get("watermark")
            asset_state.set("watermark", "2024-06-01")

To see asset state in-action in a real DAG, checkout the DAG in `example_asset_state.py <https://github.com/apache/airflow/blob/main/airflow-core/src/airflow/example_dags/example_asset_state.py>`_.

Accessing asset state in a ``BaseEventTrigger``
-----------------------------------------------

When building Triggers used for asset "watching", asset state can be retrieved using the ``self.asset_state`` attribute.

.. code-block:: python
    from airflow.sdk import Asset, BaseEventTrigger, TriggerEvent
    from collections.abc import AsyncIterator

    class GenericEventTrigger(BaseEventTrigger):
        ...

        async def run(self) -> AsyncIterator[TriggerEvent]:
            """Logic that fires a TriggerEvent."""
            my_data = Asset(name="my_data")
            asset_state = self.asset_state[my_data]
            watermark = asset_state.get("watermark")
            asset_state.set("watermark", "2024-06-01")

In the example above, ``my_data`` is created using the ``name`` However, the ``uri`` can also be used:

.. code-block:: python
    from airflow.sdk import Asset, BaseEventTrigger, TriggerEvent
    from collections.abc import AsyncIterator

    class GenericEventTrigger(BaseEventTrigger):
        ...

        async def run(self) -> AsyncIterator[TriggerEvent]:
            """Logic that fires a TriggerEvent."""
            my_data = Asset(uri="s3://bucket/my_data")
            asset_state = self.asset_state[my_data]
            watermark = asset_state.get("watermark")
            asset_state.set("watermark", "2024-06-01")

Single-inlet shorthand
~~~~~~~~~~~~~~~~~~~~~~~

For tasks with exactly **one** concrete inlet, you can call ``get``, ``set``, ``delete``, and ``clear`` directly on ``context["asset_state"]`` without subscripting.

.. code-block:: python

    @task(inlets=[my_asset], outlets=[my_asset])
    def process_single(**context):
        asset_state = context["asset_state"]
        watermark = asset_state.get("watermark")
        asset_state.set("watermark", "2024-06-01")

If the task has more than one concrete inlet, calling the shorthand raises a ``ValueError``.  Use the subscript form (``context["asset_state"][my_asset]``) whenever a task has multiple inlets.


API reference
-------------

The following methods are available on both the per-asset accessor (``context["asset_state"][my_asset]``), the shorthand (``context["asset_state"]``) when the task has exactly one inlet, and when using the ``self.asset_state`` attribute.

``get(key)``
~~~~~~~~~~~~

Returns the stored JSON value, or ``None`` if the key does not exist.

.. code-block:: python

    # Using context
    watermark = context["asset_state"][my_asset].get("watermark")

    # Using self.asset_state
    watermark = self.asset_state[my_asset].get("watermark")

``set(key, value)``
~~~~~~~~~~~~~~~~~~~

Writes or overwrites a key-value pair. Unlike Task state, asset state has no ``retention`` parameter. Values persist until explicitly deleted or until the asset is deactivated.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].set("watermark", "2024-06-01T00:00:00Z")

    # Using self.asset_state
    self.asset_state[my_asset].set("watermark", "2024-06-01T00:00:00Z")

``delete(key)``
~~~~~~~~~~~~~~~

Deletes a single key.  No-op if the key does not exist.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].delete("watermark")

    # Using self.asset_state
    self.asset_state[my_asset].delete("watermark")

``clear()``
~~~~~~~~~~~

Deletes *all* state keys for the asset.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].clear()

    # Using self.asset_state
    self.asset_state[my_asset].clear()

Use cases
---------

Watermark pattern
~~~~~~~~~~~~~~~~~

The canonical use case for asset state is an incremental-load task that advances a watermark on each run.  The watermark is stored on the asset itself so any task that reads or writes that asset can access it. This use case is especially applicable when building things like asset "watchers" using ``BaseEventTrigger``'s.

.. code-block:: python

    from airflow.sdk import Asset, DAG, task

    orders = Asset("orders", uri="s3://data/orders/")


    with DAG("incremental_orders", schedule="@daily"):

        @task(inlets=[orders], outlets=[orders])
        def load_new_orders(**context):
            asset_state = context["asset_state"]  # single-inlet shorthand

            # Read the last watermark, default to epoch if first run.
            watermark = asset_state.get("watermark") or "1970-01-01T00:00:00Z"

            # Fetch only rows created after the watermark.
            rows = fetch_orders_since(watermark)
            if not rows:
                return

            upload_to_warehouse(rows)

            # Advance the watermark to the latest row seen.
            new_watermark = max(r["created_at"] for r in rows)
            asset_state.set("watermark", new_watermark)

On each run the task reads the watermark left by the previous run, fetches only the new data, and then advances the watermark.  Because asset state persists across runs, the next run starts exactly where this one left off, even across retries, manual re-runs, or Scheduler restarts.


Lifetime and garbage collection
--------------------------------

Asset state rows persist indefinitely.  They are **not** subject to the ``[state_store] default_retention_days`` time-based expiry that applies to task state.

The only automatic cleanup is an *orphan sweep*: when an asset is deactivated (no ``asset_active`` record exists), its state rows are removed during the next garbage-collection pass.  Until that sweep runs, stale rows may exist in the database but cannot be written to. The Execution API resolver filters to active assets only.

To remove asset state explicitly, call ``clear()`` from within a task, or use the :ref:`REST API <state-api:asset-state-endpoints>`.

.. note::

   ``[state_store] clear_on_success`` does **not** clear asset state. Asset state is cross-run by design, so automatic task-level cleanup would destroy information that the next run depends on.  Always clear asset state explicitly when it is no longer needed.
