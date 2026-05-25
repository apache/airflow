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

Asset state is accessed through the task context via ``context["asset_state"]``, or via the ``AssetState`` Task SDK mechanism.


When is ``asset_state`` available?
------------------------------------

``context["asset_state"]`` is populated for **concrete** :class:`~airflow.sdk.definitions.asset.Asset` inlets and outlets.  It is *not* available for :class:`~airflow.sdk.definitions.asset.AssetAlias` inlets. Aliases are resolved at runtime, but asset state accessors are only created for concrete assets declared directly on the task. A task must declare at least one concrete inlet or outlet for ``asset_state`` to contain any entries.

Asset state is also available using ``AssetState``, provided by the Task SDK. This approach is more commonly used when building ``BaseEventTrigger``'s for things like asset-watching.

.. warning::

   **Outlets-only tasks**: if a task declares only ``outlets`` (no ``inlets``), ``context["asset_state"][my_asset]`` may raise a ``KeyError`` at runtime.  The workaround is to declare the asset in **both** ``inlets`` and ``outlets``.

   .. code-block:: python
       # my_asset defined above ...

       @task(inlets=[my_asset], outlets=[my_asset])
       def write_asset(**context):
           context["asset_state"][my_asset].set("watermark", "2024-01-01")

   This known issue will be resolved in a future release. A workaround for this is to use the ``AssetState`` class, provided as part of the Task SDK. More details on that below!


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

Accessing asset state using ``AssetState``
------------------------------------------

Asset state can also be retrieved for an asset using the ``airflow.sdk.AssetState`` class. This approach does NOT require that an asset is passed to a task using ``inlets``.

.. code-block:: python
    from airflow.sdk import DAG, task, AssetState

    with DAG("example_asset_state", schedule=None):

        @task()
        def process():
            asset_state = AssetState(name="my_data")
            watermark = asset_state.get("watermark")
            asset_state.set("watermark", "2024-06-01")

In the example above, the ``name`` of the asset is used to retrieve it's state. However, the ``uri`` can also be used:

.. code-block:: python
    from airflow.sdk import DAG, task, AssetState

    with DAG("example_asset_state", schedule=None):

        @task()
        def process():
            asset_state = AssetState(uri="s3://bucket/my_data")
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

The following methods are available on both the per-asset accessor (``context["asset_state"][my_asset]``), the shorthand (``context["asset_state"]``) when the task has exactly one inlet, and when using the ``AssetState`` Task SDK class.

``get(key)``
~~~~~~~~~~~~

Returns the stored string value, or ``None`` if the key does not exist.

.. code-block:: python

    # Using context
    watermark = context["asset_state"][my_asset].get("watermark")

    # Using the Task SDK
    AssetState(name="my_data").get("watermark")

``set(key, value)``
~~~~~~~~~~~~~~~~~~~

Writes or overwrites a key-value pair. Unlike Task state, asset state has no ``retention`` parameter. Values persist until explicitly deleted or until the asset is deactivated.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].set("watermark", "2024-06-01T00:00:00Z")

    # Using the Task SDK class
    AssetState(name="my_data").set("watermark", "2024-06-01T00:00:00Z")

``delete(key)``
~~~~~~~~~~~~~~~

Deletes a single key.  No-op if the key does not exist.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].delete("watermark")

    # Using the Task SDK class
    AssetState(name="my_data").delete("watermark")

``clear()``
~~~~~~~~~~~

Deletes *all* state keys for the asset.

.. code-block:: python

    # Using context
    context["asset_state"][my_asset].clear()

    # Using the Task SDK class
    AssetState(name="my_data").clear()

Use cases
---------

Watermark pattern
~~~~~~~~~~~~~~~~~

The canonical use case for asset state is an incremental-load task that advances a watermark on each run.  The watermark is stored on the asset itself so any task that reads or writes that asset can access it. *This use case is especially applicable when building things like asset "watchers" using ``BaseEventTrigger``'s.

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
