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

.. _howto/operator:DatabricksDeltaTableVersionSensor:

DatabricksDeltaTableVersionSensor (Phase 1)
===========================================

Use the :class:`~airflow.providers.databricks.sensors.databricks_delta_table.DatabricksDeltaTableVersionSensor`
to monitor a Delta table in Databricks and detect new commit versions. When a newer version is observed,
the sensor succeeds, pushes provenance metadata to XCom, and optionally emits an Airflow Asset event.

The sensor can execute synchronously in standard worker slots or in deferrable mode using
:class:`~airflow.providers.databricks.triggers.databricks_delta_table.DatabricksDeltaTableVersionTrigger`.

.. note::

    **Scope (Phase 1 only):** This sensor provides task-level Delta table version observation inside a scheduled
    DAG task run. A continuous external asset producer with long-running coalescing across runs is planned as a
    follow-up Phase 2 feature.


Using the Sensor
----------------

The sensor requires either ``table_name`` or ``unity_table`` to identify the target Delta table:

* ``table_name`` - String representing the 1-, 2-, or 3-part Delta table name (``table``, ``schema.table``, or ``catalog.schema.table``).
* ``unity_table`` - An instance of :class:`~airflow.providers.databricks.assets.databricks.UnityTableIdentity` specifying the host, catalog, schema, and table. When provided without explicit ``outlets``, the sensor automatically registers the corresponding Unity Catalog asset (``databricks://<host>/<catalog>/<schema>/<table>``) as an outlet.

One of the following Databricks SQL connection endpoints is required:

* ``sql_warehouse_name`` - Name of the Databricks SQL warehouse to execute queries against.
* ``http_path`` - HTTP path of the Databricks SQL warehouse or interactive cluster.

Optional parameters:

* ``baseline_version`` - The baseline Delta version to compare against. If specified, the sensor succeeds when the table version is strictly greater than ``baseline_version``. If omitted (``None``) and ``target_version`` is also ``None``, the sensor automatically captures the table\x27s current version on the first poke and waits for subsequent commits (initial enrollment pattern).
* ``target_version`` - An exact target Delta version to wait for.
* ``allow_recreation`` - Boolean (default ``True``). When ``True``, if the table is dropped and recreated (causing the Delta version to reset to 0 or a number lower than ``baseline_version``), the sensor recognizes the recreation as a change and succeeds. If ``False``, a lower version raises an ``AirflowException``.
* ``deferrable`` - Boolean (default ``conf.getboolean("operators", "default_deferrable", fallback=False)``). When ``True``, the sensor yields execution to the triggerer using ``DatabricksDeltaTableVersionTrigger``.
* ``poke_interval`` - Time in seconds between successive status checks (default ``60`` seconds).
* ``timeout`` - Maximum time in seconds to wait for a newer version before timing out (default 7 days).


Asset Outlet Events and Delivery Semantics
------------------------------------------

When the sensor declares outlets (or when ``unity_table`` automatically registers the Unity Catalog asset outlet),
the task-success asset event carries precise semantics:

.. important::

    The task-success asset event means **"a newer Delta version was observed"**, with the observed ``version``
    and ``table_identity`` included in the asset event ``extra`` dictionary.

    * It is **not** a vague "table refreshed" event.
    * It is **not** a guarantee that every intermediate commit was delivered. If multiple commits occurred between
      polling checks, intermediate versions are coalesced and only the latest observed version is emitted.
    * **Pinned reads require explicit versioning:** Observing Delta table version *N* does **not** automatically pin
      downstream queries to version *N*. Downstream tasks or queries reading the table will query the head of the table
      unless explicitly pinned. Authors who need a reproducible, pinned read **must explicitly** specify
      ``VERSION AS OF <observed_version>`` in their SQL queries (e.g. ``SELECT * FROM table VERSION AS OF {{ ti.xcom_pull(task_ids="sensor", key="delta_table_version")["version"] }}``)
      or Delta Lake reader options (e.g. ``read.option("versionAsOf", version)``).

The asset event ``extra`` dictionary includes:

* ``version`` - The newly observed Delta table version (integer).
* ``observed_version`` - Duplicate alias for ``version`` to make semantics unambiguous.
* ``table_identity`` - A dictionary representing the table identity (e.g., ``{"host": ..., "catalog": ..., "schema": ..., "table": ...}``).
* ``operation`` - The Delta operation that created the commit (e.g., ``WRITE``, ``MERGE``, ``CREATE OR REPLACE TABLE``).
* ``timestamp`` - The commit timestamp from the Delta transaction log history (if available).

This metadata is pushed to XCom under the key ``delta_table_version``.


Examples
--------

Waiting for table commits with baseline version:

.. code-block:: python

    from airflow.providers.databricks.sensors.databricks_delta_table import (
        DatabricksDeltaTableVersionSensor,
    )

    wait_for_updates = DatabricksDeltaTableVersionSensor(
        task_id="wait_for_table_updates",
        table_name="main.default.customers",
        sql_warehouse_name="my_warehouse",
        baseline_version=42,
        poke_interval=30,
        deferrable=True,
    )

Using Unity Catalog asset enrollment and downstream pinned read:

.. code-block:: python

    from airflow.providers.common.compat.assets import Asset
    from airflow.providers.databricks.assets.databricks import UnityTableIdentity
    from airflow.providers.databricks.sensors.databricks_delta_table import (
        DatabricksDeltaTableVersionSensor,
    )

    unity_table = UnityTableIdentity(
        host="my-workspace.cloud.databricks.com",
        catalog="analytics",
        schema="marketing",
        table="attribution_leads",
    )

    wait_for_attribution = DatabricksDeltaTableVersionSensor(
        task_id="wait_for_attribution_leads",
        unity_table=unity_table,
        sql_warehouse_name="my_warehouse",
        deferrable=True,
    )

    # Downstream DAG consuming the asset event and pinning reads to observed version:
    with DAG(
        dag_id="downstream_attribution_consumer",
        schedule=[unity_table.to_asset()],
        ...,
    ):
        ...
