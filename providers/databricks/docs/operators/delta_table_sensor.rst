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

DatabricksDeltaTableVersionSensor
=================================

Use the :class:`~airflow.providers.databricks.sensors.databricks_delta_table.DatabricksDeltaTableVersionSensor`
to monitor a Delta table in Databricks and detect new commit versions. When a newer version is observed,
the sensor succeeds, pushes provenance metadata to XCom, and optionally emits an Airflow Asset event.


Using the Sensor
----------------

The sensor requires a target Delta table and a Databricks SQL endpoint.

To identify the table, specify either:

* ``table_name`` - String representing the 1-, 2-, or 3-part Delta table name (``table``, ``schema.table``, or ``catalog.schema.table``).
* ``unity_table`` - An instance of :class:`~airflow.providers.databricks.assets.databricks.UnityTableIdentity`. When provided without explicit ``outlets``, the sensor automatically registers the corresponding Unity Catalog asset (``databricks://<host>/<catalog>/<schema>/<table>``) as an outlet.

To connect to Databricks SQL, specify either ``sql_warehouse_name`` or ``http_path``.

Other parameters, such as connection credentials and timeout configuration, are documented in the class reference.


Waiting for a new version
-------------------------

By default, omitting ``baseline_version`` and ``target_version`` enrolls the table by capturing its current version on the first check and waiting for subsequent commits. You can also specify an explicit ``baseline_version`` to succeed when the table's version is strictly greater:

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
    )


Waiting for a specific version
------------------------------

To wait for an exact target Delta version, set ``target_version``:

.. code-block:: python

    from airflow.providers.databricks.sensors.databricks_delta_table import (
        DatabricksDeltaTableVersionSensor,
    )

    wait_for_v50 = DatabricksDeltaTableVersionSensor(
        task_id="wait_for_v50",
        table_name="main.default.customers",
        sql_warehouse_name="my_warehouse",
        target_version=50,
    )


Using the observed version downstream
-------------------------------------

When a new version is detected, the sensor pushes metadata to XCom under the key ``delta_table_version``, including ``version``, ``timestamp``, ``operation``, and ``table_identity``. If multiple commits occurred between polling checks, the sensor observes and reports the latest commit.

Downstream tasks requiring reproducible reads should pin their queries to the observed version using ``VERSION AS OF``:

.. code-block:: sql

    SELECT * FROM main.default.customers
    VERSION AS OF {{ ti.xcom_pull(task_ids="wait_for_table_updates", key="delta_table_version")["version"] }}

When an outlet asset is configured or inferred from ``unity_table``, the sensor also attaches the observed version and table identity to the asset event extra metadata.


Deferrable mode
---------------

To avoid consuming worker slots while polling, run the sensor in deferrable mode by setting ``deferrable=True`` (or globally via ``operators.default_deferrable``). The sensor yields execution to the triggerer until a newer version is observed.
