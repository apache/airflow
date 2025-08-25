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

Postgres CDC Event Trigger
==========================

.. _howto/trigger:PostgresCDCEventTrigger:

The ``PostgresCDCEventTrigger`` is an event-based trigger that monitors a PostgreSQL table
for new changes based on a Change Data Capture (CDC) column, such as ``updated_at`` or ``last_modified``.

It is especially useful for **Airflow 3.0+** in combination with the ``AssetWatcher`` system,
enabling event-driven DAGs based on PostgreSQL updates.

How It Works
------------

1. Periodically queries the PostgreSQL table for the maximum value of the specified CDC column.
2. Compares this value with the last known value stored in an Airflow Variable via ``Variable.get()``.
3. If a newer value is found, emits a ``TriggerEvent`` that can resume a DAG run or trigger downstream logic.
4. Each instance uses a configurable ``state_key`` to manage independent trigger state.
5. The actual persistence of the new state must be done in a DAG task via ``Variable.set(...)``.

.. note::
    This trigger requires **Airflow >= 3.0** due to dependencies on:
    - ``AssetWatcher``
    - ``airflow.sdk.Variable``
    - Event-driven scheduling infrastructure

Usage Example with AssetWatcher
-------------------------------

Here's a basic example using the trigger inside an AssetWatcher with a manual persistence callback:

.. code-block:: python

    from airflow.models import DAG
    from airflow.decorators import task
    from airflow.sdk import Asset, AssetWatcher, Variable
    from airflow.providers.postgres.triggers.postgres_cdc import PostgresCDCEventTrigger
    from datetime import datetime

    with DAG(dag_id="example_postgres_cdc", start_date=datetime(2024, 1, 1), schedule=None):

        @task
        def persist_latest_value(cdc_event):
            # Persist the latest value (important!)
            Variable.set("my_table_state", cdc_event["max_iso"])

        watcher = AssetWatcher(
            asset=Asset("my_postgres_table"),
            event_trigger=PostgresCDCEventTrigger(
                conn_id="postgres_default",
                table="my_table",
                cdc_column="updated_at",
                polling_interval=60,
                state_key="my_table_state",
            ),
            callbacks=[persist_latest_value],
        )

Alternatively, you can refer to a complete working example in:

.. exampleinclude:: ../tests/system/postgres/example_postgres_cdc_asset_watcher.py
    :language: python
    :start-after: [START howto_operator_postgres_cdc_watcher]
    :end-before: [END howto_operator_postgres_cdc_watcher]

Parameters
----------

``conn_id``
    Airflow connection ID for the PostgreSQL database

``table``
    Name of the table to monitor

``cdc_column``
    Column used to track changes (e.g., ``updated_at``)

``polling_interval``
    Polling frequency in seconds (default: 10.0)

``state_key``
    Key used to retrieve the last known CDC value via ``Variable.get()``

Important Notes
---------------

1. **State Persistence Required**: This trigger **does not automatically persist** the new CDC value. You **must** persist it manually using a callback like ``Variable.set(...)``.
2. **Idempotency Warning**: If the value is not persisted, the trigger may repeatedly emit the same event.
3. **Multiple Watchers**: Avoid sharing the same ``state_key`` across multiple watchers unless explicitly coordinating their state.
4. **Error Handling**: The trigger includes internal error logging and handles polling exceptions gracefully.
5. **Timezone Support**: Supports both timezone-aware and naive datetime values by converting naive timestamps to UTC.

Reference
---------

* `PostgreSQL Documentation <https://www.postgresql.org/docs/current/index.html>`__
* `Airflow Event-driven Scheduling <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/deferring.html>`__
* :class:`airflow.providers.postgres.triggers.postgres_cdc.PostgresCDCEventTrigger`
