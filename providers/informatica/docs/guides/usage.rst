
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

Usage Guide
===========

The Informatica provider enables automatic lineage tracking for Airflow tasks that define inlets and outlets.

How It Works
------------

The Informatica plugin automatically detects tasks with lineage support and sends inlet/outlet information to Informatica EDC when tasks succeed. No additional configuration is required beyond defining inlets and outlets in your tasks.

Key Features
------------

- **Manual Lineage**: Explicitly declare inlets and outlets using EDC object URIs — the provider validates each URI against the catalog before the task executes and creates the lineage links on success.
- **Automatic SQL Lineage**: When ``auto_lineage_enabled = True`` (the default), the provider parses the ``sql`` attribute of SQL operators, resolves detected tables in the Informatica catalog, and creates lineage links automatically.  Supported SQL dialects include PostgreSQL, MySQL, Snowflake, BigQuery, Databricks, Redshift, SQLite, Oracle, Trino, Presto, Hive, Spark, and MSSQL.
- **Lineage Priority**: Manual inlets/outlets always take precedence over automatic SQL lineage.  If a task has any inlets or outlets defined, SQL parsing is skipped entirely.
- **Per-task Control**: Disable or re-enable automatic lineage per task or per DAG using :func:`~airflow.providers.informatica.lineage.disable_informatica_lineage` and :func:`~airflow.providers.informatica.lineage.enable_informatica_lineage`.
- **Operator Exclusion**: Exclude entire operator classes via ``disabled_for_operators`` in ``airflow.cfg``.
- **Fail-fast Validation**: Unresolvable URIs or tables fail the task *before* execution begins, preventing silent lineage gaps.
- **EDC Integration**: Native REST API integration with Informatica Enterprise Data Catalog.
- **Configurable**: Extensive configuration options for different environments

Architecture
------------

The provider consists of several key components:

**Hooks**
    ``InformaticaEDCHook`` provides low-level EDC API access for authentication, object retrieval, and lineage creation.

**Extractors**
    ``InformaticaLineageExtractor`` handles lineage data extraction and conversion to Airflow-compatible formats.

**Plugins**
    ``InformaticaProviderPlugin`` registers listeners that monitor task lifecycle events and trigger lineage operations.

**Listeners**
    Event-driven listeners that respond to task success/failure events and process lineage information.


Requirements
------------

- Apache Airflow 3.0+
- Access to Informatica Enterprise Data Catalog instance
- Valid EDC credentials with API access permissions


Quick Start
-----------

1. **Install the provider:**

   .. code-block:: bash

      pip install apache-airflow-providers-informatica

2. **Configure connection:**

   Create an HTTP connection in Airflow UI with EDC server details and security domain in extras.

3. **Add lineage to tasks:**

   Define inlets and outlets in your tasks using EDC object URIs.

4. **Run your DAG:**

   The provider automatically handles lineage extraction when tasks succeed.


Automatic SQL Lineage
---------------------

When ``auto_lineage_enabled = True`` (the default), the provider automatically detects SQL
operators and creates lineage without any explicit ``inlets``/``outlets`` declarations.

.. code-block:: python

   from airflow import DAG
   from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
   from datetime import datetime

   with DAG("my_sql_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
       transform = SQLExecuteQueryOperator(
           task_id="transform",
           conn_id="postgres_default",
           sql="INSERT INTO summary SELECT region, SUM(amount) FROM sales GROUP BY region",
       )

The provider parses the SQL, finds ``sales`` as the source and ``summary`` as the target,
resolves both against the Informatica catalog, and creates the lineage link on task success.

The SQL dialect is inferred automatically from the connection ID string (e.g., a connection
ID containing ``postgres`` maps to the PostgreSQL dialect, ``snowflake`` to Snowflake, etc.).

Manual Lineage
--------------

Define inlets and outlets explicitly using EDC object URIs.  These always take priority over
automatic SQL lineage.

.. code-block:: python

   from airflow import DAG
   from airflow.providers.standard.operators.python import PythonOperator
   from datetime import datetime


   def my_python_task(**kwargs): ...


   with DAG("my_dag", start_date=datetime(2024, 1, 1), schedule=None) as dag:
       task = PythonOperator(
           task_id="transform",
           python_callable=my_python_task,
           inlets=[{"dataset_uri": "edc://object/source_table_abc123"}],
           outlets=[{"dataset_uri": "edc://object/target_table_xyz789"}],
       )

When this task succeeds, the provider creates a lineage link between the source and target
objects in EDC.  URIs that cannot be resolved in the catalog fail the task before execution.

Selective Lineage Control
-------------------------

Use the helpers in :mod:`airflow.providers.informatica.lineage` to disable or re-enable
automatic lineage on individual tasks or entire DAGs:

.. code-block:: python

   from airflow.providers.informatica.lineage import (
       disable_informatica_lineage,
       enable_informatica_lineage,
   )

   with DAG("my_dag", ...) as dag:
       task_a = SomeSQLOperator(task_id="task_a", sql="SELECT * FROM orders", ...)
       task_b = SomeSQLOperator(task_id="task_b", sql="SELECT * FROM customers", ...)

       # Disable auto-lineage for task_a only
       disable_informatica_lineage(task_a)

       # Or disable for all tasks in the DAG
       disable_informatica_lineage(dag)

These helpers have no effect on manually declared inlets and outlets.

Supported Inlet/Outlet Formats
-------------------------------

Inlets and outlets can be defined as:

- String URIs: ``"edc://object/table_name"``
- Dictionary with dataset_uri: ``{"dataset_uri": "edc://object/table_name"}``

Both formats are resolved via the EDC ``GET /access/2/catalog/data/objects/{id}`` endpoint.
