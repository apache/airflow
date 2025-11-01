
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

- **Automatic Lineage Detection**: Plugin automatically detects tasks with lineage support
- **EDC Integration**: Native REST API integration with Informatica Enterprise Data Catalog
- **Transparent Operation**: No code changes required beyond inlet/outlet definitions
- **Error Handling**: Robust error handling for API failures and invalid objects
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

**Operators**
    ``EmptyOperator``: A minimal operator included for basic lineage testing and integration. It supports inlets and outlets, allowing you to verify lineage extraction and EDC integration without custom logic.

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


Example DAG
-----------

.. code-block:: python

   from airflow import DAG
   from airflow.providers.standard.operators.python import PythonOperator
   from datetime import datetime


   def my_python_task(**kwargs):
       print("Hello Informatica Lineage!")


   with DAG(
       dag_id="example_informatica_lineage_dag",
       start_date=datetime(2024, 1, 1),
       schedule_interval=None,
       catchup=False,
   ) as dag:
       python_task = PythonOperator(
           task_id="my_python_task",
           python_callable=my_python_task,
           inlets=[{"dataset_uri": "edc://object/source_table_abc123"}],
           outlets=[{"dataset_uri": "edc://object/target_table_xyz789"}],
       )

When this task succeeds, the provider automatically creates a lineage link between the source and target objects in EDC.

Hooks
-----

InformaticaEDCHook
^^^^^^^^^^^^^^^^^^

The hook provides low-level access to Informatica EDC API.

.. code-block:: python

   from airflow.providers.informatica.hooks.edc import InformaticaEDCHook

   hook = InformaticaEDCHook(informatica_edc_conn_id="my_connection")
   object_data = hook.get_object("edc://object/table_123")
   result = hook.create_lineage_link("source_id", "target_id")

Operators
---------

Empty Operator
^^^^^^^^^^^^^^

The provider includes an empty operator for basic lineage testing.

.. code-block:: python

   from airflow.providers.informatica.operators import EmptyOperator

   empty_task = EmptyOperator(
       task_id="empty_task",
       inlets=[{"dataset_uri": "edc://object/test_table"}],
       outlets=[{"dataset_uri": "edc://object/result_table"}],
   )

Plugins and Listeners
---------------------

The ``InformaticaProviderPlugin`` automatically registers listeners that:

- Monitor task success events
- Extract inlet/outlet information from tasks
- Resolve object IDs using EDC API
- Create lineage links between resolved objects

No manual intervention is required. The plugin works transparently with any task that defines inlets and outlets.

Supported Inlet/Outlet Formats
------------------------------

Inlets and outlets can be defined as:

- String URIs: ``"edc://object/table_name"``
- Dictionary with dataset_uri: ``{"dataset_uri": "edc://object/table_name"}``

The plugin automatically handles both formats and resolves them to EDC object IDs.


Support
-------

- **Documentation**: See the guides section for detailed usage and configuration
- **Issues**: Report bugs on the Apache Airflow GitHub repository
- **Community**: Join the Airflow community for discussions and support

License
-------

Licensed under the Apache License, Version 2.0. See LICENSE file for details.
