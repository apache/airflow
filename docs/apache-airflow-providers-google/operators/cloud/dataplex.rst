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

Google Dataplex Operators
=========================

Dataplex is an intelligent data fabric that provides unified analytics
and data management across your data lakes, data warehouses, and data marts.

For more information about the task visit `Dataplex production documentation <Product documentation <https://cloud.google.com/dataplex/docs/reference>`__

Create a Task
-------------

Before you create a dataplex task you need to define its body.
For more information about the available fields to pass when creating a task, visit `Dataplex create task API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.tasks#Task>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_configuration]
    :end-before: [END howto_dataplex_configuration]

With this configuration we can create the task both synchronously & asynchronously:
:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateTaskOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_task_operator]
    :end-before: [END howto_dataplex_create_task_operator]

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_async_create_task_operator]
    :end-before: [END howto_dataplex_async_create_task_operator]

Delete a task
-------------

To delete a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteTaskOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_task_operator]
    :end-before: [END howto_dataplex_delete_task_operator]

List tasks
----------

To list tasks you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexListTasksOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_list_tasks_operator]
    :end-before: [END howto_dataplex_list_tasks_operator]

Get a task
----------

To get a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetTaskOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_task_operator]
    :end-before: [END howto_dataplex_get_task_operator]

Wait for a task
---------------

To wait for a task created asynchronously you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexTaskStateSensor`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_task_state_sensor]
    :end-before: [END howto_dataplex_task_state_sensor]

Create a Lake
-------------

Before you create a dataplex lake you need to define its body.

For more information about the available fields to pass when creating a lake, visit `Dataplex create lake API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes#Lake>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_lake_configuration]
    :end-before: [END howto_dataplex_lake_configuration]

With this configuration we can create the lake:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateLakeOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_lake_operator]
    :end-before: [END howto_dataplex_create_lake_operator]

Delete a lake
-------------

To delete a lake you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteLakeOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_lake_operator]
    :end-before: [END howto_dataplex_delete_lake_operator]

Create or update a Data Quality scan
------------------------------------

Before you create a Dataplex Data Quality scan you need to define its body.
For more information about the available fields to pass when creating a Data Quality scan, visit `Dataplex create data quality API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Quality scan configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_quality_configuration]
    :end-before: [END howto_dataplex_data_quality_configuration]

With this configuration we can create or update the Data Quality scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataQualityScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_quality_operator]
    :end-before: [END howto_dataplex_create_data_quality_operator]

Get a Data Quality scan
-----------------------

To get a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataQualityScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_operator]
    :end-before: [END howto_dataplex_get_data_quality_operator]



Delete a Data Quality scan
--------------------------

To delete a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataQualityScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_quality_operator]
    :end-before: [END howto_dataplex_delete_data_quality_operator]

Run a Data Quality scan
-----------------------

You can run Dataplex Data Quality scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataQualityScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_quality_operator]
    :end-before: [END howto_dataplex_run_data_quality_operator]

To check that running Dataplex Data Quality scan succeeded you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexDataQualityJobStatusSensor`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_data_scan_job_state_sensor]
    :end-before: [END howto_dataplex_data_scan_job_state_sensor]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_quality_def_operator]
    :end-before: [END howto_dataplex_run_data_quality_def_operator]

Get a Data Quality scan job
---------------------------

To get a Data Quality scan job you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataQualityScanResultOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_job_operator]
    :end-before: [END howto_dataplex_get_data_quality_job_operator]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_job_def_operator]
    :end-before: [END howto_dataplex_get_data_quality_job_def_operator]

Create a zone
-------------

Before you create a Dataplex zone you need to define its body.

For more information about the available fields to pass when creating a zone, visit `Dataplex create zone API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones#Zone>`__

A simple zone configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_zone_configuration]
    :end-before: [END howto_dataplex_zone_configuration]

With this configuration we can create a zone:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateZoneOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_zone_operator]
    :end-before: [END howto_dataplex_create_zone_operator]

Delete a zone
-------------

To delete a zone you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteZoneOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_zone_operator]
    :end-before: [END howto_dataplex_delete_zone_operator]

Create a asset
--------------

Before you create a Dataplex asset you need to define its body.

For more information about the available fields to pass when creating a asset, visit `Dataplex create asset API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones.assets#Asset>`__

A simple asset configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_asset_configuration]
    :end-before: [END howto_dataplex_asset_configuration]

With this configuration we can create the asset:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateAssetOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_asset_operator]
    :end-before: [END howto_dataplex_create_asset_operator]

Delete a asset
--------------

To delete a asset you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteAssetOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_asset_operator]
    :end-before: [END howto_dataplex_delete_asset_operator]

Create or update a Data Profile scan
------------------------------------

Before you create a Dataplex Data Profile scan you need to define its body.
For more information about the available fields to pass when creating a Data Profile scan, visit `Dataplex create data profile API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Profile scan configuration can look as followed:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_profile_configuration]
    :end-before: [END howto_dataplex_data_profile_configuration]

With this configuration we can create or update the Data Profile scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataProfileScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_profile_operator]
    :end-before: [END howto_dataplex_create_data_profile_operator]

Get a Data Profile scan
-----------------------

To get a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataProfileScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_operator]
    :end-before: [END howto_dataplex_get_data_profile_operator]



Delete a Data Profile scan
--------------------------

To delete a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataProfileScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_profile_operator]
    :end-before: [END howto_dataplex_delete_data_profile_operator]

Run a Data Profile scan
-----------------------

You can run Dataplex Data Profile scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataProfileScanOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_profile_operator]
    :end-before: [END howto_dataplex_run_data_profile_operator]

To check that running Dataplex Data Profile scan succeeded you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexDataProfileJobStatusSensor`.

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_data_scan_job_state_sensor]
    :end-before: [END howto_dataplex_data_scan_job_state_sensor]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_profile_def_operator]
    :end-before: [END howto_dataplex_run_data_profile_def_operator]

Get a Data Profile scan job
---------------------------

To get a Data Profile scan job you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataProfileScanResultOperator`

.. exampleinclude:: /../../providers/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_job_operator]
    :end-before: [END howto_dataplex_get_data_profile_job_operator]
