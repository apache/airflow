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

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_configuration]
    :end-before: [END howto_dataplex_configuration]

With this configuration we can create the task both synchronously & asynchronously:
:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateTaskOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_task_operator]
    :end-before: [END howto_dataplex_create_task_operator]

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_async_create_task_operator]
    :end-before: [END howto_dataplex_async_create_task_operator]

Delete a task
-------------

To delete a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteTaskOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_task_operator]
    :end-before: [END howto_dataplex_delete_task_operator]

List tasks
----------

To list tasks you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexListTasksOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_list_tasks_operator]
    :end-before: [END howto_dataplex_list_tasks_operator]

Get a task
----------

To get a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetTaskOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_task_operator]
    :end-before: [END howto_dataplex_get_task_operator]

Wait for a task
---------------

To wait for a task created asynchronously you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexTaskStateSensor`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_task_state_sensor]
    :end-before: [END howto_dataplex_task_state_sensor]

Create a Lake
-------------

Before you create a dataplex lake you need to define its body.

For more information about the available fields to pass when creating a lake, visit `Dataplex create lake API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes#Lake>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_lake_configuration]
    :end-before: [END howto_dataplex_lake_configuration]

With this configuration we can create the lake:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateLakeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_lake_operator]
    :end-before: [END howto_dataplex_create_lake_operator]

Delete a lake
-------------

To delete a lake you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteLakeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_lake_operator]
    :end-before: [END howto_dataplex_delete_lake_operator]

Create or update a Data Quality scan
------------------------------------

Before you create a Dataplex Data Quality scan you need to define its body.
For more information about the available fields to pass when creating a Data Quality scan, visit `Dataplex create data quality API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Quality scan configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_quality_configuration]
    :end-before: [END howto_dataplex_data_quality_configuration]

With this configuration we can create or update the Data Quality scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataQualityScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_quality_operator]
    :end-before: [END howto_dataplex_create_data_quality_operator]

Get a Data Quality scan
-----------------------

To get a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataQualityScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_operator]
    :end-before: [END howto_dataplex_get_data_quality_operator]



Delete a Data Quality scan
--------------------------

To delete a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataQualityScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_quality_operator]
    :end-before: [END howto_dataplex_delete_data_quality_operator]

Run a Data Quality scan
-----------------------

You can run Dataplex Data Quality scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataQualityScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_quality_operator]
    :end-before: [END howto_dataplex_run_data_quality_operator]

To check that running Dataplex Data Quality scan succeeded you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexDataQualityJobStatusSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_data_scan_job_state_sensor]
    :end-before: [END howto_dataplex_data_scan_job_state_sensor]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_quality_def_operator]
    :end-before: [END howto_dataplex_run_data_quality_def_operator]

Get a Data Quality scan job
---------------------------

To get a Data Quality scan job you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataQualityScanResultOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_job_operator]
    :end-before: [END howto_dataplex_get_data_quality_job_operator]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_job_def_operator]
    :end-before: [END howto_dataplex_get_data_quality_job_def_operator]

Create a zone
-------------

Before you create a Dataplex zone you need to define its body.

For more information about the available fields to pass when creating a zone, visit `Dataplex create zone API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones#Zone>`__

A simple zone configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_zone_configuration]
    :end-before: [END howto_dataplex_zone_configuration]

With this configuration we can create a zone:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateZoneOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_zone_operator]
    :end-before: [END howto_dataplex_create_zone_operator]

Delete a zone
-------------

To delete a zone you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteZoneOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_zone_operator]
    :end-before: [END howto_dataplex_delete_zone_operator]

Create an asset
---------------

Before you create a Dataplex asset you need to define its body.

For more information about the available fields to pass when creating an asset, visit `Dataplex create asset API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones.assets#Asset>`__

A simple asset configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_asset_configuration]
    :end-before: [END howto_dataplex_asset_configuration]

With this configuration we can create the asset:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateAssetOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_asset_operator]
    :end-before: [END howto_dataplex_create_asset_operator]

Delete an asset
---------------

To delete an asset you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteAssetOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_asset_operator]
    :end-before: [END howto_dataplex_delete_asset_operator]

Create or update a Data Profile scan
------------------------------------

Before you create a Dataplex Data Profile scan you need to define its body.
For more information about the available fields to pass when creating a Data Profile scan, visit `Dataplex create data profile API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Profile scan configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_profile_configuration]
    :end-before: [END howto_dataplex_data_profile_configuration]

With this configuration we can create or update the Data Profile scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataProfileScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_profile_operator]
    :end-before: [END howto_dataplex_create_data_profile_operator]

Get a Data Profile scan
-----------------------

To get a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataProfileScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_operator]
    :end-before: [END howto_dataplex_get_data_profile_operator]



Delete a Data Profile scan
--------------------------

To delete a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataProfileScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_profile_operator]
    :end-before: [END howto_dataplex_delete_data_profile_operator]

Run a Data Profile scan
-----------------------

You can run Dataplex Data Profile scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataProfileScanOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_profile_operator]
    :end-before: [END howto_dataplex_run_data_profile_operator]

To check that running Dataplex Data Profile scan succeeded you can use:

:class:`~airflow.providers.google.cloud.sensors.dataplex.DataplexDataProfileJobStatusSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_data_scan_job_state_sensor]
    :end-before: [END howto_dataplex_data_scan_job_state_sensor]

Also for this action you can use operator in the deferrable mode:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_profile_def_operator]
    :end-before: [END howto_dataplex_run_data_profile_def_operator]

Get a Data Profile scan job
---------------------------

To get a Data Profile scan job you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataProfileScanResultOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_job_operator]
    :end-before: [END howto_dataplex_get_data_profile_job_operator]


Google Dataplex Catalog Operators
=================================

Dataplex Catalog provides a unified inventory of Google Cloud resources, such as BigQuery, and other resources,
such as on-premises resources. Dataplex Catalog automatically retrieves metadata for Google Cloud resources,
and you bring metadata for third-party resources into Dataplex Catalog.

For more information about Dataplex Catalog visit `Dataplex Catalog production documentation <Product documentation <https://cloud.google.com/dataplex/docs/catalog-overview>`__

.. _howto/operator:DataplexCatalogCreateEntryGroupOperator:

Create an EntryGroup
--------------------

To create an Entry Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`
For more information about the available fields to pass when creating an Entry Group, visit `Entry Group resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup>`__

A simple Entry Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_group_configuration]
    :end-before: [END howto_dataplex_entry_group_configuration]

With this configuration you can create an Entry Group resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_create_entry_group]

.. _howto/operator:DataplexCatalogDeleteEntryGroupOperator:

Delete an EntryGroup
--------------------

To delete an Entry Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryGroupOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry_group]

.. _howto/operator:DataplexCatalogListEntryGroupsOperator:

List EntryGroups
----------------

To list all Entry Groups in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntryGroupsOperator`.
This operator also supports filtering and ordering the result of the operation.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entry_groups]
    :end-before: [END howto_operator_dataplex_catalog_list_entry_groups]

.. _howto/operator:DataplexCatalogGetEntryGroupOperator:

Get an EntryGroup
-----------------

To retrieve an Entry Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryGroupOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_get_entry_group]

.. _howto/operator:DataplexCatalogUpdateEntryGroupOperator:

Update an EntryGroup
--------------------

To update an Entry Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryGroupOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_update_entry_group]

.. _howto/operator:DataplexCatalogCreateEntryTypeOperator:

Create an EntryType
--------------------

To create an Entry Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryTypeOperator`
For more information about the available fields to pass when creating an Entry Type, visit `Entry Type resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryTypes#EntryType>`__

A simple Entry Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_type_configuration]
    :end-before: [END howto_dataplex_entry_type_configuration]

With this configuration you can create an Entry Type resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_create_entry_type]

.. _howto/operator:DataplexCatalogDeleteEntryTypeOperator:

Delete an EntryType
--------------------

To delete an Entry Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry_type]

.. _howto/operator:DataplexCatalogListEntryTypesOperator:

List EntryTypes
----------------

To list all Entry Types in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntryTypesOperator`.
This operator also supports filtering and ordering the result of the operation.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entry_types]
    :end-before: [END howto_operator_dataplex_catalog_list_entry_types]

.. _howto/operator:DataplexCatalogGetEntryTypeOperator:

Get an EntryType
-----------------

To retrieve an Entry Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_get_entry_type]

.. _howto/operator:DataplexCatalogUpdateEntryTypeOperator:

Update an EntryType
--------------------

To update an Entry Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_update_entry_type]

.. _howto/operator:DataplexCatalogCreateAspectTypeOperator:

Create an AspectType
--------------------

To create an Aspect Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`
For more information about the available fields to pass when creating an Aspect Type, visit `Aspect Type resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.aspectTypes#AspectType>`__

A simple Aspect Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_aspect_type_configuration]
    :end-before: [END howto_dataplex_aspect_type_configuration]

With this configuration you can create an Aspect Type resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_create_aspect_type]

.. _howto/operator:DataplexCatalogDeleteAspectTypeOperator:

Delete an AspectType
--------------------

To delete an Aspect Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteAspectTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_delete_aspect_type]

.. _howto/operator:DataplexCatalogListAspectTypesOperator:

List AspectTypes
----------------

To list all Aspect Types in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListAspectTypesOperator`.
This operator also supports filtering and ordering the result of the operation.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_aspect_types]
    :end-before: [END howto_operator_dataplex_catalog_list_aspect_types]

.. _howto/operator:DataplexCatalogGetAspectTypeOperator:

Get an AspectType
-----------------

To retrieve an Aspect Group in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetAspectTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_get_aspect_type]

.. _howto/operator:DataplexCatalogUpdateAspectTypeOperator:

Update an AspectType
--------------------

To update an Aspect Type in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_update_aspect_type]

.. _howto/operator:DataplexCatalogCreateEntryOperator:

Create an Entry
---------------

To create an Entry in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`
For more information about the available fields to pass when creating an Entry, visit `Entry resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups.entries>`__

A simple Entry configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_configuration]
    :end-before: [END howto_dataplex_entry_configuration]

With this configuration you can create an Entry resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry]
    :end-before: [END howto_operator_dataplex_catalog_create_entry]

.. _howto/operator:DataplexCatalogDeleteEntryOperator:

Delete an Entry
---------------

To delete an Entry in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry]

.. _howto/operator:DataplexCatalogListEntriesOperator:

List Entries
------------

To list all Entries in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntriesOperator`.
This operator also supports filtering and ordering the result of the operation.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entries]
    :end-before: [END howto_operator_dataplex_catalog_list_entries]

.. _howto/operator:DataplexCatalogGetEntryOperator:

Get an Entry
------------

To retrieve an Entry in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry]
    :end-before: [END howto_operator_dataplex_catalog_get_entry]

.. _howto/operator:DataplexCatalogUpdateEntryOperator:

Update an Entry
---------------

To update an Entry in specific location in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry]
    :end-before: [END howto_operator_dataplex_catalog_update_entry]

.. _howto/operator:DataplexCatalogLookupEntryOperator:

Look up a single Entry
----------------------

To look up a single Entry by name using the permission on the source system in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_lookup_entry]
    :end-before: [END howto_operator_dataplex_catalog_lookup_entry]

.. _howto/operator:DataplexCatalogSearchEntriesOperator:

Search Entries
--------------

To search for Entries matching the given query and scope in Dataplex Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogSearchEntriesOperator`

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_search_entry]
    :end-before: [END howto_operator_dataplex_catalog_search_entry]
