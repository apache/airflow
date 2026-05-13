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

Google Knowledge Catalog Operators
==================================

Knowledge Catalog is an intelligent data fabric that provides unified analytics
and data management across your data lakes, data warehouses, and data marts.

For more information about the task visit `Knowledge Catalog production documentation <Product documentation <https://cloud.google.com/dataplex/docs/reference>`__

Create a Task
-------------

Before you create a dataplex task you need to define its body.
For more information about the available fields to pass when creating a task, visit `Knowledge Catalog create task API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.tasks#Task>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_configuration]
    :end-before: [END howto_dataplex_configuration]

With this configuration we can create the task both synchronously & asynchronously:
:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateTaskOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateTaskOperator``.

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

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteTaskOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_task_operator]
    :end-before: [END howto_dataplex_delete_task_operator]

List tasks
----------

To list tasks you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexListTasksOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogListTasksOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_list_tasks_operator]
    :end-before: [END howto_dataplex_list_tasks_operator]

Get a task
----------

To get a task you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetTaskOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetTaskOperator``.

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

For more information about the available fields to pass when creating a lake, visit `Knowledge Catalog create lake API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes#Lake>`__

A simple task configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_lake_configuration]
    :end-before: [END howto_dataplex_lake_configuration]

With this configuration we can create the lake:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateLakeOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateLakeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_lake_operator]
    :end-before: [END howto_dataplex_create_lake_operator]

Delete a lake
-------------

To delete a lake you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteLakeOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteLakeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_lake_operator]
    :end-before: [END howto_dataplex_delete_lake_operator]

Create or update a Data Quality scan
------------------------------------

Before you create a Knowledge Catalog Data Quality scan you need to define its body.
For more information about the available fields to pass when creating a Data Quality scan, visit `Knowledge Catalog create data quality API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Quality scan configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_quality_configuration]
    :end-before: [END howto_dataplex_data_quality_configuration]

With this configuration we can create or update the Data Quality scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataQualityScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateOrUpdateDataQualityScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_quality_operator]
    :end-before: [END howto_dataplex_create_data_quality_operator]

Get a Data Quality scan
-----------------------

To get a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataQualityScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetDataQualityScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_quality_operator]
    :end-before: [END howto_dataplex_get_data_quality_operator]



Delete a Data Quality scan
--------------------------

To delete a Data Quality scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataQualityScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteDataQualityScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_quality_operator]
    :end-before: [END howto_dataplex_delete_data_quality_operator]

Run a Data Quality scan
-----------------------

You can run Knowledge Catalog Data Quality scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataQualityScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogRunDataQualityScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_quality_operator]
    :end-before: [END howto_dataplex_run_data_quality_operator]

To check that running Knowledge Catalog Data Quality scan succeeded you can use:

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

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetDataQualityScanResultOperator``.

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

Before you create a Knowledge Catalog zone you need to define its body.

For more information about the available fields to pass when creating a zone, visit `Knowledge Catalog create zone API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones#Zone>`__

A simple zone configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_zone_configuration]
    :end-before: [END howto_dataplex_zone_configuration]

With this configuration we can create a zone:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateZoneOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateZoneOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_zone_operator]
    :end-before: [END howto_dataplex_create_zone_operator]

Delete a zone
-------------

To delete a zone you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteZoneOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteZoneOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_zone_operator]
    :end-before: [END howto_dataplex_delete_zone_operator]

Create an asset
---------------

Before you create a Knowledge Catalog asset you need to define its body.

For more information about the available fields to pass when creating an asset, visit `Knowledge Catalog create asset API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.lakes.zones.assets#Asset>`__

A simple asset configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_asset_configuration]
    :end-before: [END howto_dataplex_asset_configuration]

With this configuration we can create the asset:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateAssetOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateAssetOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_asset_operator]
    :end-before: [END howto_dataplex_create_asset_operator]

Delete an asset
---------------

To delete an asset you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteAssetOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteAssetOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dq.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_asset_operator]
    :end-before: [END howto_dataplex_delete_asset_operator]

Create or update a Data Profile scan
------------------------------------

Before you create a Knowledge Catalog Data Profile scan you need to define its body.
For more information about the available fields to pass when creating a Data Profile scan, visit `Knowledge Catalog create data profile API. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.dataScans#DataScan>`__

A simple Data Profile scan configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_data_profile_configuration]
    :end-before: [END howto_dataplex_data_profile_configuration]

With this configuration we can create or update the Data Profile scan:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCreateOrUpdateDataProfileScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateOrUpdateDataProfileScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_create_data_profile_operator]
    :end-before: [END howto_dataplex_create_data_profile_operator]

Get a Data Profile scan
-----------------------

To get a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexGetDataProfileScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetDataProfileScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_operator]
    :end-before: [END howto_dataplex_get_data_profile_operator]



Delete a Data Profile scan
--------------------------

To delete a Data Profile scan you can use:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexDeleteDataProfileScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteDataProfileScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_delete_data_profile_operator]
    :end-before: [END howto_dataplex_delete_data_profile_operator]

Run a Data Profile scan
-----------------------

You can run Knowledge Catalog Data Profile scan in asynchronous modes to later check its status using sensor:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexRunDataProfileScanOperator`

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogRunDataProfileScanOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_run_data_profile_operator]
    :end-before: [END howto_dataplex_run_data_profile_operator]

To check that running Knowledge Catalog Data Profile scan succeeded you can use:

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

The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetDataProfileScanResultOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_dp.py
    :language: python
    :dedent: 4
    :start-after: [START howto_dataplex_get_data_profile_job_operator]
    :end-before: [END howto_dataplex_get_data_profile_job_operator]


Google Knowledge Catalog Entry Operators
========================================

Knowledge Catalog provides a unified inventory of Google Cloud resources, such as BigQuery, and other resources,
such as on-premises resources. Knowledge Catalog automatically retrieves metadata for Google Cloud resources,
and you bring metadata for third-party resources into Knowledge Catalog.

For more information about Knowledge Catalog visit `Knowledge Catalog production documentation <Product documentation <https://cloud.google.com/dataplex/docs/catalog-overview>`__

.. _howto/operator:DataplexCatalogCreateEntryGroupOperator:

Create an EntryGroup
--------------------

To create an Entry Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryGroupOperator``.
For more information about the available fields to pass when creating an Entry Group, visit `Entry Group resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups#EntryGroup>`__

A simple Entry Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_group_configuration]
    :end-before: [END howto_dataplex_entry_group_configuration]

With this configuration you can create an Entry Group resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryGroupOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryGroupOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_create_entry_group]

.. _howto/operator:DataplexCatalogDeleteEntryGroupOperator:

Delete an EntryGroup
--------------------

To delete an Entry Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryGroupOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteEntryGroupOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry_group]

.. _howto/operator:DataplexCatalogListEntryGroupsOperator:

List EntryGroups
----------------

To list all Entry Groups in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntryGroupsOperator`.
This operator also supports filtering and ordering the result of the operation.
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogListEntryGroupsOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entry_groups]
    :end-before: [END howto_operator_dataplex_catalog_list_entry_groups]

.. _howto/operator:DataplexCatalogGetEntryGroupOperator:

Get an EntryGroup
-----------------

To retrieve an Entry Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryGroupOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetEntryGroupOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_get_entry_group]

.. _howto/operator:DataplexCatalogUpdateEntryGroupOperator:

Update an EntryGroup
--------------------

To update an Entry Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryGroupOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogUpdateEntryGroupOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry_group]
    :end-before: [END howto_operator_dataplex_catalog_update_entry_group]

.. _howto/operator:DataplexCatalogCreateEntryTypeOperator:

Create an EntryType
--------------------

To create an Entry Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryTypeOperator``.
For more information about the available fields to pass when creating an Entry Type, visit `Entry Type resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryTypes#EntryType>`__

A simple Entry Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_type_configuration]
    :end-before: [END howto_dataplex_entry_type_configuration]

With this configuration you can create an Entry Type resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_create_entry_type]

.. _howto/operator:DataplexCatalogDeleteEntryTypeOperator:

Delete an EntryType
--------------------

To delete an Entry Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteEntryTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry_type]

.. _howto/operator:DataplexCatalogListEntryTypesOperator:

List EntryTypes
----------------

To list all Entry Types in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntryTypesOperator`.
This operator also supports filtering and ordering the result of the operation.
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogListEntryTypesOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entry_types]
    :end-before: [END howto_operator_dataplex_catalog_list_entry_types]

.. _howto/operator:DataplexCatalogGetEntryTypeOperator:

Get an EntryType
-----------------

To retrieve an Entry Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetEntryTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_get_entry_type]

.. _howto/operator:DataplexCatalogUpdateEntryTypeOperator:

Update an EntryType
--------------------

To update an Entry Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogUpdateEntryTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry_type]
    :end-before: [END howto_operator_dataplex_catalog_update_entry_type]

.. _howto/operator:DataplexCatalogCreateAspectTypeOperator:

Create an AspectType
--------------------

To create an Aspect Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateAspectTypeOperator``.
For more information about the available fields to pass when creating an Aspect Type, visit `Aspect Type resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.aspectTypes#AspectType>`__

A simple Aspect Group configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_aspect_type_configuration]
    :end-before: [END howto_dataplex_aspect_type_configuration]

With this configuration you can create an Aspect Type resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateAspectTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateAspectTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_create_aspect_type]

.. _howto/operator:DataplexCatalogDeleteAspectTypeOperator:

Delete an AspectType
--------------------

To delete an Aspect Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteAspectTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteAspectTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_delete_aspect_type]

.. _howto/operator:DataplexCatalogListAspectTypesOperator:

List AspectTypes
----------------

To list all Aspect Types in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListAspectTypesOperator`.
This operator also supports filtering and ordering the result of the operation.
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogListAspectTypesOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_aspect_types]
    :end-before: [END howto_operator_dataplex_catalog_list_aspect_types]

.. _howto/operator:DataplexCatalogGetAspectTypeOperator:

Get an AspectType
-----------------

To retrieve an Aspect Group in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetAspectTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetAspectTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_get_aspect_type]

.. _howto/operator:DataplexCatalogUpdateAspectTypeOperator:

Update an AspectType
--------------------

To update an Aspect Type in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateAspectTypeOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogUpdateAspectTypeOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_aspect_type]
    :end-before: [END howto_operator_dataplex_catalog_update_aspect_type]

.. _howto/operator:DataplexCatalogCreateEntryOperator:

Create an Entry
---------------

To create an Entry in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryOperator``.
For more information about the available fields to pass when creating an Entry, visit `Entry resource configuration. <https://cloud.google.com/dataplex/docs/reference/rest/v1/projects.locations.entryGroups.entries>`__

A simple Entry configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 0
    :start-after: [START howto_dataplex_entry_configuration]
    :end-before: [END howto_dataplex_entry_configuration]

With this configuration you can create an Entry resource:

:class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogCreateEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogCreateEntryOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_create_entry]
    :end-before: [END howto_operator_dataplex_catalog_create_entry]

.. _howto/operator:DataplexCatalogDeleteEntryOperator:

Delete an Entry
---------------

To delete an Entry in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogDeleteEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogDeleteEntryOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_delete_entry]
    :end-before: [END howto_operator_dataplex_catalog_delete_entry]

.. _howto/operator:DataplexCatalogListEntriesOperator:

List Entries
------------

To list all Entries in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogListEntriesOperator`.
This operator also supports filtering and ordering the result of the operation.
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogListEntriesOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_list_entries]
    :end-before: [END howto_operator_dataplex_catalog_list_entries]

.. _howto/operator:DataplexCatalogGetEntryOperator:

Get an Entry
------------

To retrieve an Entry in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogGetEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogGetEntryOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_get_entry]
    :end-before: [END howto_operator_dataplex_catalog_get_entry]

.. _howto/operator:DataplexCatalogUpdateEntryOperator:

Update an Entry
---------------

To update an Entry in a specific Knowledge Catalog location you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogUpdateEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogUpdateEntryOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_update_entry]
    :end-before: [END howto_operator_dataplex_catalog_update_entry]

.. _howto/operator:DataplexCatalogLookupEntryOperator:

Look up a single Entry
----------------------

To look up a single Entry by name using the permission on the source system in Knowledge Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogLookupEntryOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogLookupEntryOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_lookup_entry]
    :end-before: [END howto_operator_dataplex_catalog_lookup_entry]

.. _howto/operator:DataplexCatalogSearchEntriesOperator:

Search Entries
--------------

To search for Entries matching the given query and scope in Knowledge Catalog you can
use :class:`~airflow.providers.google.cloud.operators.dataplex.DataplexCatalogSearchEntriesOperator`
The executable example below still imports the compatibility name shown above.
The preferred alias for new code is ``KnowledgeCatalogSearchEntriesOperator``.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataplex/example_dataplex_catalog.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_dataplex_catalog_search_entry]
    :end-before: [END howto_operator_dataplex_catalog_search_entry]
