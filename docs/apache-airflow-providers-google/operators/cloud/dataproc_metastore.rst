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

Google Cloud Dataproc Metastore Operators
=========================================

Dataproc Metastore is a fully managed, highly available, auto-healing serverless
Apache Hive metastore (HMS) that runs on Google Cloud. It supports HMS, serves as
a critical component for managing the metadata of relational entities,
and provides interoperability between data processing applications in the open source data ecosystem.

For more information about the service visit `Dataproc Metastore production documentation <Product documentation <https://cloud.google.com/dataproc-metastore/docs/reference>`__

Create a Service
----------------

Before you create a dataproc metastore service you need to define the service.
For more information about the available fields to pass when creating a service, visit `Dataproc Metastore create service API. <https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services#Service>`__

A simple service configuration can look as followed:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_metastore_create_service]
    :end-before: [END how_to_cloud_dataproc_metastore_create_service]

With this configuration we can create the service:
:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreCreateServiceOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_create_service_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_create_service_operator]

Get a service
-------------

To get a service you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreGetServiceOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_get_service_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_get_service_operator]

Update a service
----------------
You can update the service by providing a service config and an updateMask.
In the updateMask argument you specifies the path, relative to Service, of the field to update.
For more information on updateMask and other parameters take a look at `Dataproc Metastore update service API. <https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services/patch>`__

An example of a new service config and the updateMask:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_metastore_update_service]
    :end-before: [END how_to_cloud_dataproc_metastore_update_service]

To update a service you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreUpdateServiceOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_update_service_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_update_service_operator]

Delete a service
----------------

To delete a service you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteServiceOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_delete_service_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_delete_service_operator]

Export a service metadata
-------------------------

To export metadata you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreExportMetadataOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_export_metadata_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_export_metadata_operator]

Restore a service
-----------------

To restore a service you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreRestoreServiceOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_restore_service_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_restore_service_operator]

Create a metadata import
------------------------

Before you create a dataproc metastore metadata import you need to define the metadata import.
For more information about the available fields to pass when creating a metadata import, visit `Dataproc Metastore create metadata import API. <https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services.metadataImports#MetadataImport>`__

A simple metadata import configuration can look as followed:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_metastore_create_metadata_import]
    :end-before: [END how_to_cloud_dataproc_metastore_create_metadata_import]

To create a metadata import you can use:
:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreCreateMetadataImportOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_create_metadata_import_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_create_metadata_import_operator]

Create a Backup
---------------

Before you create a dataproc metastore backup of the service you need to define the backup.
For more information about the available fields to pass when creating a backup, visit `Dataproc Metastore create backup API. <https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services.backups#Backup>`__

A simple backup configuration can look as followed:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 0
    :start-after: [START how_to_cloud_dataproc_metastore_create_backup]
    :end-before: [END how_to_cloud_dataproc_metastore_create_backup]

With this configuration we can create the backup:
:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreCreateBackupOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_create_backup_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_create_backup_operator]

Delete a backup
---------------

To delete a backup you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreDeleteBackupOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_delete_backup_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_delete_backup_operator]

List backups
------------

To list backups you can use:

:class:`~airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreListBackupsOperator`

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_dataproc_metastore.py
    :language: python
    :dedent: 4
    :start-after: [START how_to_cloud_dataproc_metastore_list_backups_operator]
    :end-before: [END how_to_cloud_dataproc_metastore_list_backups_operator]
