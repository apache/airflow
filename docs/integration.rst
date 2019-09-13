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

Integration
===========

.. contents:: Content
  :local:
  :depth: 1

.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for Microsoft Azure: interfaces exist only for Azure Blob
Storage and Azure Data Lake. Hook, Sensor and Operator for Blob Storage and
Azure Data Lake Hook are in contrib section.

Logging
'''''''

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.


Azure Blob Storage
''''''''''''''''''

All classes communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).

The operators are defined in the following module:

* :mod:`airflow.contrib.sensors.wasb_sensor`
* :mod:`airflow.contrib.operators.wasb_delete_blob_operator`
* :mod:`airflow.contrib.sensors.wasb_sensor`
* :mod:`airflow.contrib.operators.file_to_wasb`

They use :class:`airflow.contrib.hooks.wasb_hook.WasbHook` to communicate with Microsoft Azure.

Azure File Share
''''''''''''''''

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `wasb` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection `wasb_default` for an example).

It uses :class:`airflow.contrib.hooks.azure_fileshare_hook.AzureFileShareHook` to communicate with Microsoft Azure.

Azure CosmosDB
''''''''''''''

AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
Airflow connection of type `azure_cosmos` exists. Authorization can be done by supplying a
login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify the
default database and collection to use (see connection `azure_cosmos_default` for an example).

The operators are defined in the following modules:

* :mod:`airflow.contrib.operators.azure_cosmos_operator`
* :mod:`airflow.contrib.sensors.azure_cosmos_sensor`

They also use :class:`airflow.contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook` to communicate with Microsoft Azure.

Azure Data Lake
'''''''''''''''

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection `azure_data_lake_default` for an example).

The operators are defined in the following modules:

* :mod:`airflow.contrib.operators.adls_list_operator`
* :mod:`airflow.contrib.operators.adls_to_gcs`

They also use :class:`airflow.contrib.hooks.azure_data_lake_hook.AzureDataLakeHook` to communicate with Microsoft Azure.


Azure Container Instances
'''''''''''''''''''''''''

Azure Container Instances provides a method to run a docker container without having to worry
about managing infrastructure. The AzureContainerInstanceHook requires a service principal. The
credentials for this principal can either be defined in the extra field ``key_path``, as an
environment variable named ``AZURE_AUTH_LOCATION``,
or by providing a login/password and tenantId in extras.

The operator is defined in the :mod:`airflow.contrib.operators.azure_container_instances_operator` module.

They also use :class:`airflow.contrib.hooks.azure_container_volume_hook.AzureContainerVolumeHook`,
:class:`airflow.contrib.hooks.azure_container_registry_hook.AzureContainerRegistryHook` and
:class:`airflow.contrib.hooks.azure_container_instance_hook.AzureContainerInstanceHook` to communicate with Microsoft Azure.

The AzureContainerRegistryHook requires a host/login/password to be defined in the connection.


.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has extensive support for Amazon Web Services. But note that the Hooks, Sensors and
Operators are in the contrib section.

Logging
'''''''

Airflow can be configured to read and write task logs in Amazon Simple Storage Service (Amazon S3).
See :ref:`write-logs-amazon`.


AWS EMR
'''''''

The operators are defined in the following modules:

* :mod:`airflow.contrib.operators.emr_add_steps_operator`
* :mod:`airflow.contrib.operators.emr_create_job_flow_operator`
* :mod:`airflow.contrib.operators.emr_terminate_job_flow_operator`

They also use :class:`airflow.contrib.hooks.emr_hook.EmrHook` to communicate with Amazon Web Service.

AWS S3
''''''

The operators are defined in the following modules:

* :mod:`airflow.operators.s3_file_transform_operator`
* :mod:`airflow.contrib.operators.s3_list_operator`
* :mod:`airflow.contrib.operators.s3_to_gcs_operator`
* :mod:`airflow.contrib.operators.s3_to_gcs_transfer_operator`
* :mod:`airflow.operators.s3_to_hive_operator`

They also use :class:`airflow.hooks.S3_hook.S3Hook` to communicate with Amazon Web Service.

AWS Batch Service
'''''''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.awsbatch_operator.AWSBatchOperator` module.

AWS RedShift
''''''''''''

The operators are defined in the following modules:

* :mod:`airflow.contrib.sensors.aws_redshift_cluster_sensor`
* :mod:`airflow.operators.redshift_to_s3_operator`
* :mod:`airflow.operators.s3_to_redshift_operator`

They also use :class:`airflow.contrib.hooks.redshift_hook.RedshiftHook` to communicate with Amazon Web Service.


AWS DynamoDB
''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.hive_to_dynamodb` module.

It uses :class:`airflow.contrib.hooks.aws_dynamodb_hook.AwsDynamoDBHook` to communicate with Amazon Web Service.


AWS Lambda
''''''''''

It uses :class:`airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook` to communicate with Amazon Web Service.

AWS Kinesis
'''''''''''

It uses :class:`airflow.contrib.hooks.aws_firehose_hook.AwsFirehoseHook` to communicate with Amazon Web Service.


Amazon SageMaker
''''''''''''''''

For more instructions on using Amazon SageMaker in Airflow, please see `the SageMaker Python SDK README`_.

.. _the SageMaker Python SDK README: https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/README.rst

The operators are defined in the following modules:

:mod:`airflow.contrib.operators.sagemaker_training_operator`
:mod:`airflow.contrib.operators.sagemaker_tuning_operator`
:mod:`airflow.contrib.operators.sagemaker_model_operator`
:mod:`airflow.contrib.operators.sagemaker_transform_operator`
:mod:`airflow.contrib.operators.sagemaker_endpoint_config_operator`
:mod:`airflow.contrib.operators.sagemaker_endpoint_operator`

They uses :class:`airflow.contrib.hooks.sagemaker_hook.SageMakerHook` to communicate with Amazon Web Service.

.. _Databricks:

Databricks
----------

With contributions from `Databricks <https://databricks.com/>`__, Airflow has several operators
which enable the submitting and running of jobs to the Databricks platform. Internally the
operators talk to the ``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.

The operators are defined in the :class:`airflow.contrib.operators.databricks_operator` module.

.. _GCP:

GCP: Google Cloud Platform
--------------------------

Airflow has extensive support for the Google Cloud Platform. But note that most Hooks and
Operators are in the contrib section. Meaning that they have a *beta* status, meaning that
they can have breaking changes between minor releases.

See the :doc:`GCP connection type <howto/connection/gcp>` documentation to
configure connections to GCP.

Logging
'''''''

Airflow can be configured to read and write task logs in Google Cloud Storage.
See :ref:`write-logs-gcp`.


GoogleCloudBaseHook
'''''''''''''''''''

All hooks are based on :class:`airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`.


BigQuery
''''''''

The operators are defined in the following modules:
 * :mod:`airflow.gcp.operators.bigquery`
 * :mod:`airflow.gcp.sensors.bigquery`
 * :mod:`airflow.operators.bigquery_to_bigquery`
 * :mod:`airflow.operators.bigquery_to_gcs`
 * :mod:`airflow.operators.bigquery_to_mysql`

They also use :class:`airflow.gcp.hooks.bigquery.BigQueryHook` to communicate with Google Cloud Platform.

BigQuery Data Transfer Service
''''''''''''''''''''''''''''''
The operators are defined in the following module:

 * :mod:`airflow.gcp.operators.bigquery_dts`
 * :mod:`airflow.gcp.sensors.bigquery_dts`

The operator is defined in the :class:`airflow.gcp.operators.spanner` package.

They also use :class:`airflow.gcp.hooks.bigquery_dts.BiqQueryDataTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Spanner
'''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.spanner` package.

They also use :class:`airflow.gcp.hooks.spanner.CloudSpannerHook` to communicate with Google Cloud Platform.


Cloud SQL
'''''''''

The operator is defined in the :class:`airflow.gcp.operators.cloud_sql` package.

They also use :class:`airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook` and :class:`airflow.gcp.hooks.cloud_sql.CloudSqlHook` to communicate with Google Cloud Platform.


Cloud Bigtable
''''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.bigtable` package.

They also use :class:`airflow.gcp.hooks.bigtable.BigtableHook` to communicate with Google Cloud Platform.

Cloud Build
'''''''''''

The operator is defined in the :class:`airflow.gcp.operators.cloud_build` package.

They also use :class:`airflow.gcp.hooks.cloud_build.CloudBuildHook` to communicate with Google Cloud Platform.


Compute Engine
''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.compute` package.

They also use :class:`airflow.gcp.hooks.compute.GceHook` to communicate with Google Cloud Platform.


Cloud Functions
'''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.functions` package.

They also use :class:`airflow.gcp.hooks.functions.GcfHook` to communicate with Google Cloud Platform.


Cloud DataFlow
''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.dataflow` package.

They also use :class:`airflow.gcp.hooks.dataflow.DataFlowHook` to communicate with Google Cloud Platform.


Cloud DataProc
''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.dataproc` package.


Cloud Datastore
'''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.datastore` package.

They also use :class:`airflow.gcp.hooks.datastore.DatastoreHook` to communicate with Google Cloud Platform.


Cloud ML Engine
'''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.mlengine` package.

They also use :class:`airflow.gcp.hooks.mlengine.MLEngineHook` to communicate with Google Cloud Platform.

Cloud Storage
'''''''''''''

The operators are defined in the following module:

 * :mod:`airflow.operators.local_to_gcs`
 * :mod:`airflow.gcp.operators.gcs`
 * :mod:`airflow.operators.gcs_to_bq`
 * :mod:`airflow.operators.mysql_to_gcs`
 * :mod:`airflow.gcp.sensors.gcs`

They also use :class:`airflow.gcp.hooks.gcs.GoogleCloudStorageHook` to communicate with Google Cloud Platform.


Transfer Service
''''''''''''''''

The operators are defined in the following module:

 * :mod:`airflow.gcp.operators.cloud_storage_transfer_service`
 * :mod:`airflow.gcp.sensors.cloud_storage_transfer_service`

They also use :class:`airflow.gcp.hooks.cloud_storage_transfer_service.GCPTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Vision
''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.vision` package.

They also use :class:`airflow.gcp.hooks.vision.CloudVisionHook` to communicate with Google Cloud Platform.

Cloud Text to Speech
''''''''''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.text_to_speech` package.

They also use :class:`airflow.gcp.hooks.text_to_speech.GCPTextToSpeechHook` to communicate with Google Cloud Platform.

Cloud Speech to Text
''''''''''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.speech_to_text` package.

They also use :class:`airflow.gcp.hooks.speech_to_text.GCPSpeechToTextHook` to communicate with Google Cloud Platform.

Cloud Speech Translate
''''''''''''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.translate_speech` package.

They also use :class:`airflow.gcp.hooks.speech_to_text.GCPSpeechToTextHook` and
    :class:`airflow.gcp.hooks.translate.CloudTranslateHook` to communicate with Google Cloud Platform.

Cloud Translate
'''''''''''''''

The operator is defined in the :class:`airflow.gcp.operators.translate` package.

Cloud Video Intelligence
''''''''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.video_intelligence` package.

They also use :class:`airflow.gcp.hooks.video_intelligence.CloudVideoIntelligenceHook` to communicate with Google Cloud Platform.

Google Kubernetes Engine
''''''''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.kubernetes_engine` package.

They also use :class:`airflow.gcp.hooks.kubernetes_engine.GKEClusterHook` to communicate with Google Cloud Platform.


Google Natural Language
'''''''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.natural_language` package.

They also use :class:`airflow.gcp.hooks.natural_language.CloudNaturalLanguageHook` to communicate with Google Cloud Platform.


Google Cloud Data Loss Prevention (DLP)
'''''''''''''''''''''''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.dlp` package.

They also use :class:`airflow.gcp.hooks.dlp.CloudDLPHook` to communicate with Google Cloud Platform.


Google Cloud Tasks
''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.tasks` package.

They also use :class:`airflow.gcp.hooks.tasks.CloudTasksHook` to communicate with Google Cloud Platform.

Google Natural Language
'''''''''''''''''''''''

The operators are defined in the :class:`airflow.gcp.operators.automl` package.

They also use :class:`airflow.gcp.hooks.automl.CloudAutoMLHook` to communicate with Google Cloud Platform.

Google Discovery API
''''''''''''''''''''

Airflow also has support for requesting data from any Google Service via Google's Discovery API.

:class:`airflow.contrib.operators.google_api_to_s3_transfer.GoogleApiToS3Transfer`
    Transfers data from any Google Service into an AWS S3 Bucket.

They also use :class:`airflow.contrib.hooks.google_discovery_api_hook.GoogleDiscoveryApiHook` to communicate with
Google Services via the `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
Please note that this library is in maintenance mode. Therefore it is recommended that you use the custom GCP Service
Operators for working with the Google Cloud Platform.


.. _Qubole:

Qubole
------

Apache Airflow has a native operator and hooks to talk to `Qubole <https://qubole.com/>`__,
which lets you submit your big data jobs directly to Qubole from Apache Airflow.

The operators are defined in the following module:

 * :mod:`airflow.contrib.operators.qubole_operator`
 * :mod:`airflow.contrib.sensors.qubole_sensor`
 * :mod:`airflow.contrib.sensors.qubole_sensor`
 * :mod:`airflow.contrib.operators.qubole_check_operator`
