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
Airflow connection of type ``wasb`` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection ``wasb_default`` for an example).

The operators are defined in the following module:

* :mod:`airflow.contrib.sensors.wasb_sensor`
* :mod:`airflow.contrib.operators.wasb_delete_blob_operator`
* :mod:`airflow.contrib.operators.file_to_wasb`

They use :class:`airflow.contrib.hooks.wasb_hook.WasbHook` to communicate with Microsoft Azure.

Azure File Share
''''''''''''''''

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type ``wasb`` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection ``wasb_default`` for an example).

It uses :class:`airflow.contrib.hooks.azure_fileshare_hook.AzureFileShareHook` to communicate with Microsoft Azure.

Azure CosmosDB
''''''''''''''

AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
Airflow connection of type ``azure_cosmos`` exists. Authorization can be done by supplying a
login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify the
default database and collection to use (see connection ``azure_cosmos_default`` for an example).

The operators are defined in the following modules:

* :mod:`airflow.contrib.operators.azure_cosmos_operator`
* :mod:`airflow.contrib.sensors.azure_cosmos_sensor`

They also use :class:`airflow.contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook` to communicate with Microsoft Azure.

Azure Data Lake
'''''''''''''''

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type ``azure_data_lake`` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection ``azure_data_lake_default`` for an example).

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

All hooks is based on :class:`airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`.


BigQuery
''''''''

The operators are defined in the following module:

 * :mod:`airflow.contrib.operators.bigquery_check_operator`
 * :mod:`airflow.contrib.operators.bigquery_get_data`
 * :mod:`airflow.contrib.operators.bigquery_table_delete_operator`
 * :mod:`airflow.contrib.operators.bigquery_to_bigquery`
 * :mod:`airflow.contrib.operators.bigquery_to_gcs`

They also use :class:`airflow.contrib.hooks.bigquery_hook.BigQueryHook` to communicate with Google Cloud Platform.


Cloud Spanner
'''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_spanner_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_spanner_hook.CloudSpannerHook` to communicate with Google Cloud Platform.


Cloud SQL
'''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_sql_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook` and :class:`airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook` to communicate with Google Cloud Platform.


Cloud Bigtable
''''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_bigtable_operator` package.


They also use :class:`airflow.contrib.hooks.gcp_bigtable_hook.BigtableHook` to communicate with Google Cloud Platform.

Cloud Build
'''''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_cloud_build_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_cloud_build_hook.CloudBuildHook` to communicate with Google Cloud Platform.


Compute Engine
''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_compute_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_compute_hook.GceHook` to communicate with Google Cloud Platform.


Cloud Functions
'''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_function_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_function_hook.GcfHook` to communicate with Google Cloud Platform.


Cloud DataFlow
''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.dataflow_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook` to communicate with Google Cloud Platform.


Cloud DataProc
''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.dataproc_operator` package.


Cloud Datastore
'''''''''''''''

:class:`airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator`
    Export entities from Google Cloud Datastore to Cloud Storage.

:class:`airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator`
    Import entities from Cloud Storage to Google Cloud Datastore.

They also use :class:`airflow.contrib.hooks.datastore_hook.DatastoreHook` to communicate with Google Cloud Platform.


Cloud ML Engine
'''''''''''''''

:class:`airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator`
    Start a Cloud ML Engine batch prediction job.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineModelOperator`
    Manages a Cloud ML Engine model.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator`
    Start a Cloud ML Engine training job.

:class:`airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator`
    Manages a Cloud ML Engine model version.

The operators are defined in the :class:`airflow.contrib.operators.mlengine_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook` to communicate with Google Cloud Platform.

Cloud Storage
'''''''''''''

The operators are defined in the following module:

 * :mod:`airflow.contrib.operators.file_to_gcs`
 * :mod:`airflow.contrib.operators.gcs_acl_operator`
 * :mod:`airflow.contrib.operators.gcs_download_operator`
 * :mod:`airflow.contrib.operators.gcs_list_operator`
 * :mod:`airflow.contrib.operators.gcs_operator`
 * :mod:`airflow.contrib.operators.gcs_to_bq`
 * :mod:`airflow.contrib.operators.gcs_to_gcs`
 * :mod:`airflow.contrib.operators.mysql_to_gcs`
 * :mod:`airflow.contrib.operators.mssql_to_gcs`
 * :mod:`airflow.contrib.sensors.gcs_sensor`
 * :mod:`airflow.contrib.operators.gcs_delete_operator`

They also use :class:`airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook` to communicate with Google Cloud Platform.


Transfer Service
''''''''''''''''


The operators are defined in the following module:

 * :mod:`airflow.contrib.operators.gcp_transfer_operator`
 * :mod:`airflow.contrib.sensors.gcp_transfer_operator`

They also use :class:`airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook` to communicate with Google Cloud Platform.


Cloud Vision
''''''''''''


The operator is defined in the :class:`airflow.contrib.operators.gcp_vision_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_vision_hook.CloudVisionHook` to communicate with Google Cloud Platform.

Cloud Text to Speech
''''''''''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_text_to_speech_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_text_to_speech_hook.GCPTextToSpeechHook` to communicate with Google Cloud Platform.

Cloud Speech to Text
''''''''''''''''''''

The operator is defined in the :class:`airflow.contrib.operators.gcp_speech_to_text_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook` to communicate with Google Cloud Platform.

Cloud Speech Translate Operators
--------------------------------

The operator is defined in the :class:`airflow.contrib.operators.gcp_translate_speech_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_speech_to_text_hook.GCPSpeechToTextHook` and
    :class:`airflow.contrib.hooks.gcp_translate_hook.CloudTranslateHook` to communicate with Google Cloud Platform.

Cloud Translate
'''''''''''''''

Cloud Translate Text Operators
""""""""""""""""""""""""""""""

:class:`airflow.contrib.operators.gcp_translate_operator.CloudTranslateTextOperator`
    Translate a string or list of strings.

The operator is defined in the :class:`airflow.contrib.operators.gcp_translate_operator` package.

Cloud Video Intelligence
''''''''''''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_video_intelligence_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_video_intelligence_hook.CloudVideoIntelligenceHook` to communicate with Google Cloud Platform.

Google Kubernetes Engine
''''''''''''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_container_operator` package.


They also use :class:`airflow.contrib.hooks.gcp_container_hook.GKEClusterHook` to communicate with Google Cloud Platform.


Google Natural Language
'''''''''''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_natural_language_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_natural_language_operator.CloudNaturalLanguageHook` to communicate with Google Cloud Platform.


Google Cloud Data Loss Prevention (DLP)
'''''''''''''''''''''''''''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_dlp_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_dlp_hook.CloudDLPHook` to communicate with Google Cloud Platform.


Google Cloud Tasks
''''''''''''''''''

The operators are defined in the :class:`airflow.contrib.operators.gcp_tasks_operator` package.

They also use :class:`airflow.contrib.hooks.gcp_tasks_hook.CloudTasksHook` to communicate with Google Cloud Platform.


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
