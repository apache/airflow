..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

Integration
===========

.. contents:: Content
    :local:
    :depth: 2

.. _Azure:

Microsoft Azure
---------------

Logging
^^^^^^^

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.

Operators and hooks
^^^^^^^^^^^^^^^^^^^

Azure Blob Storage
''''''''''''''''''

All classes communicate via the Window Azure Storage Blob protocol. Make sure that a
Airflow connection of type `wasb` exists. Authorization can be done by supplying a
login (=Storage account name) and password (=KEY), or login and SAS token in the extra
field (see connection `wasb_default` for an example).

For further information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.wasb_hook`
* :py:mod:`airflow.contrib.sensors.wasb_sensor`
* :py:mod:`airflow.contrib.operators.wasb_delete_blob_operator`
* :py:mod:`airflow.contrib.operators.file_to_wasb`

Azure Container Instances
'''''''''''''''''''''''''

Azure Container Instances provides a method to run a docker container without having to worry
about managing infrastructure. The AzureContainerInstanceHook requires a service principal. The
credentials for this principal can either be defined in the extra field ``key_path``, as an
environment variable named ``AZURE_AUTH_LOCATION``,
or by providing a login/password and tenantId in extras.

The AzureContainerRegistryHook requires a host/login/password to be defined in the connection.

For further information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.azure_container_volume_hook`
* :py:mod:`airflow.contrib.operators.azure_container_instances_operator`
* :py:mod:`airflow.contrib.hooks.azure_container_instance_hook`
* :py:mod:`airflow.contrib.hooks.azure_container_registry_hook`

Azure CosmosDB
''''''''''''''

AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
Airflow connection of type `azure_cosmos` exists. Authorization can be done by supplying a
login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify the
default database and collection to use (see connection `azure_cosmos_default` for an example).

For further information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.azure_cosmos_hook`
* :py:mod:`airflow.contrib.operators.azure_cosmos_operator`
* :py:mod:`airflow.contrib.sensors.azure_cosmos_sensor`

Azure Data Lake
'''''''''''''''

AzureDataLakeHook communicates via a REST API compatible with WebHDFS. Make sure that a
Airflow connection of type `azure_data_lake` exists. Authorization can be done by supplying a
login (=Client ID), password (=Client Secret) and extra fields tenant (Tenant) and account_name (Account Name)
(see connection `azure_data_lake_default` for an example).

For further information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.azure_data_lake_hook`
* :py:mod:`airflow.contrib.operators.adls_list_operator`
* :py:mod:`airflow.contrib.operators.adls_to_gcs`

Azure File Share
''''''''''''''''

Cloud variant of a SMB file share. Make sure that a Airflow connection of
type `wasb` exists. Authorization can be done by supplying a login (=Storage account name)
and password (=Storage account key), or login and SAS token in the extra field
(see connection `wasb_default` for an example).

For further information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.azure_fileshare_hook`

.. _AWS:

Amazon Web Services
-------------------

Airflow has extensive support for Amazon Web Services.

Logging
^^^^^^^

Airflow can be configured to read and write task logs in Amazon Simple Storage Service (Amazon S3).
See :ref:`write-logs-amazon`.

Operators and hooks
^^^^^^^^^^^^^^^^^^^

Amazon DynamoDB
'''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.hive_to_dynamodb`
* :py:mod:`airflow.contrib.hooks.aws_dynamodb_hook`

Amazon EMR
''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.emr_hook`
* :py:mod:`airflow.contrib.operators.emr_add_steps_operator`
* :py:mod:`airflow.contrib.operators.emr_create_job_flow_operator`
* :py:mod:`airflow.contrib.operators.emr_terminate_job_flow_operator`

Amazon RedShift
'''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.sensors.aws_redshift_cluster_sensor`
* :py:mod:`airflow.contrib.hooks.redshift_hook`
* :py:mod:`airflow.operators.redshift_to_s3_operator`
* :py:mod:`airflow.operators.s3_to_redshift_operator`

Amazon SageMaker
''''''''''''''''

For instructions on using Amazon SageMaker in Airflow, please see `the SageMaker Python SDK README`_.

.. _the SageMaker Python SDK README: https://github.com/aws/sagemaker-python-sdk/blob/master/src/sagemaker/workflow/README.rst

Additional information is available at API Reference:

* :py:mod:`airflow.contrib.hooks.sagemaker_hook`
* :py:mod:`airflow.contrib.operators.sagemaker_training_operator`
* :py:mod:`airflow.contrib.operators.sagemaker_tuning_operator`
* :py:mod:`airflow.contrib.hooks.azure_container_registry_hook`
* :py:mod:`airflow.contrib.operators.sagemaker_model_operator`
* :py:mod:`airflow.contrib.operators.sagemaker_transform_operator`
* :py:mod:`airflow.contrib.operators.sagemaker_endpoint_config_operator`
* :py:mod:`airflow.contrib.operators.sagemaker_endpoint_operator`

Amazon Simple Storage Service (Amazon S3)
'''''''''''''''''''''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.hooks.S3_hook`
* :py:mod:`airflow.operators.s3_file_transform_operator`
* :py:mod:`airflow.contrib.operators.s3_list_operator`
* :py:mod:`airflow.contrib.operators.s3_to_gcs_operator`
* :py:class:`airflow.contrib.operators.gcp_transfer_operator.S3ToGoogleCloudStorageTransferOperator`
* :py:mod:`airflow.operators.s3_to_hive_operator.S3ToHiveTransfer`

Amazon Web Service Batch Service
''''''''''''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.awsbatch_operator`

Amazon Web Service Kinesis
''''''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.aws_firehose_hook`

Amazon Web Service Lambda
'''''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.hooks.aws_lambda_hook`

.. _Databricks:

Databricks
----------

`Databricks <https://databricks.com/>`__ has contributed an Airflow operator which enables
submitting runs to the Databricks platform. Internally the operator talks to the
``api/2.0/jobs/runs/submit`` `endpoint <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_.


For further information, look at API Reference:

* :py:mod:`airflow.contrib.operators.databricks_operator`

.. _GCP:

Google Cloud Platform
---------------------

Airflow has extensive support for the Google Cloud Platform.

See the :doc:`GCP connection type <howto/connection/gcp>` documentation to
configure connections to GCP.

Logging
^^^^^^^

Airflow can be configured to read and write task logs in Google Cloud Storage.
See :ref:`write-logs-gcp`.

Operators and hooks
^^^^^^^^^^^^^^^^^^^

All hooks is based on :class:`airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`.

Cloud Bigtable
''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_bigtable_operator`
* :py:mod:`airflow.contrib.hooks.gcp_bigtable_hook`

Cloud Dataflow
''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.dataflow_operator`
* :py:mod:`airflow.contrib.hooks.gcp_dataflow_hook`

Cloud Dataproc
''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.dataproc_operator`
* :py:mod:`airflow.contrib.hooks.gcp_dataproc_hook`

Cloud Datastore
'''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.datastore_export_operator`
* :py:mod:`airflow.contrib.hooks.datastore_hook`

Cloud Functions
'''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_function_operator`
* :py:mod:`airflow.contrib.hooks.gcp_function_hook`

Cloud Machine Learning (ML) Engine
''''''''''''''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.mlengine_operator`
* :py:mod:`airflow.contrib.hooks.gcp_mlengine_hook`

Cloud Natural Language
'''''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_natural_language_operator`
* :py:mod:`airflow.contrib.hooks.gcp_natural_language_hook`

Cloud Spanner
'''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_spanner_operator`
* :py:mod:`airflow.contrib.hooks.gcp_spanner_hook`

Cloud Speech-To-Text
''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_speech_to_text_operator`
* :py:mod:`airflow.contrib.hooks.gcp_speech_to_text_hook`

Cloud SQL
'''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_sql_operator`
* :py:mod:`airflow.contrib.hooks.gcp_sql_hook`

Cloud Storage
'''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.file_to_gcs`
* :py:mod:`airflow.contrib.operators.gcs_acl_operator`
* :py:mod:`airflow.contrib.operators.gcs_download_operator`
* :py:mod:`airflow.contrib.operators.gcs_list_operator`
* :py:mod:`airflow.contrib.operators.gcs_operator`
* :py:mod:`airflow.contrib.operators.gcs_to_bq`
* :py:mod:`airflow.contrib.operators.gcs_to_gcs`
* :py:mod:`airflow.contrib.operators.mysql_to_gcs`
* :py:mod:`airflow.contrib.hooks.gcs_hook`

Cloud Text-To-Speech
''''''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_text_to_speech_operator`
* :py:mod:`airflow.contrib.hooks.gcp_text_to_speech_hook`

Cloud Translation
'''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_translate_operator`
* :py:mod:`airflow.contrib.hooks.gcp_translate_hook`

Cloud Vision
''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_vision_operator`
* :py:mod:`airflow.contrib.hooks.gcp_vision_hook`

Compute Engine
''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_compute_operator`
* :py:mod:`airflow.contrib.hooks.gcp_compute_hook`

Google BigQuery
'''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.bigquery_check_operator`
* :py:mod:`airflow.contrib.operators.bigquery_get_data`
* :py:mod:`airflow.contrib.operators.bigquery_operator`
* :py:mod:`airflow.contrib.operators.bigquery_table_delete_operator`
* :py:mod:`airflow.contrib.operators.bigquery_to_bigquery`
* :py:mod:`airflow.contrib.operators.bigquery_to_gcs`
* :py:mod:`airflow.contrib.hooks.bigquery_hook`

Kubernetes Engine
'''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_container_operator`
* :py:mod:`airflow.contrib.hooks.gcp_container_hook`

Transfer Service
''''''''''''''''

For information, look at API Reference:

* :py:mod:`airflow.contrib.operators.gcp_transfer_operator`
* :py:mod:`airflow.contrib.sensors.gcp_transfer_sensor`
* :py:mod:`airflow.contrib.hooks.gcp_transfer_hook`

.. _Qubole:

Qubole
------

Apache Airflow has a native operator and hooks to talk to `Qubole <https://qubole.com/>`__,
which lets you submit your big data jobs directly to Qubole from Apache Airflow.

For further information, look at API Reference:

* :py:mod:`airflow.contrib.operators.qubole_operator`
* :py:mod:`airflow.contrib.sensors.qubole_sensor`
* :py:mod:`airflow.contrib.operators.qubole_check_operator`
