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

Airflow has limited support for `Microsoft Azure <https://azure.microsoft.com/>`__.

Logging
'''''''

Airflow can be configured to read and write task logs in Azure Blob Storage.
See :ref:`write-logs-azure`.


Operators and Hooks
'''''''''''''''''''

Service operators and hooks
"""""""""""""""""""""""""""

These integrations allow you to perform various operations within the Microsoft Azure.


.. list-table::
   :header-rows: 1

   * - Service name
     - Hook
     - Operators
     - Sensors

   * - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     -
     - :mod:`airflow.contrib.operators.wasb_delete_blob_operator`
     - :mod:`airflow.contrib.sensors.wasb_sensor`

   * - `Azure Container Instances <https://azure.microsoft.com/en-us/services/container-instances/>`__
     - :mod:`airflow.contrib.hooks.azure_container_instance_hook`,
       :mod:`airflow.contrib.hooks.azure_container_registry_hook`,
       :mod:`airflow.contrib.hooks.azure_container_volume_hook`
     - :mod:`airflow.contrib.operators.azure_container_instances_operator`
     -

   * - `Azure Cosmos DB <https://azure.microsoft.com/en-us/services/cosmos-db/>`__
     - :mod:`airflow.contrib.hooks.azure_cosmos_hook`
     - :mod:`airflow.contrib.operators.azure_cosmos_operator`
     - :mod:`airflow.contrib.sensors.azure_cosmos_sensor`

   * - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     - :mod:`airflow.contrib.hooks.azure_data_lake_hook`
     - :mod:`airflow.contrib.operators.adls_list_operator`
     -

   * - `Azure Files <https://azure.microsoft.com/en-us/services/storage/files/>`__
     - :mod:`airflow.contrib.hooks.azure_fileshare_hook`
     -
     -


Transfer operators and hooks
""""""""""""""""""""""""""""

These integrations allow you to copy data from/to Microsoft Azure.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.contrib.operators.adls_to_gcs`

   * - Local
     - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     -
     - :mod:`airflow.contrib.operators.file_to_wasb`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     -
     - :mod:`airflow.contrib.operators.oracle_to_azure_data_lake_transfer`


.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has support for `Amazon Web Services <https://aws.amazon.com/>`__.

Logging
'''''''

Airflow can be configured to read and write task logs in Amazon Simple Storage Service (Amazon S3).
See :ref:`write-logs-amazon`.

Operators and Hooks
'''''''''''''''''''

All hooks are based on :mod:`airflow.contrib.hooks.aws_hook`.

Service operators and hooks
"""""""""""""""""""""""""""

These integrations allow you to perform various operations within the Amazon Web Services.

.. list-table::
   :header-rows: 1

   * - Service name
     - Hook
     - Operators
     - Sensors

   * - `Amazon Athena <https://aws.amazon.com/athena/>`__
     - :mod:`airflow.contrib.hooks.aws_athena_hook`
     - :mod:`airflow.contrib.operators.aws_athena_operator`
     - :mod:`airflow.contrib.sensors.aws_athena_sensor`

   * - `AWS Batch <https://aws.amazon.com/athena/>`__
     -
     - :mod:`airflow.contrib.operators.awsbatch_operator`
     -

   * - `Amazon CloudWatch Logs <https://aws.amazon.com/cloudwatch/>`__
     - :mod:`airflow.contrib.hooks.aws_logs_hook`
     -
     -

   * - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     - :mod:`airflow.contrib.hooks.aws_dynamodb_hook`
     -
     -

   * - `Amazon EC2 <https://aws.amazon.com/ec2/>`__
     -
     - :mod:`airflow.contrib.operators.ecs_operator`
     -

   * - `Amazon EMR <https://aws.amazon.com/emr/>`__
     - :mod:`airflow.contrib.hooks.emr_hook`
     - :mod:`airflow.contrib.operators.emr_add_steps_operator`,
       :mod:`airflow.contrib.operators.emr_create_job_flow_operator`,
       :mod:`airflow.contrib.operators.emr_terminate_job_flow_operator`.
     - :mod:`airflow.contrib.sensors.emr_base_sensor`,
       :mod:`airflow.contrib.sensors.emr_job_flow_sensor`,
       :mod:`airflow.contrib.sensors.emr_step_sensor`.

   * - `AWS Glue Catalog <https://aws.amazon.com/glue/>`__
     - :mod:`airflow.contrib.hooks.aws_glue_catalog_hook`
     -
     - :mod:`airflow.contrib.sensors.aws_glue_catalog_partition_sensor`

   * - `Amazon Kinesis Data Firehose <https://aws.amazon.com/kinesis/data-firehose/>`__
     - :mod:`airflow.contrib.hooks.aws_firehose_hook`
     -
     -

   * - `AWS Lambda <https://aws.amazon.com/kinesis/>`__
     - :mod:`airflow.contrib.hooks.aws_lambda_hook`
     -
     -

   * - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     - :mod:`airflow.contrib.hooks.redshift_hook`
     -
     - :mod:`airflow.contrib.sensors.aws_redshift_cluster_sensor`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - :mod:`airflow.hooks.S3_hook`
     - :mod:`airflow.operators.s3_file_transform_operator`
       :mod:`airflow.contrib.operators.s3_copy_object_operator`,
       :mod:`airflow.contrib.operators.s3_delete_objects_operator`,
       :mod:`airflow.contrib.operators.s3_list_operator`.
     - :mod:`airflow.sensors.s3_key_sensor`,
       :mod:`airflow.sensors.s3_prefix_sensor`,

   * - `Amazon SageMaker <https://aws.amazon.com/sagemaker/>`__
     - :mod:`airflow.contrib.hooks.sagemaker_hook`,
     - :mod:`airflow.contrib.operators.sagemaker_base_operator`,
       :mod:`airflow.contrib.operators.sagemaker_endpoint_config_operator`,
       :mod:`airflow.contrib.operators.sagemaker_endpoint_operator`,
       :mod:`airflow.contrib.operators.sagemaker_model_operator`,
       :mod:`airflow.contrib.operators.sagemaker_training_operator`,
       :mod:`airflow.contrib.operators.sagemaker_transform_operator`,
       :mod:`airflow.contrib.operators.sagemaker_tuning_operator`.
     - :mod:`airflow.contrib.sensors.sagemaker_base_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_endpoint_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_training_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_transform_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_tuning_sensor`.

   * - `Amazon Simple Notification Service (SNS) <https://aws.amazon.com/sns/>`__
     - :mod:`airflow.contrib.hooks.aws_sns_hook`
     - :mod:`airflow.contrib.operators.sns_publish_operator`
     -

   * - `Amazon Simple Queue Service (SQS) <https://aws.amazon.com/sns/>`__
     - :mod:`airflow.contrib.hooks.aws_sqs_hook`
     - :mod:`airflow.contrib.operators.aws_sqs_publish_operator`
     - :mod:`airflow.contrib.sensors.aws_sqs_sensor`

Transfer operators and hooks
""""""""""""""""""""""""""""

These integrations allow you to copy data from/to Amazon Web Services.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.contrib.operators.hive_to_dynamodb`

   * - `MongoDB <https://www.mongodb.com/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.contrib.operators.hive_to_dynamodb`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.contrib.operators.s3_to_gcs_operator`
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.operators.redshift_to_s3_operator`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.s3_to_hive_operator`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     -
     - :mod:`airflow.operators.s3_to_redshift_operator`

.. _GCP:

GCP: Google Cloud Platform
--------------------------

Airflow has extensive support for the `Google Cloud Platform <https://cloud.google.com/>`__.

See the :doc:`GCP connection type <howto/connection/gcp>` documentation to
configure connections to GCP.

Logging
'''''''

Airflow can be configured to read and write task logs in Google Cloud Storage.
See :ref:`write-logs-gcp`.


Operators and Hooks
'''''''''''''''''''

All hooks are based on :class:`airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook`.

Service operators and hooks
"""""""""""""""""""""""""""

These integrations allow you to perform various operations within the Google Cloud Platform.

..
  PLEASE KEEP THE ALPHABETICAL ORDER OF THE LIST BELOW, BUT OMIT THE "Cloud" PREFIX

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `AutoML <https://cloud.google.com/automl/>`__
     - :doc:`How to use <howto/operator/gcp/automl>`
     - :mod:`airflow.gcp.hooks.automl`
     -
     -

   * - `BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.gcp.hooks.bigquery`
     - :mod:`airflow.gcp.operators.bigquery`
     - :mod:`airflow.gcp.sensors.bigquery`

   * - `BigQuery Data Transfer Service <https://cloud.google.com/bigquery/transfer/>`__
     - :doc:`How to use <howto/operator/gcp/bigquery_dts>`
     - :mod:`airflow.gcp.hooks.bigquery_dts`
     - :mod:`airflow.gcp.operators.bigquery_dts`
     - :mod:`airflow.gcp.sensors.bigquery_dts`

   * - `Bigtable <https://cloud.google.com/bigtable/>`__
     - :doc:`How to use <howto/operator/gcp/bigtable>`
     - :mod:`airflow.gcp.hooks.bigtable`
     - :mod:`airflow.gcp.operators.bigtable`
     - :mod:`airflow.gcp.sensors.bigtable`

   * - `Cloud Build <https://cloud.google.com/cloud-build/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_build>`
     - :mod:`airflow.gcp.hooks.cloud_build`
     - :mod:`airflow.gcp.operators.cloud_build`
     -

   * - `Compute Engine <https://cloud.google.com/compute/>`__
     - :doc:`How to use <howto/operator/gcp/compute>`
     - :mod:`airflow.gcp.hooks.compute`
     - :mod:`airflow.gcp.operators.compute`
     -

   * - `Dataflow <https://cloud.google.com/dataflow/>`__
     -
     - :mod:`airflow.gcp.hooks.dataflow`
     - :mod:`airflow.gcp.operators.dataflow`
     -

   * - `Dataproc <https://cloud.google.com/dataproc/>`__
     -
     - :mod:`airflow.gcp.hooks.dataproc`
     - :mod:`airflow.gcp.operators.dataproc`
     -

   * - `Datastore <https://cloud.google.com/datastore/>`__
     -
     - :mod:`airflow.gcp.hooks.datastore`
     - :mod:`airflow.gcp.operators.datastore`
     -

   * - `Cloud Data Loss Prevention (DLP) <https://cloud.google.com/dlp/>`__
     -
     - :mod:`airflow.gcp.hooks.dlp`
     - :mod:`airflow.gcp.operators.dlp`
     -

   * - `Cloud Functions <https://cloud.google.com/functions/>`__
     - :doc:`How to use <howto/operator/gcp/functions>`
     - :mod:`airflow.gcp.hooks.functions`
     - :mod:`airflow.gcp.operators.functions`
     -

   * - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs>`
     - :mod:`airflow.gcp.hooks.gcs`
     - :mod:`airflow.gcp.operators.gcs`
     - :mod:`airflow.gcp.sensors.gcs`

   * - `Cloud Key Management Service (KMS) <https://cloud.google.com/kms/>`__
     -
     - :mod:`airflow.gcp.hooks.kms`
     -
     -

   * - `Kubernetes Engine <https://cloud.google.com/kubernetes_engine/>`__
     -
     - :mod:`airflow.gcp.hooks.kubernetes_engine`
     - :mod:`airflow.gcp.operators.kubernetes_engine`
     -

   * - `Cloud Memorystore <https://cloud.google.com/memorystore/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_memorystore>`
     - :mod:`airflow.gcp.hooks.cloud_memorystore`
     - :mod:`airflow.gcp.operators.cloud_memorystore`
     -

   * - `Machine Learning Engine <https://cloud.google.com/ml-engine/>`__
     -
     - :mod:`airflow.gcp.hooks.mlengine`
     - :mod:`airflow.gcp.operators.mlengine`
     -

   * - `Natural Language <https://cloud.google.com/natural-language/>`__
     - :doc:`How to use <howto/operator/gcp/natural_language>`
     - :mod:`airflow.gcp.hooks.natural_language`
     - :mod:`airflow.gcp.operators.natural_language`
     -

   * - `Cloud Pub/Sub <https://cloud.google.com/pubsub/>`__
     -
     - :mod:`airflow.gcp.hooks.pubsub`
     - :mod:`airflow.gcp.operators.pubsub`
     - :mod:`airflow.gcp.sensors.pubsub`

   * - `Cloud Spanner <https://cloud.google.com/spanner/>`__
     - :doc:`How to use <howto/operator/gcp/spanner>`
     - :mod:`airflow.gcp.hooks.spanner`
     - :mod:`airflow.gcp.operators.spanner`
     -

   * - `Cloud Speech-to-Text <https://cloud.google.com/speech-to-text/>`__
     - :doc:`How to use <howto/operator/gcp/speech>`
     - :mod:`airflow.gcp.hooks.speech_to_text`
     - :mod:`airflow.gcp.operators.speech_to_text`
     -

   * - `Cloud SQL <https://cloud.google.com/sql/>`__
     - :doc:`How to use <howto/operator/gcp/sql>`
     - :mod:`airflow.gcp.hooks.cloud_sql`
     - :mod:`airflow.gcp.operators.cloud_sql`
     -

   * - `Storage Transfer Service <https://cloud.google.com/storage/transfer/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.gcp.hooks.cloud_storage_transfer_service`
     - :mod:`airflow.gcp.operators.cloud_storage_transfer_service`
     - :mod:`airflow.gcp.sensors.cloud_storage_transfer_service`

   * - `Cloud Tasks <https://cloud.google.com/tasks/>`__
     -
     - :mod:`airflow.gcp.hooks.tasks`
     - :mod:`airflow.gcp.operators.tasks`
     -

   * - `Cloud Text-to-Speech <https://cloud.google.com/text-to-speech/>`__
     - :doc:`How to use <howto/operator/gcp/speech>`
     - :mod:`airflow.gcp.hooks.text_to_speech`
     - :mod:`airflow.gcp.operators.text_to_speech`
     -

   * - `Cloud Translation <https://cloud.google.com/translate/>`__
     - :doc:`How to use <howto/operator/gcp/translate>`
     - :mod:`airflow.gcp.hooks.translate`
     - :mod:`airflow.gcp.operators.translate`
     -

   * - `Cloud Video Intelligence <https://cloud.google.com/video_intelligence/>`__
     - :doc:`How to use <howto/operator/gcp/video_intelligence>`
     - :mod:`airflow.gcp.hooks.video_intelligence`
     - :mod:`airflow.gcp.operators.video_intelligence`
     -

   * - `Cloud Vision <https://cloud.google.com/vision/>`__
     - :doc:`How to use <howto/operator/gcp/vision>`
     - :mod:`airflow.gcp.hooks.vision`
     - :mod:`airflow.gcp.operators.vision`
     -


Transfer operators and hooks
""""""""""""""""""""""""""""

These integrations allow you to copy data from/to Google Cloud Platform.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * -
       .. _integration:GCP-Discovery-ref:

       All services :ref:`[1] <integration:GCP-Discovery>`
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.operators.google_api_to_s3_transfer`

   * - `Azure Data Lake Storage <https://azure.microsoft.com/pl-pl/services/storage/data-lake-storage/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.adls_to_gcs`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.operators.s3_to_gcs`
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.operators.bigquery_to_bigquery`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.bigquery_to_gcs`

   * - `BigQuery <https://cloud.google.com/bigquery/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.operators.bigquery_to_mysql`

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.cassandra_to_gcs`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.operators.gcs_to_bq`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs_to_gcs>`,
       :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.operators.gcs_to_gcs`
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.operators.gcs_to_s3`

   * - Local
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.local_to_gcs`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.mssql_to_gcs`

   * - `MySQL <https://www.mysql.com/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.mysql_to_gcs`

   * - `PostgresSQL <https://www.postgresql.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.postgres_to_gcs`

   * - SQL
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.sql_to_gcs`


.. _integration:GCP-Discovery:

:ref:`[1] <integration:GCP-Discovery-ref>` Those discovery-based operators use
:class:`airflow.gcp.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate with Google
Services via the `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
Please note that this library is in maintenance mode hence it won't fully support GCP in the future.
Therefore it is recommended that you use the custom GCP Service Operators for working with the Google
Cloud Platform.

.. note::
    You can learn how to use GCP integrations by analyzing the
    `source code <https://github.com/apache/airflow/tree/master/airflow/gcp/example_dags/>`_ of the particular example DAGs.

.. _other:

Other integrations
------------------

Operators and Hooks
'''''''''''''''''''

Service operators and hooks
"""""""""""""""""""""""""""

These integrations allow you to perform various operations within various services.

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `Qubole <https://www.qubole.com/>`__
     -
     - :mod:`airflow.contrib.hooks.qubole_hook`
     - :mod:`airflow.contrib.operators.qubole_operator`,
       :mod:`airflow.contrib.operators.qubole_check_operator`
     - :mod:`airflow.contrib.sensors.qubole_sensor`

   * - `Databricks <https://databricks.com/>`__
     -
     - :mod:`airflow.contrib.hooks.databricks_hook`
     - :mod:`airflow.contrib.operators.databricks_operator`
     -
