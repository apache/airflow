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

Operators and Hooks Reference
=============================

.. contents:: Content
  :local:
  :depth: 1

.. _fundamentals:

Fundamentals
------------

**Base:**

* :mod:`airflow.hooks.base_hook`
* :mod:`airflow.hooks.dbapi_hook`
* :mod:`airflow.models.baseoperator`
* :mod:`airflow.sensors.base_sensor_operator`

**Operators:**

* :mod:`airflow.operators.branch_operator`
* :mod:`airflow.operators.check_operator`
* :mod:`airflow.operators.dagrun_operator`
* :mod:`airflow.operators.dummy_operator`
* :mod:`airflow.operators.generic_transfer`
* :mod:`airflow.operators.latest_only_operator`
* :mod:`airflow.operators.subdag_operator`

**Sensors:**

* :mod:`airflow.contrib.sensors.weekday_sensor`
* :mod:`airflow.sensors.external_task_sensor`
* :mod:`airflow.sensors.sql_sensor`
* :mod:`airflow.sensors.time_delta_sensor`
* :mod:`airflow.sensors.time_sensor`


.. _Apache:

ASF: Apache Software Foundation
-------------------------------

Airflow supports various software created by `Apache Software Foundation <https://www.apache.org/foundation/>`__.

Software operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within software developed by Apache Software
Foundation.

* `Apache Cassandra <http://cassandra.apache.org/>`__

  * Hooks:
       * :mod:`airflow.providers.apache.cassandra.hooks.cassandra`
  * Sensors:
       * :mod:`airflow.providers.apache.cassandra.sensors.record`,
       * :mod:`airflow.providers.apache.cassandra.sensors.table`


* `Apache Druid <https://druid.apache.org/>`__

  * Hooks:
       * :mod:`airflow.hooks.druid_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.druid_operator`,
       * :mod:`airflow.operators.druid_check_operator`


* `Apache Hive <https://hive.apache.org/>`__

  * Hooks:
       * :mod:`airflow.hooks.hive_hooks`
  * Operators:
       * :mod:`airflow.operators.hive_operator`,
       * :mod:`airflow.operators.hive_stats_operator`
  * Sensors:
       * :mod:`airflow.sensors.named_hive_partition_sensor`,
       * :mod:`airflow.sensors.hive_partition_sensor`,
       * :mod:`airflow.sensors.metastore_partition_sensor`


* `Apache Pig <https://pig.apache.org/>`__

  * Hooks:
       * :mod:`airflow.hooks.pig_hook`
  * Operators:
       * :mod:`airflow.operators.pig_operator`


* `Apache Pinot <https://pinot.apache.org/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.pinot_hook`


* `Apache Spark <https://spark.apache.org/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.spark_jdbc_hook`,
       * :mod:`airflow.contrib.hooks.spark_jdbc_script`,
       * :mod:`airflow.contrib.hooks.spark_sql_hook`,
       * :mod:`airflow.contrib.hooks.spark_submit_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.spark_jdbc_operator`,
       * :mod:`airflow.contrib.operators.spark_sql_operator`,
       * :mod:`airflow.contrib.operators.spark_submit_operator`


* `Apache Sqoop <https://sqoop.apache.org/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.sqoop_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.sqoop_operator`


* `Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__

  * Hooks:
       * :mod:`airflow.hooks.hdfs_hook`
  * Sensors:
       * :mod:`airflow.sensors.hdfs_sensor`,
       * :mod:`airflow.contrib.sensors.hdfs_sensor`


* `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__

  * Hooks:
       * :mod:`airflow.hooks.webhdfs_hook`
  * Sensors:
       * :mod:`airflow.sensors.web_hdfs_sensor`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to software developed by Apache Software
Foundation.

.. list-table::

   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.operators.s3_to_hive_operator`


   * - * Source: `Apache Cassandra <http://cassandra.apache.org/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.cassandra_to_gcs`


   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
       * Operators: :mod:`airflow.contrib.operators.hive_to_dynamodb`


   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `Apache Druid <https://druid.apache.org/>`__
       * Operators: :mod:`airflow.operators.hive_to_druid`


   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.operators.hive_to_mysql`


   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `Samba <https://www.samba.org/>`__
       * Operators: :mod:`airflow.operators.hive_to_samba_operator`


   * - * Source: `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.operators.mssql_to_hive`


   * - * Source: `MySQL <https://www.mysql.com/>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.operators.mysql_to_hive`


   * - * Source: `Vertica <https://www.vertica.com/>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.contrib.operators.vertica_to_hive`


.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for `Microsoft Azure <https://azure.microsoft.com/>`__.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Microsoft Azure.

* `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.wasb_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.wasb_delete_blob_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.wasb_sensor`


* `Azure Container Instances <https://azure.microsoft.com/en-us/services/container-instances/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.azure_container_instance_hook`,
       * :mod:`airflow.contrib.hooks.azure_container_registry_hook`,
       * :mod:`airflow.contrib.hooks.azure_container_volume_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.azure_container_instances_operator`


* `Azure Cosmos DB <https://azure.microsoft.com/en-us/services/cosmos-db/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.azure_cosmos_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.azure_cosmos_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.azure_cosmos_sensor`


* `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.azure_data_lake_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.adls_list_operator`


* `Azure Files <https://azure.microsoft.com/en-us/services/storage/files/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.azure_fileshare_hook`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Microsoft Azure.

.. list-table::

   * - * Source: `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.adls_to_gcs`


   * - * Source: Local
       * Destination: `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
       * Operators: :mod:`airflow.contrib.operators.file_to_wasb`


   * - * Source: `Oracle <https://www.oracle.com/pl/database/>`__
       * Destination: `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
       * Operators: :mod:`airflow.contrib.operators.oracle_to_azure_data_lake_transfer`


.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has support for `Amazon Web Services <https://aws.amazon.com/>`__.

All hooks are based on :mod:`airflow.contrib.hooks.aws_hook`.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Amazon Web Services.

* `AWS Batch <https://aws.amazon.com/batch/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.batch_client`,
       * :mod:`airflow.providers.amazon.aws.hooks.batch_waiters`
  * Operators:
       * :mod:`airflow.providers.amazon.aws.operators.batch`


* `AWS DataSync <https://aws.amazon.com/datasync/>`__

  * Guides:
       * :doc:`How to use <howto/operator/amazon/aws/datasync>`
  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.datasync`
  * Operators:
       * :mod:`airflow.providers.amazon.aws.operators.datasync`


* `AWS Glue Catalog <https://aws.amazon.com/glue/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.aws_glue_catalog_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.aws_glue_catalog_partition_sensor`


* `AWS Lambda <https://aws.amazon.com/lambda/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.lambda_function`


* `Amazon Athena <https://aws.amazon.com/athena/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.athena`
  * Operators:
       * :mod:`airflow.providers.amazon.aws.operators.athena`
  * Sensors:
       * :mod:`airflow.providers.amazon.aws.sensors.athena`


* `Amazon CloudWatch Logs <https://aws.amazon.com/cloudwatch/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.aws_logs_hook`


* `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.aws_dynamodb_hook`


* `Amazon EC2 <https://aws.amazon.com/ec2/>`__

  * Operators:
       * :mod:`airflow.contrib.operators.ecs_operator`


* `Amazon EMR <https://aws.amazon.com/emr/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.emr_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.emr_add_steps_operator`,
       * :mod:`airflow.contrib.operators.emr_create_job_flow_operator`,
       * :mod:`airflow.contrib.operators.emr_terminate_job_flow_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.emr_base_sensor`,
       * :mod:`airflow.contrib.sensors.emr_job_flow_sensor`,
       * :mod:`airflow.contrib.sensors.emr_step_sensor`


* `Amazon Kinesis Data Firehose <https://aws.amazon.com/kinesis/data-firehose/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.kinesis`


* `Amazon Redshift <https://aws.amazon.com/redshift/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.redshift`
  * Sensors:
       * :mod:`airflow.providers.amazon.aws.sensors.redshift`


* `Amazon SageMaker <https://aws.amazon.com/sagemaker/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.sagemaker_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.sagemaker_base_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_endpoint_config_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_endpoint_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_model_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_training_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_transform_operator`,
       * :mod:`airflow.contrib.operators.sagemaker_tuning_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.sagemaker_base_sensor`,
       * :mod:`airflow.contrib.sensors.sagemaker_endpoint_sensor`,
       * :mod:`airflow.contrib.sensors.sagemaker_training_sensor`,
       * :mod:`airflow.contrib.sensors.sagemaker_transform_sensor`,
       * :mod:`airflow.contrib.sensors.sagemaker_tuning_sensor`


* `Amazon Simple Notification Service (SNS) <https://aws.amazon.com/sns/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.sns`
  * Operators:
       * :mod:`airflow.providers.amazon.aws.operators.sns`


* `Amazon Simple Queue Service (SQS) <https://aws.amazon.com/sns/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.sqs`
  * Operators:
       * :mod:`airflow.providers.amazon.aws.operators.sqs`
  * Sensors:
       * :mod:`airflow.providers.amazon.aws.sensors.sqs`


* `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__

  * Hooks:
       * :mod:`airflow.providers.amazon.aws.hooks.s3`
  * Operators:
       * :mod:`airflow.operators.s3_file_transform_operator`,
       * :mod:`airflow.contrib.operators.s3_copy_object_operator`,
       * :mod:`airflow.contrib.operators.s3_delete_objects_operator`,
       * :mod:`airflow.contrib.operators.s3_list_operator`
  * Sensors:
       * :mod:`airflow.sensors.s3_key_sensor`,
       * :mod:`airflow.sensors.s3_prefix_sensor`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Amazon Web Services.

.. list-table::

   * - * Source:
          .. _integration:AWS-Discovery-ref:

          All GCP services
          :ref:`[1] <integration:GCP-Discovery>`
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.operators.google_api_to_s3_transfer`


   * - * Source: `Amazon DataSync <https://aws.amazon.com/datasync/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Guide: :doc:`How to use <howto/operator/amazon/aws/datasync>`
       * Operators: :mod:`airflow.providers.amazon.aws.operators.datasync`


   * - * Source: `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Operators: :mod:`airflow.contrib.operators.dynamodb_to_s3`


   * - * Source: `Amazon Redshift <https://aws.amazon.com/redshift/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Operators: :mod:`airflow.operators.redshift_to_s3_operator`


   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `Amazon Redshift <https://aws.amazon.com/redshift/>`__
       * Operators: :mod:`airflow.operators.s3_to_redshift_operator`


   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators:
          - :mod:`airflow.operators.s3_to_hive_operator`
          - :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.contrib.operators.s3_to_gcs_operator`,

   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
       * Operators: :mod:`airflow.contrib.operators.s3_to_sftp_operator`


   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
       * Operators: :mod:`airflow.contrib.operators.hive_to_dynamodb`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.operators.gcs_to_s3`


   * - * Source: `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.contrib.operators.imap_attachment_to_s3_operator`


   * - * Source: `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.contrib.operators.mongo_to_s3`


   * - * Source: `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Operators: :mod:`airflow.contrib.operators.sftp_to_s3_operator`


:ref:`[1] <integration:AWS-Discovery-ref>` Those discovery-based operators use
:class:`airflow.gcp.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate with Google
Services via the `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
Please note that this library is in maintenance mode hence it won't fully support GCP in the future.
Therefore it is recommended that you use the custom GCP Service Operators for working with the Google
Cloud Platform.

.. _GCP:

GCP: Google Cloud Platform
--------------------------

Airflow has extensive support for the `Google Cloud Platform <https://cloud.google.com/>`__.

See the :doc:`GCP connection type <howto/connection/gcp>` documentation to
configure connections to GCP.

All hooks are based on :class:`airflow.gcp.hooks.base.GoogleCloudBaseHook`.

.. note::
    You can learn how to use GCP integrations by analyzing the
    `source code <https://github.com/apache/airflow/tree/master/airflow/gcp/example_dags/>`_ of the particular example DAGs.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Google Cloud Platform.

..
  PLEASE KEEP THE ALPHABETICAL ORDER OF THE LIST BELOW, BUT OMIT THE "Cloud" PREFIX

* `AutoML <https://cloud.google.com/automl/>`__

  * Guides:
     * :doc:`How to use <howto/operator/gcp/automl>`
  * Hooks:
     * :mod:`airflow.gcp.hooks.automl`
  * Operators:
     * :mod:`airflow.gcp.operators.automl`


* `BigQuery <https://cloud.google.com/bigquery/>`__

  * Hooks
     * :mod:`airflow.gcp.hooks.bigquery`
  * Operators
     * :mod:`airflow.gcp.operators.bigquery`
  * Sensors
     * :mod:`airflow.gcp.sensors.bigquery`

* `BigQuery Data Transfer Service <https://cloud.google.com/bigquery/transfer/>`__

  * Guides
     * :doc:`How to use <howto/operator/gcp/bigquery_dts>`
  * Hooks
     * :mod:`airflow.gcp.hooks.bigquery_dts`
  * Operators
     * :mod:`airflow.gcp.operators.bigquery_dts`
  * Sensors
     * :mod:`airflow.gcp.sensors.bigquery_dts`

* `Bigtable <https://cloud.google.com/bigtable/>`__

  * Guides
     * :doc:`How to use <howto/operator/gcp/bigtable>`
  * Hooks
     * :mod:`airflow.gcp.hooks.bigtable`
  * Operators
     * :mod:`airflow.gcp.operators.bigtable`
  * Sensors
     * :mod:`airflow.gcp.sensors.bigtable`

* `Cloud Build <https://cloud.google.com/cloud build/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/cloud_build>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.cloud_build`
  * Operators:
       * :mod:`airflow.gcp.operators.cloud_build`


* `Compute Engine <https://cloud.google.com/compute/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/compute>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.compute`
  * Operators:
       * :mod:`airflow.gcp.operators.compute`


* `Cloud Data Loss Prevention (DLP) <https://cloud.google.com/dlp/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.dlp`
  * Operators:
       * :mod:`airflow.gcp.operators.dlp`


* `Dataflow <https://cloud.google.com/dataflow/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.dataflow`
  * Operators:
       * :mod:`airflow.gcp.operators.dataflow`


* `Dataproc <https://cloud.google.com/dataproc/>`__

  * Hooks:
       * :mod:`airflow.providers.google.cloud.hooks.dataproc`
  * Operators:
       * :mod:`airflow.providers.google.cloud.operators.dataproc`


* `Datastore <https://cloud.google.com/datastore/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.datastore`
  * Operators:
       * :mod:`airflow.gcp.operators.datastore`


* `Cloud Functions <https://cloud.google.com/functions/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/functions>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.functions`
  * Operators:
       * :mod:`airflow.gcp.operators.functions`


* `Cloud Key Management Service (KMS) <https://cloud.google.com/kms/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.kms`


* `Kubernetes Engine <https://cloud.google.com/kubernetes_engine/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.kubernetes_engine`
  * Operators:
       * :mod:`airflow.gcp.operators.kubernetes_engine`


* `Machine Learning Engine <https://cloud.google.com/ml engine/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.mlengine`
  * Operators:
       * :mod:`airflow.gcp.operators.mlengine`


* `Cloud Memory store <https://cloud.google.com/memorystore/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/cloud_memorystore>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.cloud_memorystore`
  * Operators:
       * :mod:`airflow.gcp.operators.cloud_memorystore`


* `Natural Language <https://cloud.google.com/natural language/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/natural_language>`
  * Hooks:
       * :mod:`airflow.providers.google.cloud.hooks.natural_language`
  * Operators:
       * :mod:`airflow.providers.google.cloud.operators.natural_language`


* `Cloud Pub/Sub <https://cloud.google.com/pubsub/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/pubsub>`
  * Hooks:
       * :mod:`airflow.providers.google.cloud.hooks.pubsub`
  * Operators:
       * :mod:`airflow.providers.google.cloud.operators.pubsub`
  * Sensors:
       * :mod:`airflow.providers.google.cloud.sensors.pubsub`


* `Cloud Spanner <https://cloud.google.com/spanner/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/spanner>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.spanner`
  * Operators:
       * :mod:`airflow.gcp.operators.spanner`


* `Cloud Speech-to-Text <https://cloud.google.com/speech-to-text/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/speech>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.speech_to_text`
  * Operators:
       * :mod:`airflow.gcp.operators.speech_to_text`


* `Cloud SQL <https://cloud.google.com/sql/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/sql>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.cloud_sql`
  * Operators:
       * :mod:`airflow.gcp.operators.cloud_sql`


* `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/gcs>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.gcs`
  * Operators:
       * :mod:`airflow.gcp.operators.gcs`
  * Sensors:
       * :mod:`airflow.gcp.sensors.gcs`


* `Storage Transfer Service <https://cloud.google.com/storage/transfer/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.cloud_storage_transfer_service`
  * Operators:
       * :mod:`airflow.gcp.operators.cloud_storage_transfer_service`
  * Sensors:
       * :mod:`airflow.gcp.sensors.cloud_storage_transfer_service`


* `Cloud Tasks <https://cloud.google.com/tasks/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.tasks`
  * Operators:
       * :mod:`airflow.gcp.operators.tasks`


* `Cloud Text-to-Speech <https://cloud.google.com/text-to-speech/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/speech>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.text_to_speech`
  * Operators:
       * :mod:`airflow.gcp.operators.text_to_speech`


* `Cloud Translation <https://cloud.google.com/translate/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/translate>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.translate`
  * Operators:
       * :mod:`airflow.gcp.operators.translate`


* `Cloud Video Intelligence <https://cloud.google.com/video_intelligence/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/video_intelligence>`
  * Hooks:
       * :mod:`airflow.gcp.hooks.video_intelligence`
  * Operators:
       * :mod:`airflow.gcp.operators.video_intelligence`


* `Cloud Vision <https://cloud.google.com/vision/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/vision>`
  * Hooks:
       * :mod:`airflow.providers.google.cloud.hooks.vision`
  * Operators:
       * :mod:`airflow.providers.google.cloud.operators.vision`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Google Cloud Platform.

.. list-table::

   * - * Source:
          .. _integration:GCP-Discovery-ref:

          All services :ref:`[1] <integration:GCP-Discovery>`
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.operators.google_api_to_s3_transfer`


   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Guide: :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
       * Operators:
          - :mod:`airflow.contrib.operators.s3_to_gcs_operator`,
          - :mod:`airflow.gcp.operators.cloud_storage_transfer_service`


   * - * Source: `Apache Cassandra <http://cassandra.apache.org/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.cassandra_to_gcs`


   * - * Source: `Azure Data Lake Storage <https://azure.microsoft.com/pl-pl/services/storage/data-lake-storage/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.adls_to_gcs`


   * - * Source: `Google BigQuery <https://cloud.google.com/bigquery/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.operators.bigquery_to_mysql`


   * - * Source: `Google BigQuery <https://cloud.google.com/bigquery/>`__
       * Destination: `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.bigquery_to_gcs`


   * - * Source: `Google BigQuery <https://cloud.google.com/bigquery/>`__
       * Destination: `Google BigQuery <https://cloud.google.com/bigquery/>`__
       * Operators: :mod:`airflow.operators.bigquery_to_bigquery`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.operators.gcs_to_s3`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Google BigQuery <https://cloud.google.com/bigquery/>`__
       * Operators: :mod:`airflow.operators.gcs_to_bq`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Guides:
          - :doc:`How to use <howto/operator/gcp/gcs_to_gcs>`,
          - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
       * Operators:
          - :mod:`airflow.operators.gcs_to_gcs`,
          - :mod:`airflow.gcp.operators.cloud_storage_transfer_service`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Google Drive <https://www.google.com/drive/>`__
       * Operators: :mod:`airflow.contrib.operators.gcs_to_gdrive_operator`


   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: SFTP
       * Guide: :doc:`How to use <howto/operator/gcp/gcs_to_sftp>`
       * Operators: :mod:`airflow.operators.gcs_to_sftp`


   * - * Source: Local
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.local_to_gcs`


   * - * Source: `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.mssql_to_gcs`


   * - * Source: `MySQL <https://www.mysql.com/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.mysql_to_gcs`


   * - * Source: `PostgresSQL <https://www.postgresql.org/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.postgres_to_gcs`


   * - * Source: SFTP
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Guide: :doc:`How to use <howto/operator/gcp/sftp_to_gcs>`
       * Operators: :mod:`airflow.providers.google.cloud.operators.sftp_to_gcs`


   * - * Source: SQL
       * Destination: `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.sql_to_gcs`



.. _integration:GCP-Discovery:

:ref:`[1] <integration:GCP-Discovery-ref>` Those discovery-based operators use
:class:`airflow.gcp.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate with Google
Services via the `Google API Python Client <https://github.com/googleapis/google-api-python-client>`__.
Please note that this library is in maintenance mode hence it won't fully support GCP in the future.
Therefore it is recommended that you use the custom GCP Service Operators for working with the Google
Cloud Platform.

Other operators and hooks
'''''''''''''''''''''''''

.. list-table::

   * - * Guide: :doc:`How to use <howto/operator/gcp/translate-speech>`
       * Operators: :mod:`airflow.gcp.operators.translate_speech`

   * - * Hooks: :mod:`airflow.gcp.hooks.discovery_api`

.. _service:

Service integrations
--------------------

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.


* `Atlassian Jira <https://www.atlassian.com/pl/software/jira>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.jira_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.jira_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.jira_sensor`


* `Databricks <https://databricks.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.databricks_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.databricks_operator`


* `Datadog <https://www.datadoghq.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.datadog_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.datadog_sensor`


* `Pagerduty <https://www.pagerduty.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.pagerduty_hook`


* `Dingding <https://oapi.dingtalk.com>`__

  * Guides:
       * :doc:`How to use <howto/operator/dingding>`
  * Hooks:
       * :mod:`airflow.contrib.hooks.dingding_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.dingding_operator`


* `Discord <https://discordapp.com>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.discord_webhook_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.discord_webhook_operator`


* `Google Campaign Manager <https://developers.google.com/doubleclick-advertisers>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/campaign_manager>`
  * Hooks:
       * :mod:`airflow.providers.google.marketing_platform.hooks.campaign_manager`
  * Operators:
       * :mod:`airflow.providers.google.marketing_platform.operators.campaign_manager`
  * Sensors:
       * :mod:`airflow.providers.google.marketing_platform.sensors.campaign_manager`


* `Google Display&Video 360 <https://marketingplatform.google.com/about/display-video-360/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/display_video>`
  * Hooks:
       * :mod:`airflow.providers.google.marketing_platform.hooks.display_video`
  * Operators:
       * :mod:`airflow.providers.google.marketing_platform.operators.display_video`
  * Sensors:
       * :mod:`airflow.providers.google.marketing_platform.sensors.display_video`


* `Google Drive <https://www.google.com/drive/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.gdrive_hook`


* `Google Search Ads 360 <https://marketingplatform.google.com/about/search-ads-360/>`__

  * Guides:
       * :doc:`How to use <howto/operator/gcp/search_ads>`
  * Hooks:
       * :mod:`airflow.providers.google.marketing_platform.hooks.search_ads`
  * Operators:
       * :mod:`airflow.providers.google.marketing_platform.operators.search_ads`
  * Sensors:
       * :mod:`airflow.providers.google.marketing_platform.sensors.search_ads`


* `Google Spreadsheet <https://www.google.com/intl/en/sheets/about/>`__

  * Hooks:
       * :mod:`airflow.gcp.hooks.gsheets`


* `IBM Cloudant <https://www.ibm.com/cloud/cloudant>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.cloudant_hook`


* `Jenkins <https://jenkins.io/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.jenkins_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.jenkins_job_trigger_operator`


* `Opsgenie <https://www.opsgenie.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.opsgenie_alert_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.opsgenie_alert_operator`


* `Qubole <https://www.qubole.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.qubole_hook`,
       * :mod:`airflow.contrib.hooks.qubole_check_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.qubole_operator`,
       * :mod:`airflow.contrib.operators.qubole_check_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.qubole_sensor`


* `Salesforce <https://www.salesforce.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.salesforce_hook`


* `Segment <https://oapi.dingtalk.com>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.segment_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.segment_track_event_operator`


* `Slack <https://slack.com/>`__

  * Hooks:
       * :mod:`airflow.hooks.slack_hook`,
       * :mod:`airflow.contrib.hooks.slack_webhook_hook`
  * Operators:
       * :mod:`airflow.operators.slack_operator`,
       * :mod:`airflow.contrib.operators.slack_webhook_operator`


* `Snowflake <https://www.snowflake.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.snowflake_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.snowflake_operator`


* `Vertica <https://www.vertica.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.vertica_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.vertica_operator`


* `Zendesk <https://www.zendesk.com/>`__

  * Hooks:
       * :mod:`airflow.hooks.zendesk_hook`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.

.. list-table::

   * - * Source: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Destination: `Google Drive <https://www.google.com/drive/>`__
       * Operators: :mod:`airflow.contrib.operators.gcs_to_gdrive_operator`


   * - * Source: `Vertica <https://www.vertica.com/>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.contrib.operators.vertica_to_hive`


   * - * Source: `Vertica <https://www.vertica.com/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.contrib.operators.vertica_to_mysql`


.. _software:

Software integrations
---------------------

Software operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations using various software.

* `Celery <http://www.celeryproject.org/>`__

  * Sensors:
       * :mod:`airflow.contrib.sensors.celery_queue_sensor`


* `Docker <https://docs.docker.com/install/>`__

  * Hooks:
       * :mod:`airflow.hooks.docker_hook`
  * Operators:
       * :mod:`airflow.operators.docker_operator`,
       * :mod:`airflow.contrib.operators.docker_swarm_operator`


* `GNU Bash <https://www.gnu.org/software/bash/>`__

  * Guides:
       * :doc:`How to use <howto/operator/bash>`
  * Operators:
       * :mod:`airflow.operators.bash_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.bash_sensor`


* `Kubernetes <https://kubernetes.io/>`__

  * Guides:
       * :doc:`How to use <howto/operator/kubernetes>`
  * Operators:
       * :mod:`airflow.contrib.operators.kubernetes_pod_operator`


* `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__

  * Hooks:
       * :mod:`airflow.hooks.mssql_hook`
  * Operators:
       * :mod:`airflow.operators.mssql_operator`


* `MongoDB <https://www.mongodb.com/what-is-mongodb>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.mongo_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.mongo_sensor`


* `MySQL <https://www.mysql.com/products/>`__

  * Hooks:
       * :mod:`airflow.hooks.mysql_hook`
  * Operators:
       * :mod:`airflow.operators.mysql_operator`


* `OpenFaaS <https://www.openfaas.com/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.openfaas_hook`


* `Oracle <https://www.oracle.com/pl/database/>`__

  * Hooks:
       * :mod:`airflow.hooks.oracle_hook`
  * Operators:
       * :mod:`airflow.operators.oracle_operator`


* `Papermill <https://github.com/nteract/papermill>`__

  * Guides:
       * :doc:`How to use <howto/operator/papermill>`
  * Operators:
       * :mod:`airflow.operators.papermill_operator`


* `PostgresSQL <https://www.postgresql.org/>`__

  * Hooks:
       * :mod:`airflow.hooks.postgres_hook`
  * Operators:
       * :mod:`airflow.operators.postgres_operator`


* `Presto <http://prestodb.github.io/>`__

  * Hooks:
       * :mod:`airflow.hooks.presto_hook`
  * Operators:
       * :mod:`airflow.operators.presto_check_operator`


* `Python <https://www.python.org>`__

  * Operators:
       * :mod:`airflow.operators.python_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.python_sensor`


* `Redis <https://redis.io/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.redis_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.redis_publish_operator`
  * Sensors:
       * :mod:`airflow.contrib.sensors.redis_pub_sub_sensor`,
       * :mod:`airflow.contrib.sensors.redis_key_sensor`


* `Samba <https://www.samba.org/>`__

  * Hooks:
       * :mod:`airflow.hooks.samba_hook`


* `SQLite <https://www.sqlite.org/index.html>`__

  * Hooks:
       * :mod:`airflow.hooks.sqlite_hook`
  * Operators:
       * :mod:`airflow.operators.sqlite_operator`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. list-table::

   * - * Source: `Apache Hive <https://hive.apache.org/>`__
       * Destination: `Samba <https://www.samba.org/>`__
       * Operators: :mod:`airflow.operators.hive_to_samba_operator`


   * - * Source: `BigQuery <https://cloud.google.com/bigquery/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.operators.bigquery_to_mysql`


   * - * Source: `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.operators.mssql_to_hive`


   * - * Source: `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.mssql_to_gcs`


   * - * Source: `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.contrib.operators.mongo_to_s3`


   * - * Source: `MySQL <https://www.mysql.com/>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.operators.mysql_to_hive`


   * - * Source: `MySQL <https://www.mysql.com/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.mysql_to_gcs`


   * - * Source: `Oracle <https://www.oracle.com/pl/database/>`__
       * Destination: `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
       * Operators: :mod:`airflow.contrib.operators.oracle_to_azure_data_lake_transfer`


   * - * Source: `Oracle <https://www.oracle.com/pl/database/>`__
       * Destination: `Oracle <https://www.oracle.com/pl/database/>`__
       * Operators: :mod:`airflow.contrib.operators.oracle_to_oracle_transfer`


   * - * Source: `PostgresSQL <https://www.postgresql.org/>`__
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.postgres_to_gcs`


   * - * Source: `Presto <https://prestodb.github.io/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.operators.presto_to_mysql`


   * - * Source: SQL
       * Destination: `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.sql_to_gcs`


   * - * Source: `Vertica <https://www.vertica.com/>`__
       * Destination: `Apache Hive <https://hive.apache.org/>`__
       * Operators: :mod:`airflow.contrib.operators.vertica_to_hive`


   * - * Source: `Vertica <https://www.vertica.com/>`__
       * Destination: `MySQL <https://www.mysql.com/>`__
       * Operators: :mod:`airflow.contrib.operators.vertica_to_mysql`


.. _protocol:

Protocol integrations
---------------------

Protocol operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services using standardized
communication protocols or interface.

* `File Transfer Protocol (FTP) <https://tools.ietf.org/html/rfc114>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.ftp_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.ftp_sensor`


* Filesystem

  * Hooks:
       * :mod:`airflow.contrib.hooks.fs_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.file_sensor`


* `Hypertext Transfer Protocol (HTTP) <https://www.w3.org/Protocols/>`__

  * Hooks:
       * :mod:`airflow.hooks.http_hook`
  * Operators:
       * :mod:`airflow.operators.http_operator`
  * Sensors:
       * :mod:`airflow.sensors.http_sensor`


* `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.imap_hook`
  * Sensors:
       * :mod:`airflow.contrib.sensors.imap_attachment_sensor`


* `Java Database Connectivity (JDBC) <https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/>`__

  * Hooks:
       * :mod:`airflow.hooks.jdbc_hook`
  * Operators:
       * :mod:`airflow.operators.jdbc_operator`


* `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__

  * Hooks:
       * :mod:`airflow.providers.sftp.hooks.sftp_hook`
  * Operators:
       * :mod:`airflow.providers.sftp.operators.sftp_operator`
  * Sensors:
       * :mod:`airflow.providers.sftp.sensors.sftp_sensor`


* `Secure Shell (SSH) <https://tools.ietf.org/html/rfc4251>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.ssh_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.ssh_operator`


* `Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc821>`__

  * Operators:
       * :mod:`airflow.operators.email_operator`


* `Windows Remote Management (WinRM) <https://docs.microsoft.com/en-gb/windows/win32/winrm/portal>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.winrm_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.winrm_operator`


* `gRPC <https://grpc.io/>`__

  * Hooks:
       * :mod:`airflow.contrib.hooks.grpc_hook`
  * Operators:
       * :mod:`airflow.contrib.operators.grpc_operator`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. list-table::

   * - * Source: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Destination: `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
       * Operators: :mod:`airflow.contrib.operators.s3_to_sftp_operator`


   * - * Source: Filesystem
       * Destination: `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
       * Operators: :mod:`airflow.contrib.operators.file_to_wasb`


   * - * Source: Filesystem
       * Destination: `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
       * Operators: :mod:`airflow.operators.local_to_gcs`


   * - * Source: `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
       * Operators: :mod:`airflow.contrib.operators.imap_attachment_to_s3_operator`


   * - * Source: `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
       * Destination: `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
       * Operators: :mod:`airflow.contrib.operators.sftp_to_s3_operator`
