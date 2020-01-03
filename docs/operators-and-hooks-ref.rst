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

.. list-table::
   :header-rows: 1

   * - Service name
     - Guides
     - Hook
     - Operators
     - Sensors

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.cassandra.hooks.cassandra`
     -
     - :mod:`airflow.providers.apache.cassandra.sensors.record`,
       :mod:`airflow.providers.apache.cassandra.sensors.table`

   * - `Apache Druid <https://druid.apache.org/>`__
     -
     - :mod:`airflow.hooks.druid_hook`
     - :mod:`airflow.contrib.operators.druid_operator`,
       :mod:`airflow.operators.druid_check_operator`
     -

   * - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.hooks.hive_hooks`
     - :mod:`airflow.operators.hive_operator`,
       :mod:`airflow.operators.hive_stats_operator`
     - :mod:`airflow.sensors.named_hive_partition_sensor`,
       :mod:`airflow.sensors.hive_partition_sensor`,
       :mod:`airflow.sensors.metastore_partition_sensor`

   * - `Apache Pig <https://pig.apache.org/>`__
     -
     - :mod:`airflow.hooks.pig_hook`
     - :mod:`airflow.operators.pig_operator`
     -

   * - `Apache Pinot <https://pinot.apache.org/>`__
     -
     - :mod:`airflow.contrib.hooks.pinot_hook`
     -
     -

   * - `Apache Spark <https://spark.apache.org/>`__
     -
     - :mod:`airflow.contrib.hooks.spark_jdbc_hook`,
       :mod:`airflow.contrib.hooks.spark_jdbc_script`,
       :mod:`airflow.contrib.hooks.spark_sql_hook`,
       :mod:`airflow.contrib.hooks.spark_submit_hook`
     - :mod:`airflow.contrib.operators.spark_jdbc_operator`,
       :mod:`airflow.contrib.operators.spark_sql_operator`,
       :mod:`airflow.contrib.operators.spark_submit_operator`
     -

   * - `Apache Sqoop <https://sqoop.apache.org/>`__
     -
     - :mod:`airflow.contrib.hooks.sqoop_hook`
     - :mod:`airflow.contrib.operators.sqoop_operator`
     -

   * - `Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__
     -
     - :mod:`airflow.hooks.hdfs_hook`
     -
     - :mod:`airflow.sensors.hdfs_sensor`,
       :mod:`airflow.contrib.sensors.hdfs_sensor`

   * - `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__
     -
     - :mod:`airflow.hooks.webhdfs_hook`
     -
     - :mod:`airflow.sensors.web_hdfs_sensor`


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to software developed by Apache Software
Foundation.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.s3_to_hive_operator`

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.cassandra_to_gcs`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.contrib.operators.hive_to_dynamodb`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Apache Druid <https://druid.apache.org/>`__
     -
     - :mod:`airflow.operators.hive_to_druid`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.operators.hive_to_mysql`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Samba <https://www.samba.org/>`__
     -
     - :mod:`airflow.operators.hive_to_samba_operator`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.mssql_to_hive`

   * - `MySQL <https://www.mysql.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.mysql_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.contrib.operators.vertica_to_hive`

.. _Azure:

Azure: Microsoft Azure
----------------------

Airflow has limited support for `Microsoft Azure <https://azure.microsoft.com/>`__.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Microsoft Azure.


.. list-table::
   :header-rows: 1

   * - Service name
     - Hook
     - Operators
     - Sensors

   * - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     - :mod:`airflow.contrib.hooks.wasb_hook`
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
''''''''''''''''''''''''''''

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
     - :mod:`airflow.operators.adls_to_gcs`

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

All hooks are based on :mod:`airflow.contrib.hooks.aws_hook`.

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Amazon Web Services.

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `AWS Batch <https://aws.amazon.com/batch/>`__
     -
     -
     - :mod:`airflow.providers.amazon.aws.operators.batch`
     -

   * - `AWS DataSync <https://aws.amazon.com/datasync/>`__
     - :doc:`How to use <howto/operator/amazon/aws/datasync>`
     - :mod:`airflow.providers.amazon.aws.hooks.datasync`
     - :mod:`airflow.providers.amazon.aws.operators.datasync`
     -

   * - `AWS Glue Catalog <https://aws.amazon.com/glue/>`__
     -
     - :mod:`airflow.contrib.hooks.aws_glue_catalog_hook`
     -
     - :mod:`airflow.contrib.sensors.aws_glue_catalog_partition_sensor`

   * - `AWS Lambda <https://aws.amazon.com/lambda/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.lambda_function`
     -
     -

   * - `Amazon Athena <https://aws.amazon.com/athena/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.athena`
     - :mod:`airflow.providers.amazon.aws.operators.athena`
     - :mod:`airflow.providers.amazon.aws.sensors.athena`

   * - `Amazon CloudWatch Logs <https://aws.amazon.com/cloudwatch/>`__
     -
     - :mod:`airflow.contrib.hooks.aws_logs_hook`
     -
     -

   * - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.contrib.hooks.aws_dynamodb_hook`
     -
     -

   * - `Amazon EC2 <https://aws.amazon.com/ec2/>`__
     -
     -
     - :mod:`airflow.contrib.operators.ecs_operator`
     -

   * - `Amazon EMR <https://aws.amazon.com/emr/>`__
     -
     - :mod:`airflow.contrib.hooks.emr_hook`
     - :mod:`airflow.contrib.operators.emr_add_steps_operator`,
       :mod:`airflow.contrib.operators.emr_create_job_flow_operator`,
       :mod:`airflow.contrib.operators.emr_terminate_job_flow_operator`
     - :mod:`airflow.contrib.sensors.emr_base_sensor`,
       :mod:`airflow.contrib.sensors.emr_job_flow_sensor`,
       :mod:`airflow.contrib.sensors.emr_step_sensor`

   * - `Amazon Kinesis Data Firehose <https://aws.amazon.com/kinesis/data-firehose/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.kinesis`
     -
     -

   * - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.redshift`
     -
     - :mod:`airflow.providers.amazon.aws.sensors.redshift`

   * - `Amazon SageMaker <https://aws.amazon.com/sagemaker/>`__
     -
     - :mod:`airflow.contrib.hooks.sagemaker_hook`
     - :mod:`airflow.contrib.operators.sagemaker_base_operator`,
       :mod:`airflow.contrib.operators.sagemaker_endpoint_config_operator`,
       :mod:`airflow.contrib.operators.sagemaker_endpoint_operator`,
       :mod:`airflow.contrib.operators.sagemaker_model_operator`,
       :mod:`airflow.contrib.operators.sagemaker_training_operator`,
       :mod:`airflow.contrib.operators.sagemaker_transform_operator`,
       :mod:`airflow.contrib.operators.sagemaker_tuning_operator`
     - :mod:`airflow.contrib.sensors.sagemaker_base_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_endpoint_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_training_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_transform_sensor`,
       :mod:`airflow.contrib.sensors.sagemaker_tuning_sensor`

   * - `Amazon Simple Notification Service (SNS) <https://aws.amazon.com/sns/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.sns`
     - :mod:`airflow.providers.amazon.aws.operators.sns`
     -

   * - `Amazon Simple Queue Service (SQS) <https://aws.amazon.com/sns/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.sqs`
     - :mod:`airflow.providers.amazon.aws.operators.sqs`
     - :mod:`airflow.providers.amazon.aws.sensors.sqs`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.s3`
     - :mod:`airflow.operators.s3_file_transform_operator`,
       :mod:`airflow.contrib.operators.s3_copy_object_operator`,
       :mod:`airflow.contrib.operators.s3_delete_objects_operator`,
       :mod:`airflow.contrib.operators.s3_list_operator`
     - :mod:`airflow.sensors.s3_key_sensor`,
       :mod:`airflow.sensors.s3_prefix_sensor`

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data from/to Amazon Web Services.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * -
       .. _integration:AWS-Discovery-ref:

       All GCP services :ref:`[1] <integration:GCP-Discovery>`
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.operators.google_api_to_s3_transfer`

   * - `Amazon DataSync <https://aws.amazon.com/datasync/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - :doc:`How to use <howto/operator/amazon/aws/datasync>`
     - :mod:`airflow.providers.amazon.aws.operators.datasync`

   * - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.contrib.operators.dynamodb_to_s3`

   * - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.operators.redshift_to_s3_operator`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     -
     - :mod:`airflow.operators.s3_to_redshift_operator`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.s3_to_hive_operator`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.contrib.operators.s3_to_gcs_operator`,
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     -
     - :mod:`airflow.contrib.operators.s3_to_sftp_operator`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.contrib.operators.hive_to_dynamodb`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.operators.gcs_to_s3`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.contrib.operators.imap_attachment_to_s3_operator`

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.contrib.operators.mongo_to_s3`

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.contrib.operators.sftp_to_s3_operator`

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
     - :mod:`airflow.gcp.operators.automl`
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

   * - `Cloud Data Loss Prevention (DLP) <https://cloud.google.com/dlp/>`__
     -
     - :mod:`airflow.gcp.hooks.dlp`
     - :mod:`airflow.gcp.operators.dlp`
     -

   * - `Dataflow <https://cloud.google.com/dataflow/>`__
     -
     - :mod:`airflow.gcp.hooks.dataflow`
     - :mod:`airflow.gcp.operators.dataflow`
     -

   * - `Dataproc <https://cloud.google.com/dataproc/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.dataproc`
     - :mod:`airflow.providers.google.cloud.operators.dataproc`
     -

   * - `Datastore <https://cloud.google.com/datastore/>`__
     -
     - :mod:`airflow.gcp.hooks.datastore`
     - :mod:`airflow.gcp.operators.datastore`
     -

   * - `Cloud Functions <https://cloud.google.com/functions/>`__
     - :doc:`How to use <howto/operator/gcp/functions>`
     - :mod:`airflow.gcp.hooks.functions`
     - :mod:`airflow.gcp.operators.functions`
     -

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

   * - `Machine Learning Engine <https://cloud.google.com/ml-engine/>`__
     -
     - :mod:`airflow.gcp.hooks.mlengine`
     - :mod:`airflow.gcp.operators.mlengine`
     -

   * - `Cloud Memorystore <https://cloud.google.com/memorystore/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_memorystore>`
     - :mod:`airflow.gcp.hooks.cloud_memorystore`
     - :mod:`airflow.gcp.operators.cloud_memorystore`
     -

   * - `Natural Language <https://cloud.google.com/natural-language/>`__
     - :doc:`How to use <howto/operator/gcp/natural_language>`
     - :mod:`airflow.providers.google.cloud.hooks.natural_language`
     - :mod:`airflow.providers.google.cloud.operators.natural_language`
     -

   * - `Cloud Pub/Sub <https://cloud.google.com/pubsub/>`__
     - :doc:`How to use <howto/operator/gcp/pubsub>`
     - :mod:`airflow.providers.google.cloud.hooks.pubsub`
     - :mod:`airflow.providers.google.cloud.operators.pubsub`
     - :mod:`airflow.providers.google.cloud.sensors.pubsub`

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

   * - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs>`
     - :mod:`airflow.gcp.hooks.gcs`
     - :mod:`airflow.gcp.operators.gcs`
     - :mod:`airflow.gcp.sensors.gcs`

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
     - :mod:`airflow.providers.google.cloud.hooks.vision`
     - :mod:`airflow.providers.google.cloud.operators.vision`
     -


Transfer operators and hooks
''''''''''''''''''''''''''''

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

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.contrib.operators.s3_to_gcs_operator`,
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.cassandra_to_gcs`

   * - `Azure Data Lake Storage <https://azure.microsoft.com/pl-pl/services/storage/data-lake-storage/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.adls_to_gcs`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.operators.bigquery_to_mysql`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.bigquery_to_gcs`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.operators.bigquery_to_bigquery`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.operators.gcs_to_s3`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.operators.gcs_to_bq`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs_to_gcs>`,
       :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.operators.gcs_to_gcs`,
       :mod:`airflow.gcp.operators.cloud_storage_transfer_service`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Drive <https://www.google.com/drive/>`__
     -
     - :mod:`airflow.contrib.operators.gcs_to_gdrive_operator`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - SFTP
     - :doc:`How to use <howto/operator/gcp/gcs_to_sftp>`
     - :mod:`airflow.operators.gcs_to_sftp`

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

   * - SFTP
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/sftp_to_gcs>`
     - :mod:`airflow.providers.google.cloud.operators.sftp_to_gcs`

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

Other operators and hooks
'''''''''''''''''''''''''

.. list-table::
   :header-rows: 1

   * - Guide
     - Operators
     - Hooks

   * - :doc:`How to use <howto/operator/gcp/translate-speech>`
     - :mod:`airflow.gcp.operators.translate_speech`
     -

   * -
     -
     - :mod:`airflow.gcp.hooks.discovery_api`

.. _service:

Service integrations
--------------------

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `Atlassian Jira <https://www.atlassian.com/pl/software/jira>`__
     -
     - :mod:`airflow.contrib.hooks.jira_hook`
     - :mod:`airflow.contrib.operators.jira_operator`
     - :mod:`airflow.contrib.sensors.jira_sensor`

   * - `Databricks <https://databricks.com/>`__
     -
     - :mod:`airflow.contrib.hooks.databricks_hook`
     - :mod:`airflow.contrib.operators.databricks_operator`
     -

   * - `Datadog <https://www.datadoghq.com/>`__
     -
     - :mod:`airflow.contrib.hooks.datadog_hook`
     -
     - :mod:`airflow.contrib.sensors.datadog_sensor`

   * - `Pagerduty <https://www.pagerduty.com/>`__
     -
     - :mod:`airflow.contrib.hooks.pagerduty_hook`
     -
     -

   * - `Dingding <https://oapi.dingtalk.com>`__
     - :doc:`How to use <howto/operator/dingding>`
     - :mod:`airflow.contrib.hooks.dingding_hook`
     - :mod:`airflow.contrib.operators.dingding_operator`
     -

   * - `Discord <https://discordapp.com>`__
     -
     - :mod:`airflow.contrib.hooks.discord_webhook_hook`
     - :mod:`airflow.contrib.operators.discord_webhook_operator`
     -

   * - `Google Campaign Manager <https://developers.google.com/doubleclick-advertisers>`__
     - :doc:`How to use <howto/operator/gcp/campaign_manager>`
     - :mod:`airflow.providers.google.marketing_platform.hooks.campaign_manager`
     - :mod:`airflow.providers.google.marketing_platform.operators.campaign_manager`
     - :mod:`airflow.providers.google.marketing_platform.sensors.campaign_manager`

   * - `Google Display&Video 360 <https://marketingplatform.google.com/about/display-video-360/>`__
     - :doc:`How to use <howto/operator/gcp/display_video>`
     - :mod:`airflow.providers.google.marketing_platform.hooks.display_video`
     - :mod:`airflow.providers.google.marketing_platform.operators.display_video`
     - :mod:`airflow.providers.google.marketing_platform.sensors.display_video`

   * - `Google Drive <https://www.google.com/drive/>`__
     -
     - :mod:`airflow.contrib.hooks.gdrive_hook`
     -
     -

   * - `Google Search Ads 360 <https://marketingplatform.google.com/about/search-ads-360/>`__
     - :doc:`How to use <howto/operator/gcp/search_ads>`
     - :mod:`airflow.providers.google.marketing_platform.hooks.search_ads`
     - :mod:`airflow.providers.google.marketing_platform.operators.search_ads`
     - :mod:`airflow.providers.google.marketing_platform.sensors.search_ads`

   * - `Google Spreadsheet <https://www.google.com/intl/en/sheets/about/>`__
     -
     - :mod:`airflow.gcp.hooks.gsheets`
     -
     -

   * - `IBM Cloudant <https://www.ibm.com/cloud/cloudant>`__
     -
     - :mod:`airflow.contrib.hooks.cloudant_hook`
     -
     -

   * - `Jenkins <https://jenkins.io/>`__
     -
     - :mod:`airflow.contrib.hooks.jenkins_hook`
     - :mod:`airflow.contrib.operators.jenkins_job_trigger_operator`
     -

   * - `Opsgenie <https://www.opsgenie.com/>`__
     -
     - :mod:`airflow.contrib.hooks.opsgenie_alert_hook`
     - :mod:`airflow.contrib.operators.opsgenie_alert_operator`
     -

   * - `Qubole <https://www.qubole.com/>`__
     -
     - :mod:`airflow.contrib.hooks.qubole_hook`,
       :mod:`airflow.contrib.hooks.qubole_check_hook`
     - :mod:`airflow.contrib.operators.qubole_operator`,
       :mod:`airflow.contrib.operators.qubole_check_operator`
     - :mod:`airflow.contrib.sensors.qubole_sensor`

   * - `Salesforce <https://www.salesforce.com/>`__
     -
     - :mod:`airflow.contrib.hooks.salesforce_hook`
     -
     -

   * - `Segment <https://oapi.dingtalk.com>`__
     -
     - :mod:`airflow.contrib.hooks.segment_hook`
     - :mod:`airflow.contrib.operators.segment_track_event_operator`
     -

   * - `Slack <https://slack.com/>`__
     -
     - :mod:`airflow.hooks.slack_hook`,
       :mod:`airflow.contrib.hooks.slack_webhook_hook`
     - :mod:`airflow.operators.slack_operator`,
       :mod:`airflow.contrib.operators.slack_webhook_operator`
     -

   * - `Snowflake <https://www.snowflake.com/>`__
     -
     - :mod:`airflow.contrib.hooks.snowflake_hook`
     - :mod:`airflow.contrib.operators.snowflake_operator`
     -

   * - `Vertica <https://www.vertica.com/>`__
     -
     - :mod:`airflow.contrib.hooks.vertica_hook`
     - :mod:`airflow.contrib.operators.vertica_operator`
     -

   * - `Zendesk <https://www.zendesk.com/>`__
     -
     - :mod:`airflow.hooks.zendesk_hook`
     -
     -

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Drive <https://www.google.com/drive/>`__
     -
     - :mod:`airflow.contrib.operators.gcs_to_gdrive_operator`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.contrib.operators.vertica_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.contrib.operators.vertica_to_mysql`

.. _software:

Software integrations
---------------------

Software operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations using various software.

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `Celery <http://www.celeryproject.org/>`__
     -
     -
     -
     - :mod:`airflow.contrib.sensors.celery_queue_sensor`

   * - `Docker <https://docs.docker.com/install/>`__
     -
     - :mod:`airflow.hooks.docker_hook`
     - :mod:`airflow.operators.docker_operator`,
       :mod:`airflow.contrib.operators.docker_swarm_operator`
     -

   * - `GNU Bash <https://www.gnu.org/software/bash/>`__
     - :doc:`How to use <howto/operator/bash>`
     -
     - :mod:`airflow.operators.bash_operator`
     - :mod:`airflow.contrib.sensors.bash_sensor`

   * - `Kubernetes <https://kubernetes.io/>`__
     - :doc:`How to use <howto/operator/kubernetes>`
     -
     - :mod:`airflow.contrib.operators.kubernetes_pod_operator`
     -

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     -
     - :mod:`airflow.hooks.mssql_hook`
     - :mod:`airflow.operators.mssql_operator`
     -

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     -
     - :mod:`airflow.contrib.hooks.mongo_hook`
     -
     - :mod:`airflow.contrib.sensors.mongo_sensor`


   * - `MySQL <https://www.mysql.com/products/>`__
     -
     - :mod:`airflow.hooks.mysql_hook`
     - :mod:`airflow.operators.mysql_operator`
     -

   * - `OpenFaaS <https://www.openfaas.com/>`__
     -
     - :mod:`airflow.contrib.hooks.openfaas_hook`
     -
     -

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     -
     - :mod:`airflow.hooks.oracle_hook`
     - :mod:`airflow.operators.oracle_operator`
     -

   * - `Papermill <https://github.com/nteract/papermill>`__
     - :doc:`How to use <howto/operator/papermill>`
     -
     - :mod:`airflow.operators.papermill_operator`
     -

   * - `PostgresSQL <https://www.postgresql.org/>`__
     -
     - :mod:`airflow.hooks.postgres_hook`
     - :mod:`airflow.operators.postgres_operator`
     -

   * - `Presto <http://prestodb.github.io/>`__
     -
     - :mod:`airflow.hooks.presto_hook`
     - :mod:`airflow.operators.presto_check_operator`
     -

   * - `Python <https://www.python.org>`__
     -
     -
     - :mod:`airflow.operators.python_operator`
     - :mod:`airflow.contrib.sensors.python_sensor`

   * - `Redis <https://redis.io/>`__
     -
     - :mod:`airflow.contrib.hooks.redis_hook`
     - :mod:`airflow.contrib.operators.redis_publish_operator`
     - :mod:`airflow.contrib.sensors.redis_pub_sub_sensor`,
       :mod:`airflow.contrib.sensors.redis_key_sensor`

   * - `Samba <https://www.samba.org/>`__
     -
     - :mod:`airflow.hooks.samba_hook`
     -
     -

   * - `SQLite <https://www.sqlite.org/index.html>`__
     -
     - :mod:`airflow.hooks.sqlite_hook`
     - :mod:`airflow.operators.sqlite_operator`
     -


Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Samba <https://www.samba.org/>`__
     -
     - :mod:`airflow.operators.hive_to_samba_operator`

   * - `BigQuery <https://cloud.google.com/bigquery/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.operators.bigquery_to_mysql`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.mssql_to_hive`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.mssql_to_gcs`

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.contrib.operators.mongo_to_s3`

   * - `MySQL <https://www.mysql.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.operators.mysql_to_hive`

   * - `MySQL <https://www.mysql.com/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.mysql_to_gcs`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     -
     - :mod:`airflow.contrib.operators.oracle_to_azure_data_lake_transfer`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Oracle <https://www.oracle.com/pl/database/>`__
     -
     - :mod:`airflow.contrib.operators.oracle_to_oracle_transfer`

   * - `PostgresSQL <https://www.postgresql.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.postgres_to_gcs`

   * - `Presto <https://prestodb.github.io/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.operators.presto_to_mysql`

   * - SQL
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.sql_to_gcs`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.contrib.operators.vertica_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.contrib.operators.vertica_to_mysql`

.. _protocol:

Protocol integrations
---------------------

Protocol operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to perform various operations within various services using standardized
communication protocols or interface.

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `File Transfer Protocol (FTP) <https://tools.ietf.org/html/rfc114>`__
     -
     - :mod:`airflow.contrib.hooks.ftp_hook`
     -
     - :mod:`airflow.contrib.sensors.ftp_sensor`

   * - Filesystem
     -
     - :mod:`airflow.contrib.hooks.fs_hook`
     -
     - :mod:`airflow.contrib.sensors.file_sensor`

   * - `Hypertext Transfer Protocol (HTTP) <https://www.w3.org/Protocols/>`__
     -
     - :mod:`airflow.hooks.http_hook`
     - :mod:`airflow.operators.http_operator`
     - :mod:`airflow.sensors.http_sensor`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     -
     - :mod:`airflow.contrib.hooks.imap_hook`
     -
     - :mod:`airflow.contrib.sensors.imap_attachment_sensor`

   * - `Java Database Connectivity (JDBC) <https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/>`__
     -
     - :mod:`airflow.hooks.jdbc_hook`
     - :mod:`airflow.operators.jdbc_operator`
     -

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     -
     - :mod:`airflow.providers.sftp.hooks.sftp_hook`
     - :mod:`airflow.providers.sftp.operators.sftp_operator`
     - :mod:`airflow.providers.sftp.sensors.sftp_sensor`

   * - `Secure Shell (SSH) <https://tools.ietf.org/html/rfc4251>`__
     -
     - :mod:`airflow.contrib.hooks.ssh_hook`
     - :mod:`airflow.contrib.operators.ssh_operator`
     -

   * - `Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc821>`__
     -
     -
     - :mod:`airflow.operators.email_operator`
     -

   * - `Windows Remote Management (WinRM) <https://docs.microsoft.com/en-gb/windows/win32/winrm/portal>`__
     -
     - :mod:`airflow.contrib.hooks.winrm_hook`
     - :mod:`airflow.contrib.operators.winrm_operator`
     -

   * - `gRPC <https://grpc.io/>`__
     -
     - :mod:`airflow.contrib.hooks.grpc_hook`
     - :mod:`airflow.contrib.operators.grpc_operator`
     -

Transfer operators and hooks
''''''''''''''''''''''''''''

These integrations allow you to copy data.

.. list-table::
   :header-rows: 1

   * - Source
     - Destination
     - Guide
     - Operators

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     -
     - :mod:`airflow.contrib.operators.s3_to_sftp_operator`

   * - Filesystem
     - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     -
     - :mod:`airflow.contrib.operators.file_to_wasb`

   * - Filesystem
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.operators.local_to_gcs`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.contrib.operators.imap_attachment_to_s3_operator`

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.contrib.operators.sftp_to_s3_operator`
