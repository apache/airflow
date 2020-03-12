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

* :mod:`airflow.sensors.weekday_sensor`
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
     - :mod:`airflow.providers.apache.druid.hooks.druid`
     - :mod:`airflow.providers.apache.druid.operators.druid`,
       :mod:`airflow.providers.apache.druid.operators.druid_check`
     -

   * - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.hooks.hive`
     - :mod:`airflow.providers.apache.hive.operators.hive`,
       :mod:`airflow.providers.apache.hive.operators.hive_stats`
     - :mod:`airflow.providers.apache.hive.sensors.named_hive_partition`,
       :mod:`airflow.providers.apache.hive.sensors.hive_partition`,
       :mod:`airflow.providers.apache.hive.sensors.metastore_partition`

   * - `Apache Livy <https://livy.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.livy.hooks.livy`
     - :mod:`airflow.providers.apache.livy.operators.livy`
     - :mod:`airflow.providers.apache.livy.sensors.livy`

   * - `Apache Pig <https://pig.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.pig.hooks.pig`
     - :mod:`airflow.providers.apache.pig.operators.pig`
     -

   * - `Apache Pinot <https://pinot.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.pinot.hooks.pinot`
     -
     -

   * - `Apache Spark <https://spark.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.spark.hooks.spark_jdbc`,
       :mod:`airflow.providers.apache.spark.hooks.spark_jdbc_script`,
       :mod:`airflow.providers.apache.spark.hooks.spark_sql`,
       :mod:`airflow.providers.apache.spark.hooks.spark_submit`
     - :mod:`airflow.providers.apache.spark.operators.spark_jdbc`,
       :mod:`airflow.providers.apache.spark.operators.spark_sql`,
       :mod:`airflow.providers.apache.spark.operators.spark_submit`
     -

   * - `Apache Sqoop <https://sqoop.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.sqoop.hooks.sqoop`
     - :mod:`airflow.providers.apache.sqoop.operators.sqoop`
     -

   * - `Hadoop Distributed File System (HDFS) <https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html>`__
     -
     - :mod:`airflow.providers.apache.hdfs.hooks.hdfs`
     -
     - :mod:`airflow.providers.apache.hdfs.sensors.hdfs`

   * - `WebHDFS <https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html>`__
     -
     - :mod:`airflow.providers.apache.hdfs.hooks.webhdfs`
     -
     - :mod:`airflow.providers.apache.hdfs.sensors.web_hdfs`


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
     - :mod:`airflow.providers.apache.hive.operators.s3_to_hive`

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.cassandra_to_gcs`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.hive_to_dynamodb`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Apache Druid <https://druid.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.druid.operators.hive_to_druid`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.hive_to_mysql`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Samba <https://www.samba.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.hive_to_samba`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.mssql_to_hive`

   * - `MySQL <https://www.mysql.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.mysql_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.vertica_to_hive`

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
     - :mod:`airflow.providers.microsoft.azure.hooks.wasb`
     - :mod:`airflow.providers.microsoft.azure.operators.wasb_delete_blob`
     - :mod:`airflow.providers.microsoft.azure.sensors.wasb`

   * - `Azure Container Instances <https://azure.microsoft.com/en-us/services/container-instances/>`__
     - :mod:`airflow.providers.microsoft.azure.hooks.azure_container_instance`,
       :mod:`airflow.providers.microsoft.azure.hooks.azure_container_registry`,
       :mod:`airflow.providers.microsoft.azure.hooks.azure_container_volume`
     - :mod:`airflow.providers.microsoft.azure.operators.azure_container_instances`
     -

   * - `Azure Cosmos DB <https://azure.microsoft.com/en-us/services/cosmos-db/>`__
     - :mod:`airflow.providers.microsoft.azure.hooks.azure_cosmos`
     - :mod:`airflow.providers.microsoft.azure.operators.azure_cosmos`
     - :mod:`airflow.providers.microsoft.azure.sensors.azure_cosmos`

   * - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     - :mod:`airflow.providers.microsoft.azure.hooks.azure_data_lake`
     - :mod:`airflow.providers.microsoft.azure.operators.adls_list`
     -

   * - `Azure Data Explorer <https://azure.microsoft.com/en-us/services/data-explorer//>`__
     - :mod:`airflow.providers.microsoft.azure.hooks.adx`
     - :mod:`airflow.providers.microsoft.azure.operators.adx`
     -

   * - `Azure Files <https://azure.microsoft.com/en-us/services/storage/files/>`__
     - :mod:`airflow.providers.microsoft.azure.hooks.azure_fileshare`
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
     - :mod:`airflow.providers.google.cloud.operators.adls_to_gcs`

   * - Local
     - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     -
     - :mod:`airflow.providers.microsoft.azure.operators.file_to_wasb`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     -
     - :mod:`airflow.providers.microsoft.azure.operators.oracle_to_azure_data_lake_transfer`


.. _AWS:

AWS: Amazon Web Services
------------------------

Airflow has support for `Amazon Web Services <https://aws.amazon.com/>`__.

All hooks are based on :mod:`airflow.providers.amazon.aws.hooks.base_aws`.

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
     - :mod:`airflow.providers.amazon.aws.hooks.batch_client`,
       :mod:`airflow.providers.amazon.aws.hooks.batch_waiters`
     - :mod:`airflow.providers.amazon.aws.operators.batch`
     -

   * - `AWS DataSync <https://aws.amazon.com/datasync/>`__
     - :doc:`How to use <howto/operator/amazon/aws/datasync>`
     - :mod:`airflow.providers.amazon.aws.hooks.datasync`
     - :mod:`airflow.providers.amazon.aws.operators.datasync`
     -

   * - `AWS Glue Catalog <https://aws.amazon.com/glue/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.glue_catalog`
     -
     - :mod:`airflow.providers.amazon.aws.sensors.glue_catalog_partition`

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

   * - `Amazon CloudFormation <https://aws.amazon.com/cloudformation/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.cloud_formation`
     - :mod:`airflow.providers.amazon.aws.operators.cloud_formation`
     - :mod:`airflow.providers.amazon.aws.sensors.cloud_formation`

   * - `Amazon CloudWatch Logs <https://aws.amazon.com/cloudwatch/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.logs`
     -
     -

   * - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.aws_dynamodb`
     -
     -

   * - `Amazon EC2 <https://aws.amazon.com/ec2/>`__
     -
     -
     - :mod:`airflow.providers.amazon.aws.operators.ecs`
     -

   * - `Amazon EMR <https://aws.amazon.com/emr/>`__
     -
     - :mod:`airflow.providers.amazon.aws.hooks.emr`
     - :mod:`airflow.providers.amazon.aws.operators.emr_add_steps`,
       :mod:`airflow.providers.amazon.aws.operators.emr_create_job_flow`,
       :mod:`airflow.providers.amazon.aws.operators.emr_terminate_job_flow`,
       :mod:`airflow.providers.amazon.aws.operators.emr_modify_cluster`
     - :mod:`airflow.providers.amazon.aws.sensors.emr_base`,
       :mod:`airflow.providers.amazon.aws.sensors.emr_job_flow`,
       :mod:`airflow.providers.amazon.aws.sensors.emr_step`

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
     - :mod:`airflow.providers.amazon.aws.hooks.sagemaker`
     - :mod:`airflow.providers.amazon.aws.operators.sagemaker_base`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_endpoint_config`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_endpoint`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_model`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_training`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_transform`,
       :mod:`airflow.providers.amazon.aws.operators.sagemaker_tuning`
     - :mod:`airflow.providers.amazon.aws.sensors.sagemaker_base`,
       :mod:`airflow.providers.amazon.aws.sensors.sagemaker_endpoint`,
       :mod:`airflow.providers.amazon.aws.sensors.sagemaker_training`,
       :mod:`airflow.providers.amazon.aws.sensors.sagemaker_transform`,
       :mod:`airflow.providers.amazon.aws.sensors.sagemaker_tuning`

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
     - :mod:`airflow.providers.amazon.aws.operators.s3_file_transform`,
       :mod:`airflow.providers.amazon.aws.operators.s3_copy_object`,
       :mod:`airflow.providers.amazon.aws.operators.s3_delete_objects`,
       :mod:`airflow.providers.amazon.aws.operators.s3_list`
     - :mod:`airflow.providers.amazon.aws.sensors.s3_key`,
       :mod:`airflow.providers.amazon.aws.sensors.s3_prefix`

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
     - :mod:`airflow.providers.amazon.aws.operators.google_api_to_s3_transfer`

   * - `Amazon DataSync <https://aws.amazon.com/datasync/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - :doc:`How to use <howto/operator/amazon/aws/datasync>`
     - :mod:`airflow.providers.amazon.aws.operators.datasync`

   * - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.providers.amazon.aws.operators.dynamodb_to_s3`

   * - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.providers.amazon.aws.operators.redshift_to_s3`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Amazon Redshift <https://aws.amazon.com/redshift/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.s3_to_redshift`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Snowflake <https://snowflake.com/>`__
     -
     - :mod:`airflow.providers.snowflake.operators.s3_to_snowflake`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.s3_to_hive`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.providers.google.cloud.operators.s3_to_gcs`,
       :mod:`airflow.providers.google.cloud.operators.cloud_storage_transfer_service`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.s3_to_sftp`

   * - `Apache Hive <https://hive.apache.org/>`__
     - `Amazon DynamoDB <https://aws.amazon.com/dynamodb/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.hive_to_dynamodb`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.gcs_to_s3`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.imap_attachment_to_s3`

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.mongo_to_s3`

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.providers.amazon.aws.operators.sftp_to_s3`

:ref:`[1] <integration:AWS-Discovery-ref>` Those discovery-based operators use
:class:`airflow.providers.google.cloud.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate with Google
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

All hooks are based on :class:`airflow.providers.google.cloud.hooks.base.GoogleCloudBaseHook`.

.. note::
    You can learn how to use GCP integrations by analyzing the
    `source code <https://github.com/apache/airflow/tree/master/airflow/providers/google/cloud/example_dags/>`_ of the particular example DAGs.

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
     - :mod:`airflow.providers.google.cloud.hooks.automl`
     - :mod:`airflow.providers.google.cloud.operators.automl`
     -

   * - `BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.bigquery`
     - :mod:`airflow.providers.google.cloud.operators.bigquery`
     - :mod:`airflow.providers.google.cloud.sensors.bigquery`

   * - `BigQuery Data Transfer Service <https://cloud.google.com/bigquery/transfer/>`__
     - :doc:`How to use <howto/operator/gcp/bigquery_dts>`
     - :mod:`airflow.providers.google.cloud.hooks.bigquery_dts`
     - :mod:`airflow.providers.google.cloud.operators.bigquery_dts`
     - :mod:`airflow.providers.google.cloud.sensors.bigquery_dts`

   * - `Bigtable <https://cloud.google.com/bigtable/>`__
     - :doc:`How to use <howto/operator/gcp/bigtable>`
     - :mod:`airflow.providers.google.cloud.hooks.bigtable`
     - :mod:`airflow.providers.google.cloud.operators.bigtable`
     - :mod:`airflow.providers.google.cloud.sensors.bigtable`

   * - `Cloud Build <https://cloud.google.com/cloud-build/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_build>`
     - :mod:`airflow.providers.google.cloud.hooks.cloud_build`
     - :mod:`airflow.providers.google.cloud.operators.cloud_build`
     -

   * - `Compute Engine <https://cloud.google.com/compute/>`__
     - :doc:`How to use <howto/operator/gcp/compute>`
     - :mod:`airflow.providers.google.cloud.hooks.compute`
     - :mod:`airflow.providers.google.cloud.operators.compute`
     -

   * - `Cloud Data Loss Prevention (DLP) <https://cloud.google.com/dlp/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.dlp`
     - :mod:`airflow.providers.google.cloud.operators.dlp`
     -

   * - `DataFusion <https://cloud.google.com/data-fusion/>`__
     - :doc:`How to use <howto/operator/gcp/datafusion>`
     - :mod:`airflow.providers.google.cloud.hooks.datafusion`
     - :mod:`airflow.providers.google.cloud.operators.datafusion`
     -

   * - `Datacatalog <https://cloud.google.com/data-catalog>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.datacatalog`
     - :mod:`airflow.providers.google.cloud.operators.datacatalog`
     -

   * - `Dataflow <https://cloud.google.com/dataflow/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.dataflow`
     - :mod:`airflow.providers.google.cloud.operators.dataflow`
     -

   * - `Dataproc <https://cloud.google.com/dataproc/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.dataproc`
     - :mod:`airflow.providers.google.cloud.operators.dataproc`
     -

   * - `Datastore <https://cloud.google.com/datastore/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.datastore`
     - :mod:`airflow.providers.google.cloud.operators.datastore`
     -

   * - `Cloud Functions <https://cloud.google.com/functions/>`__
     - :doc:`How to use <howto/operator/gcp/functions>`
     - :mod:`airflow.providers.google.cloud.hooks.functions`
     - :mod:`airflow.providers.google.cloud.operators.functions`
     -

   * - `Cloud Key Management Service (KMS) <https://cloud.google.com/kms/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.kms`
     -
     -

   * - `Kubernetes Engine <https://cloud.google.com/kubernetes_engine/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.kubernetes_engine`
     - :mod:`airflow.providers.google.cloud.operators.kubernetes_engine`
     -

   * - `Machine Learning Engine <https://cloud.google.com/ml-engine/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.mlengine`
     - :mod:`airflow.providers.google.cloud.operators.mlengine`
     -

   * - `Cloud Memorystore <https://cloud.google.com/memorystore/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_memorystore>`
     - :mod:`airflow.providers.google.cloud.hooks.cloud_memorystore`
     - :mod:`airflow.providers.google.cloud.operators.cloud_memorystore`
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
     - :mod:`airflow.providers.google.cloud.hooks.spanner`
     - :mod:`airflow.providers.google.cloud.operators.spanner`
     -

   * - `Cloud Speech-to-Text <https://cloud.google.com/speech-to-text/>`__
     - :doc:`How to use <howto/operator/gcp/speech>`
     - :mod:`airflow.providers.google.cloud.hooks.speech_to_text`
     - :mod:`airflow.providers.google.cloud.operators.speech_to_text`
     -

   * - `Cloud SQL <https://cloud.google.com/sql/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_sql>`
     - :mod:`airflow.providers.google.cloud.hooks.cloud_sql`
     - :mod:`airflow.providers.google.cloud.operators.cloud_sql`
     -

   * - `Cloud Stackdriver <https://cloud.google.com/stackdriver>`__
     - :doc:`How to use <howto/operator/gcp/stackdriver>`
     - :mod:`airflow.providers.google.cloud.hooks.stackdriver`
     - :mod:`airflow.providers.google.cloud.operators.stackdriver`
     -

   * - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs>`
     - :mod:`airflow.providers.google.cloud.hooks.gcs`
     - :mod:`airflow.providers.google.cloud.operators.gcs`
     - :mod:`airflow.providers.google.cloud.sensors.gcs`

   * - `Storage Transfer Service <https://cloud.google.com/storage/transfer/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.providers.google.cloud.hooks.cloud_storage_transfer_service`
     - :mod:`airflow.providers.google.cloud.operators.cloud_storage_transfer_service`
     - :mod:`airflow.providers.google.cloud.sensors.cloud_storage_transfer_service`

   * - `Cloud Tasks <https://cloud.google.com/tasks/>`__
     -
     - :mod:`airflow.providers.google.cloud.hooks.tasks`
     - :mod:`airflow.providers.google.cloud.operators.tasks`
     -

   * - `Cloud Text-to-Speech <https://cloud.google.com/text-to-speech/>`__
     - :doc:`How to use <howto/operator/gcp/speech>`
     - :mod:`airflow.providers.google.cloud.hooks.text_to_speech`
     - :mod:`airflow.providers.google.cloud.operators.text_to_speech`
     -

   * - `Cloud Translation <https://cloud.google.com/translate/>`__
     - :doc:`How to use <howto/operator/gcp/translate>`
     - :mod:`airflow.providers.google.cloud.hooks.translate`
     - :mod:`airflow.providers.google.cloud.operators.translate`
     -

   * - `Cloud Video Intelligence <https://cloud.google.com/video_intelligence/>`__
     - :doc:`How to use <howto/operator/gcp/video_intelligence>`
     - :mod:`airflow.providers.google.cloud.hooks.video_intelligence`
     - :mod:`airflow.providers.google.cloud.operators.video_intelligence`
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
     - :mod:`airflow.providers.amazon.aws.operators.google_api_to_s3_transfer`

   * - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.providers.google.cloud.operators.s3_to_gcs`,
       :mod:`airflow.providers.google.cloud.operators.cloud_storage_transfer_service`

   * - `Apache Cassandra <http://cassandra.apache.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.cassandra_to_gcs`

   * - `Azure Data Lake Storage <https://azure.microsoft.com/pl-pl/services/storage/data-lake-storage/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.adls_to_gcs`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.bigquery_to_mysql`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.bigquery_to_gcs`

   * - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.bigquery_to_bigquery`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.gcs_to_s3`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google BigQuery <https://cloud.google.com/bigquery/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.gcs_to_bigquery`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/gcs_to_gcs>`,
       :doc:`How to use <howto/operator/gcp/cloud_storage_transfer_service>`
     - :mod:`airflow.providers.google.cloud.operators.gcs_to_gcs`,
       :mod:`airflow.providers.google.cloud.operators.cloud_storage_transfer_service`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - `Google Drive <https://www.google.com/drive/>`__
     -
     - :mod:`airflow.providers.google.suite.operators.gcs_to_gdrive`

   * - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - SFTP
     - :doc:`How to use <howto/operator/gcp/gcs_to_sftp>`
     - :mod:`airflow.providers.google.cloud.operators.gcs_to_sftp`

   * - Local
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.local_to_gcs`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.mssql_to_gcs`

   * - `MySQL <https://www.mysql.com/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.mysql_to_gcs`

   * - `PostgresSQL <https://www.postgresql.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.postgres_to_gcs`

   * - SFTP
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     - :doc:`How to use <howto/operator/gcp/sftp_to_gcs>`
     - :mod:`airflow.providers.google.cloud.operators.sftp_to_gcs`

   * - SQL
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.sql_to_gcs`


.. _integration:GCP-Discovery:

:ref:`[1] <integration:GCP-Discovery-ref>` Those discovery-based operators use
:class:`airflow.providers.google.cloud.hooks.discovery_api.GoogleDiscoveryApiHook` to communicate with Google
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
     - :mod:`airflow.providers.google.cloud.operators.translate_speech`
     -

   * -
     -
     - :mod:`airflow.providers.google.cloud.hooks.discovery_api`

.. _service:


Yandex.Cloud
--------------------------

Airflow has a limited support for the `Yandex.Cloud <https://cloud.yandex.com/>`__.

See the :doc:`Yandex.Cloud connection type <howto/connection/yandexcloud>` documentation to
configure connections to Yandex.Cloud.

All hooks are based on :class:`airflow.contrib.hooks.yandexcloud_base_hook.YandexCloudBaseHook`.

.. note::
    You can learn how to use Yandex.Cloud integrations by analyzing the
    `example DAG <https://github.com/apache/airflow/tree/master/airflow/contrib/example_dags/example_yandexcloud_dataproc.py>`_

Service operators and hooks
'''''''''''''''''''''''''''

These integrations allow you to perform various operations within the Yandex.Cloud.

..
  PLEASE KEEP THE ALPHABETICAL ORDER OF THE LIST BELOW, BUT OMIT THE "Cloud" PREFIX

.. list-table::
   :header-rows: 1

   * - Service name
     - Guide
     - Hook
     - Operators
     - Sensors

   * - `Base Classes <https://cloud.yandex.com>`__
     - :doc:`How to use <howto/operator/yandexcloud>`
     - :mod:`airflow.providers.yandex.hooks.yandexcloud_base_hook`
     - :mod:`airflow.providers.yandex.operators.yandexcloud_base_operator`
     -

   * - `Data Proc <https://cloud.yandex.com/services/data-proc>`__
     - :doc:`How to use <howto/operator/yandexcloud>`
     - :mod:`airflow.providers.yandex.hooks.yandexcloud_dataproc`
     - :mod:`airflow.providers.yandex.operators.yandexcloud_dataproc`
     -

.. _yc_service:

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
     - :mod:`airflow.providers.jira.hooks.jira`
     - :mod:`airflow.providers.jira.operators.jira`
     - :mod:`airflow.providers.jira.sensors.jira`

   * - `Databricks <https://databricks.com/>`__
     -
     - :mod:`airflow.providers.databricks.hooks.databricks`
     - :mod:`airflow.providers.databricks.operators.databricks`
     -

   * - `Datadog <https://www.datadoghq.com/>`__
     -
     - :mod:`airflow.providers.datadog.hooks.datadog`
     -
     - :mod:`airflow.providers.datadog.sensors.datadog`

   * - `Pagerduty <https://www.pagerduty.com/>`__
     -
     - :mod:`airflow.providers.pagerduty.hooks.pagerduty`
     -
     -

   * - `Dingding <https://oapi.dingtalk.com>`__
     - :doc:`How to use <howto/operator/dingding>`
     - :mod:`airflow.providers.dingding.hooks.dingding`
     - :mod:`airflow.providers.dingding.operators.dingding`
     -

   * - `Discord <https://discordapp.com>`__
     -
     - :mod:`airflow.providers.discord.hooks.discord_webhook`
     - :mod:`airflow.providers.discord.operators.discord_webhook`
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
     - :mod:`airflow.providers.google.suite.hooks.drive`
     -
     -

   * - `Google Search Ads 360 <https://marketingplatform.google.com/about/search-ads-360/>`__
     - :doc:`How to use <howto/operator/gcp/search_ads>`
     - :mod:`airflow.providers.google.marketing_platform.hooks.search_ads`
     - :mod:`airflow.providers.google.marketing_platform.operators.search_ads`
     - :mod:`airflow.providers.google.marketing_platform.sensors.search_ads`

   * - `Google Spreadsheet <https://www.google.com/intl/en/sheets/about/>`__
     -
     - :mod:`airflow.providers.google.suite.hooks.sheets`
     -
     -

   * - `IBM Cloudant <https://www.ibm.com/cloud/cloudant>`__
     -
     - :mod:`airflow.providers.cloudant.hooks.cloudant`
     -
     -

   * - `Jenkins <https://jenkins.io/>`__
     -
     - :mod:`airflow.providers.jenkins.hooks.jenkins`
     - :mod:`airflow.providers.jenkins.operators.jenkins_job_trigger`
     -

   * - `Opsgenie <https://www.opsgenie.com/>`__
     -
     - :mod:`airflow.providers.opsgenie.hooks.opsgenie_alert`
     - :mod:`airflow.providers.opsgenie.operators.opsgenie_alert`
     -

   * - `Qubole <https://www.qubole.com/>`__
     -
     - :mod:`airflow.providers.qubole.hooks.qubole`,
       :mod:`airflow.providers.qubole.hooks.qubole_check`
     - :mod:`airflow.providers.qubole.operators.qubole`,
       :mod:`airflow.providers.qubole.operators.qubole_check`
     - :mod:`airflow.providers.qubole.sensors.qubole`

   * - `Salesforce <https://www.salesforce.com/>`__
     -
     - :mod:`airflow.providers.salesforce.hooks.salesforce`,
       :mod:`airflow.providers.salesforce.hooks.tableau`
     - :mod:`airflow.providers.salesforce.operators.tableau_refresh_workbook`
     - :mod:`airflow.providers.salesforce.sensors.tableau_job_status`

   * - `Segment <https://oapi.dingtalk.com>`__
     -
     - :mod:`airflow.providers.segment.hooks.segment`
     - :mod:`airflow.providers.segment.operators.segment_track_event`
     -

   * - `Slack <https://slack.com/>`__
     -
     - :mod:`airflow.providers.slack.hooks.slack`,
       :mod:`airflow.providers.slack.hooks.slack_webhook`
     - :mod:`airflow.providers.slack.operators.slack`,
       :mod:`airflow.providers.slack.operators.slack_webhook`
     -

   * - `Snowflake <https://www.snowflake.com/>`__
     -
     - :mod:`airflow.providers.snowflake.hooks.snowflake`
     - :mod:`airflow.providers.snowflake.operators.snowflake`
     -

   * - `Vertica <https://www.vertica.com/>`__
     -
     - :mod:`airflow.providers.vertica.hooks.vertica`
     - :mod:`airflow.providers.vertica.operators.vertica`
     -

   * - `Zendesk <https://www.zendesk.com/>`__
     -
     - :mod:`airflow.providers.zendesk.hooks.zendesk`
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
     - :mod:`airflow.providers.google.suite.operators.gcs_to_gdrive`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.vertica_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.mysql.operators.vertica_to_mysql`

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
     - :mod:`airflow.providers.celery.sensors.celery_queue`

   * - `Docker <https://docs.docker.com/install/>`__
     -
     - :mod:`airflow.providers.docker.hooks.docker`
     - :mod:`airflow.providers.docker.operators.docker`,
       :mod:`airflow.providers.docker.operators.docker_swarm`
     -

   * - `Elasticsearch <https://https://www.elastic.co/elasticsearch>`__
     -
     - :mod:`airflow.providers.elasticsearch.hooks.elasticsearch`
     -
     -

   * - `GNU Bash <https://www.gnu.org/software/bash/>`__
     - :doc:`How to use <howto/operator/bash>`
     -
     - :mod:`airflow.operators.bash`
     - :mod:`airflow.sensors.bash`

   * - `Kubernetes <https://kubernetes.io/>`__
     - :doc:`How to use <howto/operator/kubernetes>`
     - :mod:`airflow.providers.cncf.kubernetes.hooks.kubernetes`
     - :mod:`airflow.providers.cncf.kubernetes.operators.kubernetes_pod`
       :mod:`airflow.providers.cncf.kubernetes.operators.spark_kubernetes`
     - :mod:`airflow.providers.cncf.kubernetes.sensors.spark_kubernetes`


   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     -
     - :mod:`airflow.providers.microsoft.mssql.hooks.mssql`,
       :mod:`airflow.providers.odbc.hooks.odbc`
     - :mod:`airflow.providers.microsoft.mssql.operators.mssql`
     -


   * - `ODBC <https://github.com/mkleehammer/pyodbc/wiki>`__
     -
     - :mod:`airflow.providers.odbc.hooks.odbc`
     -
     -

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     -
     - :mod:`airflow.providers.mongo.hooks.mongo`
     -
     - :mod:`airflow.providers.mongo.sensors.mongo`


   * - `MySQL <https://www.mysql.com/products/>`__
     - :mod:`airflow.providers.mysql.operators.mysql`
     - :mod:`airflow.providers.mysql.hooks.mysql`
     - :mod:`airflow.providers.mssql.operators.mysql`
     -

   * - `OpenFaaS <https://www.openfaas.com/>`__
     -
     - :mod:`airflow.providers.openfass.hooks.openfaas`
     -
     -

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     -
     - :mod:`airflow.providers.oracle.hooks.oracle`
     - :mod:`airflow.providers.oracle.operators.oracle`
     -

   * - `Papermill <https://github.com/nteract/papermill>`__
     - :doc:`How to use <howto/operator/papermill>`
     -
     - :mod:`airflow.providers.papermill.operators.papermill`
     -

   * - `PostgresSQL <https://www.postgresql.org/>`__
     -
     - :mod:`airflow.providers.postgres.hooks.postgres`
     - :mod:`airflow.providers.postgres.operators.postgres`
     -

   * - `Presto <http://prestodb.github.io/>`__
     -
     - :mod:`airflow.providers.presto.hooks.presto`
     - :mod:`airflow.providers.presto.operators.presto_check`
     -

   * - `Python <https://www.python.org>`__
     -
     -
     - :mod:`airflow.operators.python`
     - :mod:`airflow.sensors.python`

   * - `Redis <https://redis.io/>`__
     -
     - :mod:`airflow.providers.redis.hooks.redis`
     - :mod:`airflow.providers.redis.operators.redis_publish`
     - :mod:`airflow.providers.redis.sensors.redis_pub_sub`,
       :mod:`airflow.providers.redis.sensors.redis_key`

   * - `Samba <https://www.samba.org/>`__
     -
     - :mod:`airflow.providers.samba.hooks.samba`
     -
     -

   * - `Singularity <https://sylabs.io/guides/latest/user-guide/>`__
     -
     -
     - :mod:`airflow.providers.singularity.operators.singularity`
     -

   * - `SQLite <https://www.sqlite.org/index.html>`__
     -
     - :mod:`airflow.providers.sqlite.hooks.sqlite`
     - :mod:`airflow.providers.sqlite.operators.sqlite`
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
     - :mod:`airflow.providers.apache.hive.operators.hive_to_samba`

   * - `BigQuery <https://cloud.google.com/bigquery/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.bigquery_to_mysql`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.mssql_to_hive`

   * - `Microsoft SQL Server (MSSQL) <https://www.microsoft.com/pl-pl/sql-server/sql-server-downloads>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.mssql_to_gcs`

   * - `MongoDB <https://www.mongodb.com/what-is-mongodb>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.mongo_to_s3`

   * - `MySQL <https://www.mysql.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.mysql_to_hive`

   * - `MySQL <https://www.mysql.com/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.mysql_to_gcs`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Azure Data Lake Storage <https://azure.microsoft.com/en-us/services/storage/data-lake-storage/>`__
     -
     - :mod:`airflow.providers.microsoft.azure.operators.oracle_to_azure_data_lake_transfer`

   * - `Oracle <https://www.oracle.com/pl/database/>`__
     - `Oracle <https://www.oracle.com/pl/database/>`__
     -
     - :mod:`airflow.providers.oracle.operators.oracle_to_oracle_transfer`

   * - `PostgresSQL <https://www.postgresql.org/>`__
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.postgres_to_gcs`

   * - `Presto <https://prestodb.github.io/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.mysql.operators.presto_to_mysql`

   * - SQL
     - `Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.sql_to_gcs`

   * - `Vertica <https://www.vertica.com/>`__
     - `Apache Hive <https://hive.apache.org/>`__
     -
     - :mod:`airflow.providers.apache.hive.operators.vertica_to_hive`

   * - `Vertica <https://www.vertica.com/>`__
     - `MySQL <https://www.mysql.com/>`__
     -
     - :mod:`airflow.providers.mysql.operators.vertica_to_mysql`

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
     - :mod:`airflow.providers.ftp.hooks.ftp`
     -
     - :mod:`airflow.providers.ftp.sensors.ftp`

   * - Filesystem
     -
     - :mod:`airflow.hooks.filesystem`
     -
     - :mod:`airflow.sensors.filesystem`

   * - `Hypertext Transfer Protocol (HTTP) <https://www.w3.org/Protocols/>`__
     -
     - :mod:`airflow.providers.http.hooks.http`
     - :mod:`airflow.providers.http.operators.http`
     - :mod:`airflow.providers.http.sensors.http`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     -
     - :mod:`airflow.providers.imap.hooks.imap`
     -
     - :mod:`airflow.providers.imap.sensors.imap_attachment`

   * - `Java Database Connectivity (JDBC) <https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/>`__
     -
     - :mod:`airflow.providers.jdbc.hooks.jdbc`
     - :mod:`airflow.providers.jdbc.operators.jdbc`
     -

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     -
     - :mod:`airflow.providers.sftp.hooks.sftp`
     - :mod:`airflow.providers.sftp.operators.sftp`
     - :mod:`airflow.providers.sftp.sensors.sftp`

   * - `Secure Shell (SSH) <https://tools.ietf.org/html/rfc4251>`__
     -
     - :mod:`airflow.providers.ssh.hooks.ssh`
     - :mod:`airflow.providers.ssh.operators.ssh`
     -

   * - `Simple Mail Transfer Protocol (SMTP) <https://tools.ietf.org/html/rfc821>`__
     -
     -
     - :mod:`airflow.providers.email.operators.email`
     -

   * - `Windows Remote Management (WinRM) <https://docs.microsoft.com/en-gb/windows/win32/winrm/portal>`__
     -
     - :mod:`airflow.providers.microsoft.winrm.hooks.winrm`
     - :mod:`airflow.providers.microsoft.winrm.operators.winrm`
     -

   * - `gRPC <https://grpc.io/>`__
     -
     - :mod:`airflow.providers.grpc.hooks.grpc`
     - :mod:`airflow.providers.grpc.operators.grpc`
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
     - :mod:`airflow.providers.amazon.aws.operators.s3_to_sftp`

   * - Filesystem
     - `Azure Blob Storage <https://azure.microsoft.com/en-us/services/storage/blobs/>`__
     -
     - :mod:`airflow.providers.microsoft.azure.operators.file_to_wasb`

   * - Filesystem
     - `Google Cloud Storage (GCS) <https://cloud.google.com/gcs/>`__
     -
     - :mod:`airflow.providers.google.cloud.operators.local_to_gcs`

   * - `Internet Message Access Protocol (IMAP) <https://tools.ietf.org/html/rfc3501>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`__
     -
     - :mod:`airflow.providers.amazon.aws.operators.imap_attachment_to_s3`

   * - `SSH File Transfer Protocol (SFTP) <https://tools.ietf.org/wg/secsh/draft-ietf-secsh-filexfer/>`__
     - `Amazon Simple Storage Service (S3) <https://aws.amazon.com/s3/>`_
     -
     - :mod:`airflow.providers.amazon.aws.operators.sftp_to_s3`
