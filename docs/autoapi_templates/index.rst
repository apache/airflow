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



Python API Reference
====================

.. _pythonapi:operators:

Operators
---------
Operators allow for generation of certain types of tasks that become nodes in
the DAG when instantiated. All operators derive from :class:`~airflow.models.BaseOperator` and
inherit many attributes and methods that way.

There are 3 main types of operators:

- Operators that performs an **action**, or tell another system to
  perform an action
- **Transfer** operators move data from one system to another
- **Sensors** are a certain type of operator that will keep running until a
  certain criterion is met. Examples include a specific file landing in HDFS or
  S3, a partition appearing in Hive, or a specific time of the day. Sensors
  are derived from :class:`~airflow.sensors.base_sensor_operator.BaseSensorOperator` and run a poke
  method at a specified :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.poke_interval` until it returns ``True``.

BaseOperator
''''''''''''
All operators are derived from :class:`~airflow.models.BaseOperator` and acquire much
functionality through inheritance. Since this is the core of the engine,
it's worth taking the time to understand the parameters of :class:`~airflow.models.BaseOperator`
to understand the primitive features that can be leveraged in your
DAGs.

BaseSensorOperator
''''''''''''''''''
All sensors are derived from :class:`~airflow.sensors.base_sensor_operator.BaseSensorOperator`. All sensors inherit
the :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.timeout` and :attr:`~airflow.sensors.base_sensor_operator.BaseSensorOperator.poke_interval` on top of the :class:`~airflow.models.BaseOperator`
attributes.

Operators packages
''''''''''''''''''
All operators are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/providers/amazon/aws/operators/athena/index

  airflow/providers/amazon/aws/operators/batch/index

  airflow/providers/amazon/aws/operators/cloud_formation/index

  airflow/providers/amazon/aws/operators/datasync/index

  airflow/providers/amazon/aws/operators/dynamodb_to_s3/index

  airflow/providers/amazon/aws/operators/ecs/index

  airflow/providers/amazon/aws/operators/emr_add_steps/index

  airflow/providers/amazon/aws/operators/emr_create_job_flow/index

  airflow/providers/amazon/aws/operators/emr_modify_cluster/index

  airflow/providers/amazon/aws/operators/emr_terminate_job_flow/index

  airflow/providers/amazon/aws/operators/gcs_to_s3/index

  airflow/providers/amazon/aws/operators/google_api_to_s3_transfer/index

  airflow/providers/amazon/aws/operators/hive_to_dynamodb/index

  airflow/providers/amazon/aws/operators/imap_attachment_to_s3/index

  airflow/providers/amazon/aws/operators/mongo_to_s3/index

  airflow/providers/amazon/aws/operators/redshift_to_s3/index

  airflow/providers/amazon/aws/operators/s3_copy_object/index

  airflow/providers/amazon/aws/operators/s3_delete_objects/index

  airflow/providers/amazon/aws/operators/s3_file_transform/index

  airflow/providers/amazon/aws/operators/s3_list/index

  airflow/providers/amazon/aws/operators/s3_to_redshift/index

  airflow/providers/amazon/aws/operators/s3_to_sftp/index

  airflow/providers/amazon/aws/operators/sagemaker_base/index

  airflow/providers/amazon/aws/operators/sagemaker_endpoint/index

  airflow/providers/amazon/aws/operators/sagemaker_endpoint_config/index

  airflow/providers/amazon/aws/operators/sagemaker_model/index

  airflow/providers/amazon/aws/operators/sagemaker_training/index

  airflow/providers/amazon/aws/operators/sagemaker_transform/index

  airflow/providers/amazon/aws/operators/sagemaker_tuning/index

  airflow/providers/amazon/aws/operators/sftp_to_s3/index

  airflow/providers/amazon/aws/operators/sns/index

  airflow/providers/amazon/aws/operators/sqs/index

  airflow/providers/apache/druid/operators/druid/index

  airflow/providers/apache/druid/operators/druid_check/index

  airflow/providers/apache/druid/operators/hive_to_druid/index

  airflow/providers/apache/hive/operators/hive/index

  airflow/providers/apache/hive/operators/hive_stats/index

  airflow/providers/apache/hive/operators/hive_to_mysql/index

  airflow/providers/apache/hive/operators/hive_to_samba/index

  airflow/providers/apache/hive/operators/mssql_to_hive/index

  airflow/providers/apache/hive/operators/mysql_to_hive/index

  airflow/providers/apache/hive/operators/s3_to_hive/index

  airflow/providers/apache/hive/operators/vertica_to_hive/index

  airflow/providers/apache/pig/operators/pig/index

  airflow/providers/apache/spark/operators/spark_jdbc/index

  airflow/providers/apache/spark/operators/spark_sql/index

  airflow/providers/apache/spark/operators/spark_submit/index

  airflow/providers/apache/sqoop/operators/sqoop/index

  airflow/providers/cncf/kubernetes/operators/kubernetes_pod/index

  airflow/providers/databricks/operators/databricks/index

  airflow/providers/dingding/operators/dingding/index

  airflow/providers/discord/operators/discord_webhook/index

  airflow/providers/docker/operators/docker_operator/index

  airflow/providers/docker/operators/docker_swarm/index

  airflow/providers/email/operators/email/index

  airflow/providers/google/cloud/operators/adls_to_gcs/index

  airflow/providers/google/cloud/operators/automl/index

  airflow/providers/google/cloud/operators/bigquery/index

  airflow/providers/google/cloud/operators/bigquery_dts/index

  airflow/providers/google/cloud/operators/bigquery_to_bigquery/index

  airflow/providers/google/cloud/operators/bigquery_to_gcs/index

  airflow/providers/google/cloud/operators/bigquery_to_mysql/index

  airflow/providers/google/cloud/operators/bigtable/index

  airflow/providers/google/cloud/operators/cassandra_to_gcs/index

  airflow/providers/google/cloud/operators/cloud_build/index

  airflow/providers/google/cloud/operators/cloud_memorystore/index

  airflow/providers/google/cloud/operators/cloud_sql/index

  airflow/providers/google/cloud/operators/cloud_storage_transfer_service/index

  airflow/providers/google/cloud/operators/compute/index

  airflow/providers/google/cloud/operators/dataflow/index

  airflow/providers/google/cloud/operators/dataproc/index

  airflow/providers/google/cloud/operators/datastore/index

  airflow/providers/google/cloud/operators/dlp/index

  airflow/providers/google/cloud/operators/functions/index

  airflow/providers/google/cloud/operators/gcs/index

  airflow/providers/google/cloud/operators/gcs_to_bigquery/index

  airflow/providers/google/cloud/operators/gcs_to_gcs/index

  airflow/providers/google/cloud/operators/gcs_to_sftp/index

  airflow/providers/google/cloud/operators/kubernetes_engine/index

  airflow/providers/google/cloud/operators/local_to_gcs/index

  airflow/providers/google/cloud/operators/mlengine/index

  airflow/providers/google/cloud/operators/mssql_to_gcs/index

  airflow/providers/google/cloud/operators/mysql_to_gcs/index

  airflow/providers/google/cloud/operators/natural_language/index

  airflow/providers/google/cloud/operators/postgres_to_gcs/index

  airflow/providers/google/cloud/operators/pubsub/index

  airflow/providers/google/cloud/operators/s3_to_gcs/index

  airflow/providers/google/cloud/operators/sftp_to_gcs/index

  airflow/providers/google/cloud/operators/spanner/index

  airflow/providers/google/cloud/operators/speech_to_text/index

  airflow/providers/google/cloud/operators/sql_to_gcs/index

  airflow/providers/google/cloud/operators/tasks/index

  airflow/providers/google/cloud/operators/text_to_speech/index

  airflow/providers/google/cloud/operators/translate/index

  airflow/providers/google/cloud/operators/translate_speech/index

  airflow/providers/google/cloud/operators/video_intelligence/index

  airflow/providers/google/cloud/operators/vision/index

  airflow/providers/google/marketing_platform/operators/campaign_manager/index

  airflow/providers/google/marketing_platform/operators/display_video/index

  airflow/providers/google/marketing_platform/operators/search_ads/index

  airflow/providers/google/suite/operators/gcs_to_gdrive_operator/index

  airflow/providers/grpc/operators/airflow_grpc/index

  airflow/providers/http/operators/http/index

  airflow/providers/jdbc/operators/jdbc/index

  airflow/providers/jenkins/operators/jenkins_job_trigger/index

  airflow/providers/jira/operators/jira/index

  airflow/providers/microsoft/azure/operators/adls_list/index

  airflow/providers/microsoft/azure/operators/azure_container_instances/index

  airflow/providers/microsoft/azure/operators/azure_cosmos/index

  airflow/providers/microsoft/azure/operators/file_to_wasb/index

  airflow/providers/microsoft/azure/operators/oracle_to_azure_data_lake_transfer/index

  airflow/providers/microsoft/azure/operators/wasb_delete_blob/index

  airflow/providers/microsoft/mssql/operators/mssql/index

  airflow/providers/microsoft/winrm/operators/airflow_winrm/index

  airflow/providers/mysql/operators/mysql/index

  airflow/providers/mysql/operators/presto_to_mysql/index

  airflow/providers/mysql/operators/vertica_to_mysql/index

  airflow/providers/opsgenie/operators/opsgenie_alert/index

  airflow/providers/oracle/operators/oracle/index

  airflow/providers/oracle/operators/oracle_to_oracle_transfer/index

  airflow/providers/papermill/operators/papermill/index

  airflow/providers/postgres/operators/postgres/index

  airflow/providers/presto/operators/presto_check/index

  airflow/providers/qubole/operators/qubole/index

  airflow/providers/qubole/operators/qubole_check/index

  airflow/providers/redis/operators/redis_publish/index

  airflow/providers/segment/operators/segment_track_event/index

  airflow/providers/sftp/operators/sftp/index

  airflow/providers/slack/operators/slack/index

  airflow/providers/slack/operators/slack_webhook/index

  airflow/providers/snowflake/operators/s3_to_snowflake/index

  airflow/providers/snowflake/operators/snowflake/index

  airflow/providers/sqlite/operators/sqlite/index

  airflow/providers/ssh/operators/ssh/index

  airflow/providers/vertica/operators/vertica/index


.. _pythonapi:hooks:

Sensor packages
''''''''''''''''''
All sensors are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/providers/amazon/aws/sensors/athena/index

  airflow/providers/amazon/aws/sensors/cloud_formation/index

  airflow/providers/amazon/aws/sensors/emr_base/index

  airflow/providers/amazon/aws/sensors/emr_job_flow/index

  airflow/providers/amazon/aws/sensors/emr_step/index

  airflow/providers/amazon/aws/sensors/glue_catalog_partition/index

  airflow/providers/amazon/aws/sensors/redshift/index

  airflow/providers/amazon/aws/sensors/s3_key/index

  airflow/providers/amazon/aws/sensors/s3_prefix/index

  airflow/providers/amazon/aws/sensors/sagemaker_base/index

  airflow/providers/amazon/aws/sensors/sagemaker_endpoint/index

  airflow/providers/amazon/aws/sensors/sagemaker_training/index

  airflow/providers/amazon/aws/sensors/sagemaker_transform/index

  airflow/providers/amazon/aws/sensors/sagemaker_tuning/index

  airflow/providers/amazon/aws/sensors/sqs/index

  airflow/providers/apache/cassandra/sensors/record/index

  airflow/providers/apache/cassandra/sensors/table/index

  airflow/providers/apache/hdfs/sensors/hdfs/index

  airflow/providers/apache/hdfs/sensors/web_hdfs/index

  airflow/providers/apache/hive/sensors/hive_partition/index

  airflow/providers/apache/hive/sensors/metastore_partition/index

  airflow/providers/apache/hive/sensors/named_hive_partition/index

  airflow/providers/celery/sensors/celery_queue/index

  airflow/providers/datadog/sensors/airflow_datadog/index

  airflow/providers/ftp/sensors/ftp/index

  airflow/providers/google/cloud/sensors/bigquery/index

  airflow/providers/google/cloud/sensors/bigquery_dts/index

  airflow/providers/google/cloud/sensors/bigtable/index

  airflow/providers/google/cloud/sensors/cloud_storage_transfer_service/index

  airflow/providers/google/cloud/sensors/gcs/index

  airflow/providers/google/cloud/sensors/pubsub/index

  airflow/providers/google/marketing_platform/sensors/campaign_manager/index

  airflow/providers/google/marketing_platform/sensors/display_video/index

  airflow/providers/google/marketing_platform/sensors/search_ads/index

  airflow/providers/http/sensors/http/index

  airflow/providers/imap/sensors/imap_attachment/index

  airflow/providers/jira/sensors/airflow_jira/index

  airflow/providers/microsoft/azure/sensors/azure_cosmos/index

  airflow/providers/microsoft/azure/sensors/wasb/index

  airflow/providers/mongo/sensors/mongo/index

  airflow/providers/qubole/sensors/qubole/index

  airflow/providers/redis/sensors/redis_key/index

  airflow/providers/redis/sensors/redis_pub_sub/index

  airflow/providers/sftp/sensors/sftp/index

Hooks
-----
Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators. All hooks
are derived from :class:`~airflow.hooks.base_hook.BaseHook`.

Hooks packages
''''''''''''''
All hooks are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/providers/amazon/aws/hooks/athena/index.rst:

  airflow/providers/amazon/aws/hooks/aws_dynamodb_hook/index

  airflow/providers/amazon/aws/hooks/aws_hook/index

  airflow/providers/amazon/aws/hooks/batch_client/index

  airflow/providers/amazon/aws/hooks/batch_waiters/index

  airflow/providers/amazon/aws/hooks/cloud_formation/index

  airflow/providers/amazon/aws/hooks/datasync/index

  airflow/providers/amazon/aws/hooks/emr/index

  airflow/providers/amazon/aws/hooks/glue_catalog/index

  airflow/providers/amazon/aws/hooks/kinesis/index

  airflow/providers/amazon/aws/hooks/lambda_function/index

  airflow/providers/amazon/aws/hooks/logs/index

  airflow/providers/amazon/aws/hooks/redshift/index

  airflow/providers/amazon/aws/hooks/s3/index

  airflow/providers/amazon/aws/hooks/sagemaker/index

  airflow/providers/amazon/aws/hooks/sns/index

  airflow/providers/amazon/aws/hooks/sqs/index

  airflow/providers/apache/cassandra/hooks/airflow_cassandra/index

  airflow/providers/apache/druid/hooks/druid/index

  airflow/providers/apache/hdfs/hooks/airflow_hdfs/index

  airflow/providers/apache/hdfs/hooks/webhdfs/index

  airflow/providers/apache/hive/hooks/hive/index

  airflow/providers/apache/pig/hooks/pig/index

  airflow/providers/apache/pinot/hooks/pinot/index

  airflow/providers/apache/spark/hooks/spark_jdbc/index

  airflow/providers/apache/spark/hooks/spark_jdbc_script/index

  airflow/providers/apache/spark/hooks/spark_sql/index

  airflow/providers/apache/spark/hooks/spark_submit/index

  airflow/providers/apache/sqoop/hooks/sqoop/index

  airflow/providers/cloudant/hooks/airflow_cloudant/index

  airflow/providers/databricks/hooks/databricks/index

  airflow/providers/datadog/hooks/datadog/index

  airflow/providers/dingding/hooks/dingding/index

  airflow/providers/discord/hooks/discord_webhook/index

  airflow/providers/docker/hooks/airflow_docker/index

  airflow/providers/ftp/hooks/ftp/index

  airflow/providers/google/cloud/hooks/automl/index

  airflow/providers/google/cloud/hooks/base/index

  airflow/providers/google/cloud/hooks/bigquery/index

  airflow/providers/google/cloud/hooks/bigquery_dts/index

  airflow/providers/google/cloud/hooks/bigtable/index

  airflow/providers/google/cloud/hooks/cloud_build/index

  airflow/providers/google/cloud/hooks/cloud_memorystore/index

  airflow/providers/google/cloud/hooks/cloud_sql/index

  airflow/providers/google/cloud/hooks/cloud_storage_transfer_service/index

  airflow/providers/google/cloud/hooks/compute/index

  airflow/providers/google/cloud/hooks/dataflow/index

  airflow/providers/google/cloud/hooks/dataproc/index

  airflow/providers/google/cloud/hooks/datastore/index

  airflow/providers/google/cloud/hooks/discovery_api/index

  airflow/providers/google/cloud/hooks/dlp/index

  airflow/providers/google/cloud/hooks/functions/index

  airflow/providers/google/cloud/hooks/gcs/index

  airflow/providers/google/cloud/hooks/kms/index

  airflow/providers/google/cloud/hooks/kubernetes_engine/index

  airflow/providers/google/cloud/hooks/mlengine/index

  airflow/providers/google/cloud/hooks/natural_language/index

  airflow/providers/google/cloud/hooks/pubsub/index

  airflow/providers/google/cloud/hooks/spanner/index

  airflow/providers/google/cloud/hooks/speech_to_text/index

  airflow/providers/google/cloud/hooks/tasks/index

  airflow/providers/google/cloud/hooks/text_to_speech/index

  airflow/providers/google/cloud/hooks/translate/index

  airflow/providers/google/cloud/hooks/video_intelligence/index

  airflow/providers/google/cloud/hooks/vision/index

  airflow/providers/google/marketing_platform/hooks/campaign_manager/index

  airflow/providers/google/marketing_platform/hooks/display_video/index

  airflow/providers/google/marketing_platform/hooks/search_ads/index

  airflow/providers/google/suite/hooks/drive/index

  airflow/providers/google/suite/hooks/sheets/index

  airflow/providers/grpc/hooks/airflow_grpc/index

  airflow/providers/http/hooks/http/index

  airflow/providers/imap/hooks/imap/index

  airflow/providers/jdbc/hooks/jdbc/index

  airflow/providers/jenkins/hooks/jenkins/index

  airflow/providers/jira/hooks/airflow_jira/index

  airflow/providers/microsoft/azure/hooks/azure_container_instance/index

  airflow/providers/microsoft/azure/hooks/azure_container_registry/index

  airflow/providers/microsoft/azure/hooks/azure_container_volume/index

  airflow/providers/microsoft/azure/hooks/azure_cosmos/index

  airflow/providers/microsoft/azure/hooks/azure_data_lake/index

  airflow/providers/microsoft/azure/hooks/azure_fileshare/index

  airflow/providers/microsoft/azure/hooks/wasb/index

  airflow/providers/microsoft/mssql/hooks/mssql/index

  airflow/providers/microsoft/winrm/hooks/airflow_winrm/index

  airflow/providers/mongo/hooks/mongo/index

  airflow/providers/mysql/hooks/mysql/index

  airflow/providers/odbc/hooks/odbc/index

  airflow/providers/openfass/hooks/openfaas/index

  airflow/providers/opsgenie/hooks/opsgenie_alert/index

  airflow/providers/oracle/hooks/oracle/index

  airflow/providers/pagerduty/hooks/pagerduty/index

  airflow/providers/postgres/hooks/postgres/index

  airflow/providers/presto/hooks/presto/index

  airflow/providers/qubole/hooks/qubole/index

  airflow/providers/qubole/hooks/qubole_check/index

  airflow/providers/redis/hooks/airflow_redis/index

  airflow/providers/salesforce/hooks/salesforce/index

  airflow/providers/samba/hooks/samba/index

  airflow/providers/segment/hooks/segment/index

  airflow/providers/sftp/hooks/sftp/index

  airflow/providers/slack/hooks/slack/index

  airflow/providers/slack/hooks/slack_webhook/index

  airflow/providers/snowflake/hooks/airflow_snowflake/index

  airflow/providers/sqlite/hooks/sqlite/index

  airflow/providers/ssh/hooks/ssh/index

  airflow/providers/vertica/hooks/vertica/index

  airflow/providers/zendesk/hooks/zendesk/index

Executors
---------
Executors are the mechanism by which task instances get run. All executors are
derived from :class:`~airflow.executors.base_executor.BaseExecutor`.

Executors packages
''''''''''''''''''
All executors are in the following packages:

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/executors/index

Models
------
Models are built on top of the SQLAlchemy ORM Base class, and instances are
persisted in the database.

.. toctree::
  :includehidden:
  :glob:
  :maxdepth: 1

  airflow/models/index
