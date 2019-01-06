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

API Reference
=============

.. _api-reference-operators:

Operators
---------
Operators allow for generation of certain types of tasks that become nodes in
the DAG when instantiated. All operators derive from ``BaseOperator`` and
inherit many attributes and methods that way. Refer to the BaseOperator_
documentation for more details.

There are 3 main types of operators:

- Operators that performs an **action**, or tell another system to
  perform an action
- **Transfer** operators move data from one system to another
- **Sensors** are a certain type of operator that will keep running until a
  certain criterion is met. Examples include a specific file landing in HDFS or
  S3, a partition appearing in Hive, or a specific time of the day. Sensors
  are derived from ``BaseSensorOperator`` and run a poke
  method at a specified ``poke_interval`` until it returns ``True``.

BaseOperator
''''''''''''
All operators are derived from ``BaseOperator`` and acquire much
functionality through inheritance. Since this is the core of the engine,
it's worth taking the time to understand the parameters of ``BaseOperator``
to understand the primitive features that can be leveraged in your
DAGs.


.. autoclass:: airflow.models.BaseOperator


BaseSensorOperator
'''''''''''''''''''
All sensors are derived from ``BaseSensorOperator``. All sensors inherit
the ``timeout`` and ``poke_interval`` on top of the ``BaseOperator``
attributes.

.. autoclass:: airflow.sensors.base_sensor_operator.BaseSensorOperator


Core Operators
''''''''''''''

Operators
^^^^^^^^^

.. autoclass:: airflow.operators.bash_operator.BashOperator
.. autoclass:: airflow.operators.python_operator.BranchPythonOperator
.. autoclass:: airflow.operators.check_operator.CheckOperator
.. autoclass:: airflow.operators.docker_operator.DockerOperator
.. autoclass:: airflow.operators.dummy_operator.DummyOperator
.. autoclass:: airflow.operators.druid_check_operator.DruidCheckOperator
.. autoclass:: airflow.operators.email_operator.EmailOperator
.. autoclass:: airflow.operators.generic_transfer.GenericTransfer
.. autoclass:: airflow.operators.hive_to_druid.HiveToDruidTransfer
.. autoclass:: airflow.operators.hive_to_mysql.HiveToMySqlTransfer
.. autoclass:: airflow.operators.hive_to_samba_operator.Hive2SambaOperator
.. autoclass:: airflow.operators.hive_operator.HiveOperator
.. autoclass:: airflow.operators.hive_stats_operator.HiveStatsCollectionOperator
.. autoclass:: airflow.operators.check_operator.IntervalCheckOperator
.. autoclass:: airflow.operators.jdbc_operator.JdbcOperator
.. autoclass:: airflow.operators.latest_only_operator.LatestOnlyOperator
.. autoclass:: airflow.operators.mssql_operator.MsSqlOperator
.. autoclass:: airflow.operators.mssql_to_hive.MsSqlToHiveTransfer
.. autoclass:: airflow.operators.mysql_operator.MySqlOperator
.. autoclass:: airflow.operators.mysql_to_hive.MySqlToHiveTransfer
.. autoclass:: airflow.operators.oracle_operator.OracleOperator
.. autoclass:: airflow.operators.pig_operator.PigOperator
.. autoclass:: airflow.operators.postgres_operator.PostgresOperator
.. autoclass:: airflow.operators.presto_check_operator.PrestoCheckOperator
.. autoclass:: airflow.operators.presto_check_operator.PrestoIntervalCheckOperator
.. autoclass:: airflow.operators.presto_to_mysql.PrestoToMySqlTransfer
.. autoclass:: airflow.operators.presto_check_operator.PrestoValueCheckOperator
.. autoclass:: airflow.operators.python_operator.PythonOperator
.. autoclass:: airflow.operators.python_operator.PythonVirtualenvOperator
.. autoclass:: airflow.operators.s3_file_transform_operator.S3FileTransformOperator
.. autoclass:: airflow.operators.s3_to_hive_operator.S3ToHiveTransfer
.. autoclass:: airflow.operators.s3_to_redshift_operator.S3ToRedshiftTransfer
.. autoclass:: airflow.operators.python_operator.ShortCircuitOperator
.. autoclass:: airflow.operators.http_operator.SimpleHttpOperator
.. autoclass:: airflow.operators.slack_operator.SlackAPIOperator
.. autoclass:: airflow.operators.slack_operator.SlackAPIPostOperator
.. autoclass:: airflow.operators.sqlite_operator.SqliteOperator
.. autoclass:: airflow.operators.subdag_operator.SubDagOperator
.. autoclass:: airflow.operators.dagrun_operator.TriggerDagRunOperator
.. autoclass:: airflow.operators.check_operator.ValueCheckOperator
.. autoclass:: airflow.operators.redshift_to_s3_operator.RedshiftToS3Transfer

Sensors
^^^^^^^
.. autoclass:: airflow.sensors.external_task_sensor.ExternalTaskSensor
.. autoclass:: airflow.sensors.hdfs_sensor.HdfsSensor
.. autoclass:: airflow.sensors.hive_partition_sensor.HivePartitionSensor
.. autoclass:: airflow.sensors.http_sensor.HttpSensor
.. autoclass:: airflow.sensors.metastore_partition_sensor.MetastorePartitionSensor
.. autoclass:: airflow.sensors.named_hive_partition_sensor.NamedHivePartitionSensor
.. autoclass:: airflow.sensors.s3_key_sensor.S3KeySensor
.. autoclass:: airflow.sensors.s3_prefix_sensor.S3PrefixSensor
.. autoclass:: airflow.sensors.sql_sensor.SqlSensor
.. autoclass:: airflow.sensors.time_sensor.TimeSensor
.. autoclass:: airflow.sensors.time_delta_sensor.TimeDeltaSensor
.. autoclass:: airflow.sensors.web_hdfs_sensor.WebHdfsSensor

Community-contributed Operators
'''''''''''''''''''''''''''''''

Operators
^^^^^^^^^
.. Alphabetize this list

.. autoclass:: airflow.contrib.operators.adls_list_operator.AzureDataLakeStorageListOperator
.. autoclass:: airflow.contrib.operators.adls_to_gcs.AdlsToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.aws_athena_operator.AWSAthenaOperator
.. autoclass:: airflow.contrib.operators.awsbatch_operator.AWSBatchOperator
.. autoclass:: airflow.contrib.operators.azure_cosmos_operator.AzureCosmosInsertDocumentOperator
.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryCheckOperator
.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryValueCheckOperator
.. autoclass:: airflow.contrib.operators.bigquery_check_operator.BigQueryIntervalCheckOperator
.. autoclass:: airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyTableOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateExternalTableOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryDeleteDatasetOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryCreateEmptyDatasetOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryOperator
.. autoclass:: airflow.contrib.operators.bigquery_table_delete_operator.BigQueryTableDeleteOperator
.. autoclass:: airflow.contrib.operators.bigquery_to_bigquery.BigQueryToBigQueryOperator
.. autoclass:: airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator
.. autoclass:: airflow.contrib.operators.cassandra_to_gcs.CassandraToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator
.. autoclass:: airflow.contrib.operators.dataflow_operator.DataFlowJavaOperator
.. autoclass:: airflow.contrib.operators.dataflow_operator.DataflowTemplateOperator
.. autoclass:: airflow.contrib.operators.dataflow_operator.DataFlowPythonOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterCreateOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterScaleOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocClusterDeleteOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPigOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHiveOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkSqlOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcSparkOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcHadoopOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataProcPySparkOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateBaseOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateOperator
.. autoclass:: airflow.contrib.operators.dataproc_operator.DataprocWorkflowTemplateInstantiateInlineOperator
.. autoclass:: airflow.contrib.operators.datastore_export_operator.DatastoreExportOperator
.. autoclass:: airflow.contrib.operators.datastore_import_operator.DatastoreImportOperator
.. autoclass:: airflow.contrib.operators.discord_webhook_operator.DiscordWebhookOperator
.. autoclass:: airflow.contrib.operators.druid_operator.DruidOperator
.. autoclass:: airflow.contrib.operators.ecs_operator.ECSOperator
.. autoclass:: airflow.contrib.operators.emr_add_steps_operator.EmrAddStepsOperator
.. autoclass:: airflow.contrib.operators.emr_create_job_flow_operator.EmrCreateJobFlowOperator
.. autoclass:: airflow.contrib.operators.emr_terminate_job_flow_operator.EmrTerminateJobFlowOperator
.. autoclass:: airflow.contrib.operators.file_to_gcs.FileToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.file_to_wasb.FileToWasbOperator
.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEClusterCreateOperator
.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEClusterDeleteOperator
.. autoclass:: airflow.contrib.operators.gcp_container_operator.GKEPodOperator
.. autoclass:: airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator
.. autoclass:: airflow.contrib.operators.gcs_list_operator.GoogleCloudStorageListOperator
.. autoclass:: airflow.contrib.operators.gcs_operator.GoogleCloudStorageCreateBucketOperator
.. autoclass:: airflow.contrib.operators.gcs_to_bq.GoogleCloudStorageToBigQueryOperator
.. autoclass:: airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageTransferOperator
.. autoclass:: airflow.contrib.operators.gcs_to_s3.GoogleCloudStorageToS3Operator
.. autoclass:: airflow.contrib.operators.hipchat_operator.HipChatAPIOperator
.. autoclass:: airflow.contrib.operators.hipchat_operator.HipChatAPISendRoomNotificationOperator
.. autoclass:: airflow.contrib.operators.hive_to_dynamodb.HiveToDynamoDBTransferOperator
.. autoclass:: airflow.contrib.operators.jenkins_job_trigger_operator.JenkinsJobTriggerOperator
.. autoclass:: airflow.contrib.operators.jira_operator.JiraOperator
.. autoclass:: airflow.contrib.operators.kubernetes_pod_operator.KubernetesPodOperator
.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineBatchPredictionOperator
.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineModelOperator
.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineVersionOperator
.. autoclass:: airflow.contrib.operators.mlengine_operator.MLEngineTrainingOperator
.. autoclass:: airflow.contrib.operators.mongo_to_s3.MongoToS3Operator
.. autoclass:: airflow.contrib.operators.mysql_to_gcs.MySqlToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.oracle_to_azure_data_lake_transfer.OracleToAzureDataLakeTransfer
.. autoclass:: airflow.contrib.operators.oracle_to_oracle_transfer.OracleToOracleTransfer
.. autoclass:: airflow.contrib.operators.postgres_to_gcs_operator.PostgresToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubPublishOperator
.. autoclass:: airflow.contrib.operators.qubole_check_operator.QuboleCheckOperator
.. autoclass:: airflow.contrib.operators.qubole_check_operator.QuboleValueCheckOperator
.. autoclass:: airflow.contrib.operators.qubole_operator.QuboleOperator
.. autoclass:: airflow.contrib.operators.s3_copy_object_operator.S3CopyObjectOperator
.. autoclass:: airflow.contrib.operators.s3_delete_objects_operator.S3DeleteObjectsOperator
.. autoclass:: airflow.contrib.operators.s3_list_operator.S3ListOperator
.. autoclass:: airflow.contrib.operators.s3_to_gcs_operator.S3ToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.s3_to_gcs_transfer_operator.S3ToGoogleCloudStorageTransferOperator
.. autoclass:: airflow.contrib.operators.sagemaker_base_operator.SageMakerBaseOperator
.. autoclass:: airflow.contrib.operators.sagemaker_endpoint_operator.SageMakerEndpointOperator
.. autoclass:: airflow.contrib.operators.sagemaker_endpoint_config_operator.SageMakerEndpointConfigOperator
.. autoclass:: airflow.contrib.operators.sagemaker_model_operator.SageMakerModelOperator
.. autoclass:: airflow.contrib.operators.sagemaker_training_operator.SageMakerTrainingOperator
.. autoclass:: airflow.contrib.operators.sagemaker_transform_operator.SageMakerTransformOperator
.. autoclass:: airflow.contrib.operators.sagemaker_tuning_operator.SageMakerTuningOperator
.. autoclass:: airflow.contrib.operators.segment_track_event_operator.SegmentTrackEventOperator
.. autoclass:: airflow.contrib.operators.sftp_operator.SFTPOperator
.. autoclass:: airflow.contrib.operators.slack_webhook_operator.SlackWebhookOperator
.. autoclass:: airflow.contrib.operators.snowflake_operator.SnowflakeOperator
.. autoclass:: airflow.contrib.operators.sns_publish_operator.SnsPublishOperator
.. autoclass:: airflow.contrib.operators.spark_jdbc_operator.SparkJDBCOperator
.. autoclass:: airflow.contrib.operators.spark_sql_operator.SparkSqlOperator
.. autoclass:: airflow.contrib.operators.spark_submit_operator.SparkSubmitOperator
.. autoclass:: airflow.contrib.operators.sqoop_operator.SqoopOperator
.. autoclass:: airflow.contrib.operators.ssh_operator.SSHOperator
.. autoclass:: airflow.contrib.operators.vertica_operator.VerticaOperator
.. autoclass:: airflow.contrib.operators.vertica_to_hive.VerticaToHiveTransfer
.. autoclass:: airflow.contrib.operators.winrm_operator.WinRMOperator

Sensors
^^^^^^^

.. autoclass:: airflow.contrib.sensors.aws_athena_sensor.AthenaSensor
.. autoclass:: airflow.contrib.sensors.aws_redshift_cluster_sensor.AwsRedshiftClusterSensor
.. autoclass:: airflow.contrib.sensors.azure_cosmos_sensor.AzureCosmosDocumentSensor
.. autoclass:: airflow.contrib.sensors.bash_sensor.BashSensor
.. autoclass:: airflow.contrib.sensors.bigquery_sensor.BigQueryTableSensor
.. autoclass:: airflow.contrib.sensors.cassandra_record_sensor.CassandraRecordSensor
.. autoclass:: airflow.contrib.sensors.cassandra_table_sensor.CassandraTableSensor
.. autoclass:: airflow.contrib.sensors.datadog_sensor.DatadogSensor
.. autoclass:: airflow.contrib.sensors.emr_base_sensor.EmrBaseSensor
.. autoclass:: airflow.contrib.sensors.emr_job_flow_sensor.EmrJobFlowSensor
.. autoclass:: airflow.contrib.sensors.emr_step_sensor.EmrStepSensor
.. autoclass:: airflow.contrib.sensors.file_sensor.FileSensor
.. autoclass:: airflow.contrib.sensors.ftp_sensor.FTPSensor
.. autoclass:: airflow.contrib.sensors.ftp_sensor.FTPSSensor
.. autoclass:: airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectSensor
.. autoclass:: airflow.contrib.sensors.gcs_sensor.GoogleCloudStorageObjectUpdatedSensor
.. autoclass:: airflow.contrib.sensors.gcs_sensor.GoogleCloudStoragePrefixSensor
.. autoclass:: airflow.contrib.sensors.hdfs_sensor.HdfsSensorFolder
.. autoclass:: airflow.contrib.sensors.hdfs_sensor.HdfsSensorRegex
.. autoclass:: airflow.contrib.sensors.imap_attachment_sensor.ImapAttachmentSensor
.. autoclass:: airflow.contrib.sensors.jira_sensor.JiraSensor
.. autoclass:: airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor
.. autoclass:: airflow.contrib.sensors.python_sensor.PythonSensor
.. autoclass:: airflow.contrib.sensors.qubole_sensor.QuboleSensor
.. autoclass:: airflow.contrib.sensors.redis_key_sensor.RedisKeySensor
.. autoclass:: airflow.contrib.sensors.sagemaker_base_sensor.SageMakerBaseSensor
.. autoclass:: airflow.contrib.sensors.sagemaker_endpoint_sensor.SageMakerEndpointSensor
.. autoclass:: airflow.contrib.sensors.sagemaker_training_sensor.SageMakerTrainingSensor
.. autoclass:: airflow.contrib.sensors.sagemaker_transform_sensor.SageMakerTransformSensor
.. autoclass:: airflow.contrib.sensors.sagemaker_tuning_sensor.SageMakerTuningSensor
.. autoclass:: airflow.contrib.sensors.sftp_sensor.SFTPSensor
.. autoclass:: airflow.contrib.sensors.wasb_sensor.WasbBlobSensor
.. autoclass:: airflow.contrib.sensors.weekday_sensor.DayOfWeekSensor

.. _macros:

Macros
------
Here's a list of variables and macros that can be used in templates


Default Variables
'''''''''''''''''
The Airflow engine passes a few variables by default that are accessible
in all templates

=================================   ====================================
Variable                            Description
=================================   ====================================
``{{ ds }}``                        the execution date as ``YYYY-MM-DD``
``{{ ds_nodash }}``                 the execution date as ``YYYYMMDD``
``{{ prev_ds }}``                   the previous execution date as ``YYYY-MM-DD``
                                    if ``{{ ds }}`` is ``2018-01-08`` and ``schedule_interval`` is ``@weekly``,
                                    ``{{ prev_ds }}`` will be ``2016-01-01``
``{{ prev_ds_nodash }}``            the previous execution date as ``YYYYMMDD`` if exists, else ``None`
``{{ next_ds }}``                   the next execution date as ``YYYY-MM-DD``
                                    if ``{{ ds }}`` is ``2018-01-01`` and ``schedule_interval`` is ``@weekly``,
                                    ``{{ next_ds }}`` will be ``2018-01-08``
``{{ next_ds_nodash }}``            the next execution date as ``YYYYMMDD`` if exists, else ``None`
``{{ yesterday_ds }}``              the day before the execution date as ``YYYY-MM-DD``
``{{ yesterday_ds_nodash }}``       the day before the execution date as ``YYYYMMDD``
``{{ tomorrow_ds }}``               the day after the execution date as ``YYYY-MM-DD``
``{{ tomorrow_ds_nodash }}``        the day after the execution date as ``YYYYMMDD``
``{{ ts }}``                        same as ``execution_date.isoformat()``. Example: ``2018-01-01T00:00:00+00:00``
``{{ ts_nodash }}``                 same as ``ts`` without ``-``, ``:`` and TimeZone info. Example: ``20180101T000000``
``{{ ts_nodash_with_tz }}``         same as ``ts`` without ``-`` and ``:``. Example: ``20180101T000000+0000``
``{{ execution_date }}``            the execution_date, (datetime.datetime)
``{{ prev_execution_date }}``       the previous execution date (if available) (datetime.datetime)
``{{ next_execution_date }}``       the next execution date (datetime.datetime)
``{{ dag }}``                       the DAG object
``{{ task }}``                      the Task object
``{{ macros }}``                    a reference to the macros package, described below
``{{ task_instance }}``             the task_instance object
``{{ end_date }}``                  same as ``{{ ds }}``
``{{ latest_date }}``               same as ``{{ ds }}``
``{{ ti }}``                        same as ``{{ task_instance }}``
``{{ params }}``                    a reference to the user-defined params dictionary which can be overridden by
                                    the dictionary passed through ``trigger_dag -c`` if you enabled
                                    ``dag_run_conf_overrides_params` in ``airflow.cfg``
``{{ var.value.my_var }}``          global defined variables represented as a dictionary
``{{ var.json.my_var.path }}``      global defined variables represented as a dictionary
                                    with deserialized JSON object, append the path to the
                                    key within the JSON object
``{{ task_instance_key_str }}``     a unique, human-readable key to the task instance
                                    formatted ``{dag_id}_{task_id}_{ds}``
``{{ conf }}``                      the full configuration object located at
                                    ``airflow.configuration.conf`` which
                                    represents the content of your
                                    ``airflow.cfg``
``{{ run_id }}``                    the ``run_id`` of the current DAG run
``{{ dag_run }}``                   a reference to the DagRun object
``{{ test_mode }}``                 whether the task instance was called using
                                    the CLI's test subcommand
=================================   ====================================

Note that you can access the object's attributes and methods with simple
dot notation. Here are some examples of what is possible:
``{{ task.owner }}``, ``{{ task.task_id }}``, ``{{ ti.hostname }}``, ...
Refer to the models documentation for more information on the objects'
attributes and methods.

The ``var`` template variable allows you to access variables defined in Airflow's
UI. You can access them as either plain-text or JSON. If you use JSON, you are
also able to walk nested structures, such as dictionaries like:
``{{ var.json.my_dict_var.key1 }}``

Macros
''''''
Macros are a way to expose objects to your templates and live under the
``macros`` namespace in your templates.

A few commonly used libraries and methods are made available.


=================================   ====================================
Variable                            Description
=================================   ====================================
``macros.datetime``                 The standard lib's ``datetime.datetime``
``macros.timedelta``                 The standard lib's ``datetime.timedelta``
``macros.dateutil``                 A reference to the ``dateutil`` package
``macros.time``                     The standard lib's ``time``
``macros.uuid``                     The standard lib's ``uuid``
``macros.random``                   The standard lib's ``random``
=================================   ====================================


Some airflow specific macros are also defined:

.. automodule:: airflow.macros
    :show-inheritance:
    :members:

.. autofunction:: airflow.macros.hive.closest_ds_partition
.. autofunction:: airflow.macros.hive.max_partition

.. _models_ref:

Models
------

Models are built on top of the SQLAlchemy ORM Base class, and instances are
persisted in the database.


.. automodule:: airflow.models
    :show-inheritance:
    :members: DAG, BaseOperator, TaskInstance, DagBag, Connection

Hooks
-----

Hooks are interfaces to external platforms and databases, implementing a common
interface when possible and acting as building blocks for operators.

.. autoclass:: airflow.hooks.dbapi_hook.DbApiHook
.. autoclass:: airflow.hooks.docker_hook.DockerHook
.. automodule:: airflow.hooks.hive_hooks
    :members:
      HiveCliHook,
      HiveMetastoreHook,
      HiveServer2Hook
.. autoclass:: airflow.hooks.http_hook.HttpHook
.. autoclass:: airflow.hooks.druid_hook.DruidDbApiHook
.. autoclass:: airflow.hooks.druid_hook.DruidHook
.. autoclass:: airflow.hooks.hdfs_hook.HDFSHook
.. autoclass:: airflow.hooks.jdbc_hook.JdbcHook
.. autoclass:: airflow.hooks.mssql_hook.MsSqlHook
.. autoclass:: airflow.hooks.mysql_hook.MySqlHook
.. autoclass:: airflow.hooks.oracle_hook.OracleHook
.. autoclass:: airflow.hooks.pig_hook.PigCliHook
.. autoclass:: airflow.hooks.postgres_hook.PostgresHook
.. autoclass:: airflow.hooks.presto_hook.PrestoHook
.. autoclass:: airflow.hooks.S3_hook.S3Hook
.. autoclass:: airflow.hooks.samba_hook.SambaHook
.. autoclass:: airflow.hooks.slack_hook.SlackHook
.. autoclass:: airflow.hooks.sqlite_hook.SqliteHook
.. autoclass:: airflow.hooks.webhdfs_hook.WebHDFSHook
.. autoclass:: airflow.hooks.zendesk_hook.ZendeskHook

Community contributed hooks
'''''''''''''''''''''''''''
.. Alphabetize this list
.. autoclass:: airflow.contrib.hooks.aws_athena_hook.AWSAthenaHook
.. autoclass:: airflow.contrib.hooks.aws_dynamodb_hook.AwsDynamoDBHook
.. autoclass:: airflow.contrib.hooks.aws_firehose_hook.AwsFirehoseHook
.. autoclass:: airflow.contrib.hooks.aws_hook.AwsHook
.. autoclass:: airflow.contrib.hooks.aws_lambda_hook.AwsLambdaHook
.. autoclass:: airflow.contrib.hooks.aws_sns_hook.AwsSnsHook
.. autoclass:: airflow.contrib.hooks.azure_cosmos_hook.AzureCosmosDBHook
.. autoclass:: airflow.contrib.hooks.azure_data_lake_hook.AzureDataLakeHook
.. autoclass:: airflow.contrib.hooks.azure_fileshare_hook.AzureFileShareHook
.. autoclass:: airflow.contrib.hooks.bigquery_hook.BigQueryHook
.. autoclass:: airflow.contrib.hooks.cassandra_hook.CassandraHook
.. autoclass:: airflow.contrib.hooks.cloudant_hook.CloudantHook
.. autoclass:: airflow.contrib.hooks.databricks_hook.DatabricksHook
.. autoclass:: airflow.contrib.hooks.datadog_hook.DatadogHook
.. autoclass:: airflow.contrib.hooks.datastore_hook.DatastoreHook
.. autoclass:: airflow.contrib.hooks.discord_webhook_hook.DiscordWebhookHook
.. autoclass:: airflow.contrib.hooks.emr_hook.EmrHook
.. autoclass:: airflow.contrib.hooks.fs_hook.FSHook
.. autoclass:: airflow.contrib.hooks.ftp_hook.FTPHook
.. autoclass:: airflow.contrib.hooks.ftp_hook.FTPSHook
.. autoclass:: airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook
.. autoclass:: airflow.contrib.hooks.gcp_container_hook.GKEClusterHook
.. autoclass:: airflow.contrib.hooks.gcp_dataflow_hook.DataFlowHook
.. autoclass:: airflow.contrib.hooks.gcp_dataproc_hook.DataProcHook
.. autoclass:: airflow.contrib.hooks.gcp_mlengine_hook.MLEngineHook
.. autoclass:: airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook
.. autoclass:: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook
.. autoclass:: airflow.contrib.hooks.gcp_transfer_hook.GCPTransferServiceHook
.. autoclass:: airflow.contrib.hooks.imap_hook.ImapHook
.. autoclass:: airflow.contrib.hooks.jenkins_hook.JenkinsHook
.. autoclass:: airflow.contrib.hooks.jira_hook.JiraHook
.. autoclass:: airflow.contrib.hooks.mongo_hook.MongoHook
.. autoclass:: airflow.contrib.hooks.openfaas_hook.OpenFaasHook
.. autoclass:: airflow.contrib.hooks.pinot_hook.PinotDbApiHook
.. autoclass:: airflow.contrib.hooks.qubole_hook.QuboleHook
.. autoclass:: airflow.contrib.hooks.redis_hook.RedisHook
.. autoclass:: airflow.contrib.hooks.redshift_hook.RedshiftHook
.. autoclass:: airflow.contrib.hooks.sagemaker_hook.SageMakerHook
.. autoclass:: airflow.contrib.hooks.salesforce_hook.SalesforceHook
.. autoclass:: airflow.contrib.hooks.segment_hook.SegmentHook
.. autoclass:: airflow.contrib.hooks.sftp_hook.SFTPHook
.. autoclass:: airflow.contrib.hooks.slack_webhook_hook.SlackWebhookHook
.. autoclass:: airflow.contrib.hooks.snowflake_hook.SnowflakeHook
.. autoclass:: airflow.contrib.hooks.spark_jdbc_hook.SparkJDBCHook
.. autoclass:: airflow.contrib.hooks.spark_sql_hook.SparkSqlHook
.. autoclass:: airflow.contrib.hooks.spark_submit_hook.SparkSubmitHook
.. autoclass:: airflow.contrib.hooks.sqoop_hook.SqoopHook
.. autoclass:: airflow.contrib.hooks.ssh_hook.SSHHook
.. autoclass:: airflow.contrib.hooks.vertica_hook.VerticaHook
.. autoclass:: airflow.contrib.hooks.wasb_hook.WasbHook
.. autoclass:: airflow.contrib.hooks.winrm_hook.WinRMHook

Executors
---------
Executors are the mechanism by which task instances get run.

.. autoclass:: airflow.executors.local_executor.LocalExecutor
.. autoclass:: airflow.executors.celery_executor.CeleryExecutor
.. autoclass:: airflow.executors.sequential_executor.SequentialExecutor

Community-contributed executors
'''''''''''''''''''''''''''''''

.. autoclass:: airflow.contrib.executors.mesos_executor.MesosExecutor
.. autoclass:: airflow.contrib.executors.kubernetes_executor.KubernetesExecutor
