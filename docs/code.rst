API Reference
=============

Operators
---------
Operators allow for generation of certain types of tasks that become nodes in
the DAG when instantiated. All operators derive from BaseOperator and
inherit many attributes and methods that way. Refer to the BaseOperator
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

.. autoclass:: airflow.operators.sensors.BaseSensorOperator


Operator API
''''''''''''

.. automodule:: airflow.operators
    :no-members:
.. deprecated:: 1.8
 Use :code:`from airflow.operators.bash_operator import BashOperator` instead.

.. autoclass:: airflow.operators.bash_operator.BashOperator
.. autoclass:: airflow.operators.python_operator.BranchPythonOperator
.. autoclass:: airflow.operators.dagrun_operator.TriggerDagRunOperator
.. autoclass:: airflow.operators.docker_operator.DockerOperator
.. autoclass:: airflow.operators.dummy_operator.DummyOperator
.. autoclass:: airflow.operators.email_operator.EmailOperator
.. autoclass:: airflow.operators.sensors.ExternalTaskSensor
.. autoclass:: airflow.operators.generic_transfer.GenericTransfer
.. autoclass:: airflow.operators.sensors.HdfsSensor
.. autoclass:: airflow.operators.hive_to_samba_operator.Hive2SambaOperator
.. autoclass:: airflow.operators.hive_operator.HiveOperator
.. autoclass:: airflow.operators.sensors.HivePartitionSensor
.. autoclass:: airflow.operators.hive_to_druid.HiveToDruidTransfer
.. autoclass:: airflow.operators.hive_to_mysql.HiveToMySqlTransfer
.. autoclass:: airflow.operators.http_operator.SimpleHttpOperator
.. autoclass:: airflow.operators.sensors.HttpSensor
.. autoclass:: airflow.operators.sensors.MetastorePartitionSensor
.. autoclass:: airflow.operators.mssql_operator.MsSqlOperator
.. autoclass:: airflow.operators.mssql_to_hive.MsSqlToHiveTransfer
.. autoclass:: airflow.operators.sensors.NamedHivePartitionSensor
.. autoclass:: airflow.operators.postgres_operator.PostgresOperator
.. autoclass:: airflow.operators.presto_check_operator.PrestoCheckOperator
.. autoclass:: airflow.operators.presto_check_operator.PrestoIntervalCheckOperator
.. autoclass:: airflow.operators.presto_check_operator.PrestoValueCheckOperator
.. autoclass:: airflow.operators.python_operator.PythonOperator
.. autoclass:: airflow.operators.python_operator.PythonVirtualenvOperator
.. autoclass:: airflow.operators.sensors.S3KeySensor
.. autoclass:: airflow.operators.s3_to_hive_operator.S3ToHiveTransfer
.. autoclass:: airflow.operators.ShortCircuitOperator
.. autoclass:: airflow.operators.slack_operator.SlackAPIOperator
.. autoclass:: airflow.operators.sensors.SqlSensor
.. autoclass:: airflow.operators.subdag_operator.SubDagOperator
.. autoclass:: airflow.operators.sensors.TimeSensor
.. autoclass:: airflow.operators.sensors.HdfsSensor

Community-contributed Operators
'''''''''''''''''''''''''''''''

.. automodule:: airflow.contrib.operators
    :no-members:
.. deprecated:: 1.8
 Use :code:`from airflow.operators.bash_operator import BashOperator` instead.

.. autoclass:: airflow.contrib.sensors.aws_redshift_cluster_sensor.AwsRedshiftClusterSensor
.. autoclass:: airflow.contrib.operators.bigquery_get_data.BigQueryGetDataOperator
.. autoclass:: airflow.contrib.operators.bigquery_operator.BigQueryOperator
.. autoclass:: airflow.contrib.operators.bigquery_to_gcs.BigQueryToCloudStorageOperator
.. autoclass:: airflow.contrib.operators.databricks_operator.DatabricksSubmitRunOperator
.. autoclass:: airflow.contrib.operators.ecs_operator.ECSOperator
.. autoclass:: airflow.contrib.operators.file_to_wasb.FileToWasbOperator
.. autoclass:: airflow.contrib.operators.gcs_copy_operator.GoogleCloudStorageCopyOperator
.. autoclass:: airflow.contrib.operators.gcs_download_operator.GoogleCloudStorageDownloadOperator
.. autoclass:: airflow.contrib.operators.gcs_to_gcs.GoogleCloudStorageToGoogleCloudStorageOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubTopicCreateOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubTopicDeleteOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubSubscriptionCreateOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubSubscriptionDeleteOperator
.. autoclass:: airflow.contrib.operators.pubsub_operator.PubSubPublishOperator
.. autoclass:: airflow.contrib.sensors.pubsub_sensor.PubSubPullSensor
.. autoclass:: airflow.contrib.operators.hipchat_operator.HipChatAPIOperator
.. autoclass:: airflow.contrib.operators.hipchat_operator.HipChatAPISendRoomNotificationOperator
.. autoclass:: airflow.contrib.operators.qubole_operator.QuboleOperator
.. autoclass:: airflow.contrib.operators.ssh_operator.SSHOperator
.. autoclass:: airflow.contrib.operators.vertica_operator.VerticaOperator
.. autoclass:: airflow.contrib.operators.vertica_to_hive.VerticaToHiveTransfer
.. autoclass:: airflow.contrib.sensors.bash_sensor.BashSensor

.. _macros:

Macros
---------
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
``{{ yesterday_ds }}``              yesterday's date as ``YYYY-MM-DD``
``{{ yesterday_ds_nodash }}``       yesterday's date as ``YYYYMMDD``
``{{ tomorrow_ds }}``               tomorrow's date as ``YYYY-MM-DD``
``{{ tomorrow_ds_nodash }}``        tomorrow's date as ``YYYYMMDD``
``{{ ts }}``                        same as ``execution_date.isoformat()``
``{{ ts_nodash }}``                 same as ``ts`` without ``-`` and ``:``
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
``{{ params }}``                    a reference to the user-defined params dictionary
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
.. automodule:: airflow.hooks
    :no-members:
.. deprecated:: 1.8
 Use :code:`from airflow.operators.bash_operator import BashOperator` instead.

.. autoclass:: airflow.hooks.dbapi_hook.DbApiHook
.. autoclass:: airflow.hooks.docker_hook.DockerHook
.. automodule:: airflow.hooks.hive_hooks
    :members:
      HiveCliHook,
      HiveMetastoreHook,
      HiveServer2Hook
.. autoclass:: airflow.hooks.http_hook.HttpHook
.. autoclass:: airflow.hooks.druid_hook.DruidHook
.. autoclass:: airflow.hooks.mssql_hook.MsSqlHook
.. autoclass:: airflow.hooks.mysql_hook.MySqlHook
.. autoclass:: airflow.hooks.postgres_hook.PostgresHook
.. autoclass:: airflow.hooks.presto_hook.PrestoHook
.. autoclass:: airflow.hooks.S3_hook.S3Hook
.. autoclass:: airflow.hooks.sqlite_hook.SqliteHook
.. autoclass:: airflow.hooks.webhdfs_hook.WebHDFSHook

Community contributed hooks
'''''''''''''''''''''''''''

.. automodule:: airflow.contrib.hooks
    :no-members:
.. deprecated:: 1.8
 Use :code:`from airflow.operators.bash_operator import BashOperator` instead.

.. autoclass:: airflow.contrib.hooks.redshift_hook.RedshiftHook
.. autoclass:: airflow.contrib.hooks.bigquery_hook.BigQueryHook
.. autoclass:: airflow.contrib.hooks.vertica_hook.VerticaHook
.. autoclass:: airflow.contrib.hooks.ftp_hook.FTPHook
.. autoclass:: airflow.contrib.hooks.ssh_hook.SSHHook
.. autoclass:: airflow.contrib.hooks.cloudant_hook.CloudantHook
.. autoclass:: airflow.contrib.hooks.gcs_hook.GoogleCloudStorageHook
.. autoclass:: airflow.contrib.hooks.gcp_pubsub_hook.PubSubHook

Executors
---------
Executors are the mechanism by which task instances get run.

.. autoclass:: airflow.executors.local_executor.LocalExecutor
.. autoclass:: airflow.executors.celery_executor.CeleryExecutor
.. autoclass:: airflow.executors.sequential_executor.SequentialExecutor

Community-contributed executors
'''''''''''''''''''''''''''''''

.. autoclass:: airflow.contrib.executors.mesos_executor.MesosExecutor
