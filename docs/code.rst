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
    :show-inheritance:
    :members:
        BashOperator,
        BranchPythonOperator,
        TriggerDagRunOperator,
        DummyOperator,
        EmailOperator,
        ExternalTaskSensor,
        GenericTransfer,
        HdfsSensor,
        Hive2SambaOperator,
        HiveOperator,
        HivePartitionSensor,
        HiveToDruidTransfer,
        HiveToMySqlTransfer,
        SimpleHttpOperator,
        HttpSensor,
        MetastorePartitionSensor,
        MsSqlOperator,
        MsSqlToHiveTransfer,
        MySqlOperator,
        MySqlToHiveTransfer,
        PostgresOperator,
        PrestoCheckOperator,
        PrestoIntervalCheckOperator,
        PrestoValueCheckOperator,
        PythonOperator,
        S3KeySensor,
        S3ToHiveTransfer,
        ShortCircuitOperator,
        SlackAPIOperator,
        SlackAPIPostOperator,
        SqlSensor,
        SubDagOperator,
        TimeSensor,
        WebHdfsSensor


Community-contributed Operators
'''''''''''''''''''''''''''''''

.. automodule:: airflow.contrib.operators
    :show-inheritance:
    :members:
        BigQueryOperator,
        BigQueryToCloudStorageOperator,
        GoogleCloudStorageDownloadOperator,
        VerticaOperator,
        VerticaToHiveTransfer

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
``{{ yesterday_ds }}``              yesterday's date as  ``YYYY-MM-DD``
``{{ tomorrow_ds }}``               tomorrow's date as  ``YYYY-MM-DD``
``{{ ts }}``                        same as ``execution_date.isoformat()``
``{{ ts_nodash }}``                 same as ``ts`` without ``-`` and ``:``
``{{ execution_date }}``            the execution_date, (datetime.datetime)
``{{ dag }}``                       the DAG object
``{{ task }}``                      the Task object
``{{ macros }}``                    a reference to the macros package, described below
``{{ task_instance }}``             the task_instance object
``{{ ds_nodash }}``                 the execution date as ``YYYYMMDD``
``{{ end_date }}``                  same as ``{{ ds }}``
``{{ latest_date }}``               same as ``{{ ds }}``
``{{ ti }}``                        same as ``{{ task_instance }}``
``{{ params }}``                    a reference to the user-defined params dictionary
``{{ task_instance_key_str }}``     a unique, human-readable key to the task instance
                                    formatted ``{dag_id}_{task_id}_{ds}``
``conf``                            the full configuration object located at
                                    ``airflow.configuration.conf`` which
                                    represents the content of your
                                    ``airflow.cfg``
``run_id``                          the ``run_id`` of the current DAG run
``dag_run``                         a reference to the DAG run object
``test_mode``                       whether the task instance was called using
                                    the CLI's test subcommand
=================================   ====================================

Note that you can access the object's attributes and methods with simple
dot notation. Here are some examples of what is possible:
``{{ task.owner }}``, ``{{ task.task_id }}``, ``{{ ti.hostname }}``, ...
Refer to the models documentation for more information on the objects'
attributes and methods.

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

.. automodule:: airflow.macros.hive
    :show-inheritance:
    :members:

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
    :show-inheritance:
    :members:
        DbApiHook,
        HiveCliHook,
        HiveMetastoreHook,
        HiveServer2Hook,
        HttpHook,
        DruidHook,
        MsSqlHook,
        MySqlHook,
        PostgresHook,
        PrestoHook,
        S3Hook,
        SqliteHook,
        WebHDFSHook

Community contributed hooks
'''''''''''''''''''''''''''

.. automodule:: airflow.contrib.hooks
    :show-inheritance:
    :members:
        BigQueryHook,
        GoogleCloudStorageHook,
        VerticaHook,
        FTPHook,
        SSHHook

Executors
---------
Executors are the mechanism by which task instances get run.

.. automodule:: airflow.executors
    :show-inheritance:
    :members: LocalExecutor, CeleryExecutor, SequentialExecutor

Community-contributed executors
'''''''''''''''''''''''''''''''

.. automodule:: airflow.contrib.executors
    :show-inheritance:
    :members:
        MesosExecutor
