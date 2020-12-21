:mod:`airflow.settings`
=======================

.. py:module:: airflow.settings


Module Contents
---------------

.. data:: log
   

   

.. data:: TIMEZONE
   

   

.. data:: tz
   

   

.. data:: HEADER
   

   

.. data:: LOGGING_LEVEL
   

   

.. data:: GUNICORN_WORKER_READY_PREFIX
   :annotation: = [ready] 

   

.. data:: LOG_FORMAT
   

   

.. data:: SIMPLE_LOG_FORMAT
   

   

.. data:: SQL_ALCHEMY_CONN
   :annotation: :Optional[str]

   

.. data:: PLUGINS_FOLDER
   :annotation: :Optional[str]

   

.. data:: LOGGING_CLASS_PATH
   :annotation: :Optional[str]

   

.. data:: DAGS_FOLDER
   :annotation: :str

   

.. data:: engine
   :annotation: :Optional[Engine]

   

.. data:: Session
   :annotation: :Optional[SASession]

   

.. data:: json
   

   

.. data:: STATE_COLORS
   

   

.. function:: _get_rich_console(file)

.. function:: custom_show_warning(message, category, filename, lineno, file=None, line=None)
   Custom function to print rich and visible warnings


.. data:: showwarning
   

   

.. function:: task_policy(task) -> None
   This policy setting allows altering tasks after they are loaded in
   the DagBag. It allows administrator to rewire some task's parameters.
   Alternatively you can raise ``AirflowClusterPolicyViolation`` exception
   to stop DAG from being executed.

   To define policy, add a ``airflow_local_settings`` module
   to your PYTHONPATH that defines this ``task_policy`` function.

   Here are a few examples of how this can be useful:

   * You could enforce a specific queue (say the ``spark`` queue)
       for tasks using the ``SparkOperator`` to make sure that these
       tasks get wired to the right workers
   * You could enforce a task timeout policy, making sure that no tasks run
       for more than 48 hours

   :param task: task to be mutated
   :type task: airflow.models.baseoperator.BaseOperator


.. function:: dag_policy(dag) -> None
   This policy setting allows altering DAGs after they are loaded in
   the DagBag. It allows administrator to rewire some DAG's parameters.
   Alternatively you can raise ``AirflowClusterPolicyViolation`` exception
   to stop DAG from being executed.

   To define policy, add a ``airflow_local_settings`` module
   to your PYTHONPATH that defines this ``dag_policy`` function.

   Here are a few examples of how this can be useful:

   * You could enforce default user for DAGs
   * Check if every DAG has configured tags

   :param dag: dag to be mutated
   :type dag: airflow.models.dag.DAG


.. function:: task_instance_mutation_hook(task_instance)
   This setting allows altering task instances before they are queued by
   the Airflow scheduler.

   To define task_instance_mutation_hook, add a ``airflow_local_settings`` module
   to your PYTHONPATH that defines this ``task_instance_mutation_hook`` function.

   This could be used, for instance, to modify the task instance during retries.

   :param task_instance: task instance to be mutated
   :type task_instance: airflow.models.taskinstance.TaskInstance


.. function:: pod_mutation_hook(pod)
   This setting allows altering ``kubernetes.client.models.V1Pod`` object
   before they are passed to the Kubernetes client by the ``PodLauncher``
   for scheduling.

   To define a pod mutation hook, add a ``airflow_local_settings`` module
   to your PYTHONPATH that defines this ``pod_mutation_hook`` function.
   It receives a ``Pod`` object and can alter it where needed.

   This could be used, for instance, to add sidecar or init containers
   to every worker pod launched by KubernetesExecutor or KubernetesPodOperator.


.. function:: configure_vars()
   Configure Global Variables from airflow.cfg


.. function:: configure_orm(disable_connection_pool=False)
   Configure ORM using SQLAlchemy


.. function:: prepare_engine_args(disable_connection_pool=False)
   Prepare SQLAlchemy engine args


.. function:: dispose_orm()
   Properly close pooled database connections


.. function:: configure_adapters()
   Register Adapters and DB Converters


.. function:: validate_session()
   Validate ORM Session


.. function:: configure_action_logging()
   Any additional configuration (register callback) for airflow.utils.action_loggers
   module
   :rtype: None


.. function:: prepare_syspath()
   Ensures that certain subfolders of AIRFLOW_HOME are on the classpath


.. function:: get_session_lifetime_config()
   Gets session timeout configs and handles outdated configs gracefully.


.. function:: import_local_settings()
   Import airflow_local_settings.py files to allow overriding any configs in settings.py file


.. function:: initialize()
   Initialize Airflow with all the settings from this file


.. data:: KILOBYTE
   :annotation: = 1024

   

.. data:: MEGABYTE
   

   

.. data:: WEB_COLORS
   

   

.. data:: MIN_SERIALIZED_DAG_UPDATE_INTERVAL
   

   

.. data:: MIN_SERIALIZED_DAG_FETCH_INTERVAL
   

   

.. data:: STORE_DAG_CODE
   

   

.. data:: DONOT_MODIFY_HANDLERS
   

   

.. data:: CAN_FORK
   

   

.. data:: EXECUTE_TASKS_NEW_PYTHON_INTERPRETER
   

   

.. data:: ALLOW_FUTURE_EXEC_DATES
   

   

.. data:: CHECK_SLAS
   

   

.. data:: MAX_DB_RETRIES
   

   

.. data:: USE_JOB_SCHEDULE
   

   

.. data:: LAZY_LOAD_PLUGINS
   

   

.. data:: IS_K8S_OR_K8SCELERY_EXECUTOR
   

   

