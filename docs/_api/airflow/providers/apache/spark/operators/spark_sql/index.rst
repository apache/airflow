:mod:`airflow.providers.apache.spark.operators.spark_sql`
=========================================================

.. py:module:: airflow.providers.apache.spark.operators.spark_sql


Module Contents
---------------

.. py:class:: SparkSqlOperator(*, sql: str, conf: Optional[str] = None, conn_id: str = 'spark_sql_default', total_executor_cores: Optional[int] = None, executor_cores: Optional[int] = None, executor_memory: Optional[str] = None, keytab: Optional[str] = None, principal: Optional[str] = None, master: str = 'yarn', name: str = 'default-name', num_executors: Optional[int] = None, verbose: bool = True, yarn_queue: str = 'default', **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   Execute Spark SQL query

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SparkSqlOperator`

   :param sql: The SQL query to execute. (templated)
   :type sql: str
   :param conf: arbitrary Spark configuration property
   :type conf: str (format: PROP=VALUE)
   :param conn_id: connection_id string
   :type conn_id: str
   :param total_executor_cores: (Standalone & Mesos only) Total cores for all
       executors (Default: all the available cores on the worker)
   :type total_executor_cores: int
   :param executor_cores: (Standalone & YARN only) Number of cores per
       executor (Default: 2)
   :type executor_cores: int
   :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
   :type executor_memory: str
   :param keytab: Full path to the file that contains the keytab
   :type keytab: str
   :param master: spark://host:port, mesos://host:port, yarn, or local
   :type master: str
   :param name: Name of the job
   :type name: str
   :param num_executors: Number of executors to launch
   :type num_executors: int
   :param verbose: Whether to pass the verbose flag to spark-sql
   :type verbose: bool
   :param yarn_queue: The YARN queue to submit to (Default: "default")
   :type yarn_queue: str

   .. attribute:: template_fields
      :annotation: = ['_sql']

      

   .. attribute:: template_ext
      :annotation: = ['.sql', '.hql']

      

   
   .. method:: execute(self, context: Dict[str, Any])

      Call the SparkSqlHook to run the provided sql query



   
   .. method:: on_kill(self)



   
   .. method:: _get_hook(self)

      Get SparkSqlHook




