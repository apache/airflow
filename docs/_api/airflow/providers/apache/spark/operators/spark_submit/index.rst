:mod:`airflow.providers.apache.spark.operators.spark_submit`
============================================================

.. py:module:: airflow.providers.apache.spark.operators.spark_submit


Module Contents
---------------

.. py:class:: SparkSubmitOperator(*, application: str = '', conf: Optional[Dict[str, Any]] = None, conn_id: str = 'spark_default', files: Optional[str] = None, py_files: Optional[str] = None, archives: Optional[str] = None, driver_class_path: Optional[str] = None, jars: Optional[str] = None, java_class: Optional[str] = None, packages: Optional[str] = None, exclude_packages: Optional[str] = None, repositories: Optional[str] = None, total_executor_cores: Optional[int] = None, executor_cores: Optional[int] = None, executor_memory: Optional[str] = None, driver_memory: Optional[str] = None, keytab: Optional[str] = None, principal: Optional[str] = None, proxy_user: Optional[str] = None, name: str = 'arrow-spark', num_executors: Optional[int] = None, status_poll_interval: int = 1, application_args: Optional[List[Any]] = None, env_vars: Optional[Dict[str, Any]] = None, verbose: bool = False, spark_binary: Optional[str] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
   It requires that the "spark-submit" binary is in the PATH or the spark-home is set
   in the extra on the connection.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SparkSubmitOperator`

   :param application: The application that submitted as a job, either jar or py file. (templated)
   :type application: str
   :param conf: Arbitrary Spark configuration properties (templated)
   :type conf: dict
   :param conn_id: The connection id as configured in Airflow administration. When an
                   invalid connection_id is supplied, it will default to yarn.
   :type conn_id: str
   :param files: Upload additional files to the executor running the job, separated by a
                 comma. Files will be placed in the working directory of each executor.
                 For example, serialized objects. (templated)
   :type files: str
   :param py_files: Additional python files used by the job, can be .zip, .egg or .py. (templated)
   :type py_files: str
   :param jars: Submit additional jars to upload and place them in executor classpath. (templated)
   :type jars: str
   :param driver_class_path: Additional, driver-specific, classpath settings. (templated)
   :type driver_class_path: str
   :param java_class: the main class of the Java application
   :type java_class: str
   :param packages: Comma-separated list of maven coordinates of jars to include on the
                    driver and executor classpaths. (templated)
   :type packages: str
   :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
                            while resolving the dependencies provided in 'packages' (templated)
   :type exclude_packages: str
   :param repositories: Comma-separated list of additional remote repositories to search
                        for the maven coordinates given with 'packages'
   :type repositories: str
   :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
                                (Default: all the available cores on the worker)
   :type total_executor_cores: int
   :param executor_cores: (Standalone & YARN only) Number of cores per executor (Default: 2)
   :type executor_cores: int
   :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
   :type executor_memory: str
   :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
   :type driver_memory: str
   :param keytab: Full path to the file that contains the keytab (templated)
   :type keytab: str
   :param principal: The name of the kerberos principal used for keytab (templated)
   :type principal: str
   :param proxy_user: User to impersonate when submitting the application (templated)
   :type proxy_user: str
   :param name: Name of the job (default airflow-spark). (templated)
   :type name: str
   :param num_executors: Number of executors to launch
   :type num_executors: int
   :param status_poll_interval: Seconds to wait between polls of driver status in cluster
       mode (Default: 1)
   :type status_poll_interval: int
   :param application_args: Arguments for the application being submitted (templated)
   :type application_args: list
   :param env_vars: Environment variables for spark-submit. It supports yarn and k8s mode too. (templated)
   :type env_vars: dict
   :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
   :type verbose: bool
   :param spark_binary: The command to use for spark submit.
                        Some distros may use spark2-submit.
   :type spark_binary: str

   .. attribute:: template_fields
      :annotation: = ['_application', '_conf', '_files', '_py_files', '_jars', '_driver_class_path', '_packages', '_exclude_packages', '_keytab', '_principal', '_proxy_user', '_name', '_application_args', '_env_vars']

      

   .. attribute:: ui_color
      

      

   
   .. method:: execute(self, context: Dict[str, Any])

      Call the SparkSubmitHook to run the provided spark job



   
   .. method:: on_kill(self)



   
   .. method:: _get_hook(self)




