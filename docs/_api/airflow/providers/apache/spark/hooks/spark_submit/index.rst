:mod:`airflow.providers.apache.spark.hooks.spark_submit`
========================================================

.. py:module:: airflow.providers.apache.spark.hooks.spark_submit


Module Contents
---------------

.. py:class:: SparkSubmitHook(conf: Optional[Dict[str, Any]] = None, conn_id: str = 'spark_default', files: Optional[str] = None, py_files: Optional[str] = None, archives: Optional[str] = None, driver_class_path: Optional[str] = None, jars: Optional[str] = None, java_class: Optional[str] = None, packages: Optional[str] = None, exclude_packages: Optional[str] = None, repositories: Optional[str] = None, total_executor_cores: Optional[int] = None, executor_cores: Optional[int] = None, executor_memory: Optional[str] = None, driver_memory: Optional[str] = None, keytab: Optional[str] = None, principal: Optional[str] = None, proxy_user: Optional[str] = None, name: str = 'default-name', num_executors: Optional[int] = None, status_poll_interval: int = 1, application_args: Optional[List[Any]] = None, env_vars: Optional[Dict[str, Any]] = None, verbose: bool = False, spark_binary: Optional[str] = None)

   Bases: :class:`airflow.hooks.base_hook.BaseHook`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   This hook is a wrapper around the spark-submit binary to kick off a spark-submit job.
   It requires that the "spark-submit" binary is in the PATH or the spark_home to be
   supplied.

   :param conf: Arbitrary Spark configuration properties
   :type conf: dict
   :param conn_id: The connection id as configured in Airflow administration. When an
       invalid connection_id is supplied, it will default to yarn.
   :type conn_id: str
   :param files: Upload additional files to the executor running the job, separated by a
       comma. Files will be placed in the working directory of each executor.
       For example, serialized objects.
   :type files: str
   :param py_files: Additional python files used by the job, can be .zip, .egg or .py.
   :type py_files: str
   :param: archives: Archives that spark should unzip (and possibly tag with #ALIAS) into
       the application working directory.
   :param driver_class_path: Additional, driver-specific, classpath settings.
   :type driver_class_path: str
   :param jars: Submit additional jars to upload and place them in executor classpath.
   :type jars: str
   :param java_class: the main class of the Java application
   :type java_class: str
   :param packages: Comma-separated list of maven coordinates of jars to include on the
       driver and executor classpaths
   :type packages: str
   :param exclude_packages: Comma-separated list of maven coordinates of jars to exclude
       while resolving the dependencies provided in 'packages'
   :type exclude_packages: str
   :param repositories: Comma-separated list of additional remote repositories to search
       for the maven coordinates given with 'packages'
   :type repositories: str
   :param total_executor_cores: (Standalone & Mesos only) Total cores for all executors
       (Default: all the available cores on the worker)
   :type total_executor_cores: int
   :param executor_cores: (Standalone, YARN and Kubernetes only) Number of cores per
       executor (Default: 2)
   :type executor_cores: int
   :param executor_memory: Memory per executor (e.g. 1000M, 2G) (Default: 1G)
   :type executor_memory: str
   :param driver_memory: Memory allocated to the driver (e.g. 1000M, 2G) (Default: 1G)
   :type driver_memory: str
   :param keytab: Full path to the file that contains the keytab
   :type keytab: str
   :param principal: The name of the kerberos principal used for keytab
   :type principal: str
   :param proxy_user: User to impersonate when submitting the application
   :type proxy_user: str
   :param name: Name of the job (default airflow-spark)
   :type name: str
   :param num_executors: Number of executors to launch
   :type num_executors: int
   :param status_poll_interval: Seconds to wait between polls of driver status in cluster
       mode (Default: 1)
   :type status_poll_interval: int
   :param application_args: Arguments for the application being submitted
   :type application_args: list
   :param env_vars: Environment variables for spark-submit. It
       supports yarn and k8s mode too.
   :type env_vars: dict
   :param verbose: Whether to pass the verbose flag to spark-submit process for debugging
   :type verbose: bool
   :param spark_binary: The command to use for spark submit.
                        Some distros may use spark2-submit.
   :type spark_binary: str

   
   .. method:: _resolve_should_track_driver_status(self)

      Determines whether or not this hook should poll the spark driver status through
      subsequent spark-submit status requests after the initial spark-submit request
      :return: if the driver status should be tracked



   
   .. method:: _resolve_connection(self)



   
   .. method:: get_conn(self)



   
   .. method:: _get_spark_binary_path(self)



   
   .. method:: _mask_cmd(self, connection_cmd: Union[str, List[str]])



   
   .. method:: _build_spark_submit_command(self, application: str)

      Construct the spark-submit command to execute.

      :param application: command to append to the spark-submit command
      :type application: str
      :return: full command to be executed



   
   .. method:: _build_track_driver_status_command(self)

      Construct the command to poll the driver status.

      :return: full command to be executed



   
   .. method:: submit(self, application: str = '', **kwargs)

      Remote Popen to execute the spark-submit job

      :param application: Submitted application, jar or py file
      :type application: str
      :param kwargs: extra arguments to Popen (see subprocess.Popen)



   
   .. method:: _process_spark_submit_log(self, itr: Iterator[Any])

      Processes the log files and extracts useful information out of it.

      If the deploy-mode is 'client', log the output of the submit command as those
      are the output logs of the Spark worker directly.

      Remark: If the driver needs to be tracked for its status, the log-level of the
      spark deploy needs to be at least INFO (log4j.logger.org.apache.spark.deploy=INFO)

      :param itr: An iterator which iterates over the input of the subprocess



   
   .. method:: _process_spark_status_log(self, itr: Iterator[Any])

      Parses the logs of the spark driver status query process

      :param itr: An iterator which iterates over the input of the subprocess



   
   .. method:: _start_driver_status_tracking(self)

      Polls the driver based on self._driver_id to get the status.
      Finish successfully when the status is FINISHED.
      Finish failed when the status is ERROR/UNKNOWN/KILLED/FAILED.

      Possible status:

      SUBMITTED
          Submitted but not yet scheduled on a worker
      RUNNING
          Has been allocated to a worker to run
      FINISHED
          Previously ran and exited cleanly
      RELAUNCHING
          Exited non-zero or due to worker failure, but has not yet
          started running again
      UNKNOWN
          The status of the driver is temporarily not known due to
          master failure recovery
      KILLED
          A user manually killed this driver
      FAILED
          The driver exited non-zero and was not supervised
      ERROR
          Unable to run or restart due to an unrecoverable error
          (e.g. missing jar file)



   
   .. method:: _build_spark_driver_kill_command(self)

      Construct the spark-submit command to kill a driver.
      :return: full command to kill a driver



   
   .. method:: on_kill(self)

      Kill Spark submit command




