:mod:`airflow.providers.apache.livy.operators.livy`
===================================================

.. py:module:: airflow.providers.apache.livy.operators.livy

.. autoapi-nested-parse::

   This module contains the Apache Livy operator.



Module Contents
---------------

.. py:class:: LivyOperator(*, file: str, class_name: Optional[str] = None, args: Optional[Sequence[Union[str, int, float]]] = None, conf: Optional[Dict[Any, Any]] = None, jars: Optional[Sequence[str]] = None, py_files: Optional[Sequence[str]] = None, files: Optional[Sequence[str]] = None, driver_memory: Optional[str] = None, driver_cores: Optional[Union[int, str]] = None, executor_memory: Optional[str] = None, executor_cores: Optional[Union[int, str]] = None, num_executors: Optional[Union[int, str]] = None, archives: Optional[Sequence[str]] = None, queue: Optional[str] = None, name: Optional[str] = None, proxy_user: Optional[str] = None, livy_conn_id: str = 'livy_default', polling_interval: int = 0, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   This operator wraps the Apache Livy batch REST API, allowing to submit a Spark
   application to the underlying cluster.

   :param file: path of the file containing the application to execute (required).
   :type file: str
   :param class_name: name of the application Java/Spark main class.
   :type class_name: str
   :param args: application command line arguments.
   :type args: list
   :param jars: jars to be used in this sessions.
   :type jars: list
   :param py_files: python files to be used in this session.
   :type py_files: list
   :param files: files to be used in this session.
   :type files: list
   :param driver_memory: amount of memory to use for the driver process.
   :type driver_memory: str
   :param driver_cores: number of cores to use for the driver process.
   :type driver_cores: str, int
   :param executor_memory: amount of memory to use per executor process.
   :type executor_memory: str
   :param executor_cores: number of cores to use for each executor.
   :type executor_cores: str, int
   :param num_executors: number of executors to launch for this session.
   :type num_executors: str, int
   :param archives: archives to be used in this session.
   :type archives: list
   :param queue: name of the YARN queue to which the application is submitted.
   :type queue: str
   :param name: name of this session.
   :type name: str
   :param conf: Spark configuration properties.
   :type conf: dict
   :param proxy_user: user to impersonate when running the job.
   :type proxy_user: str
   :param livy_conn_id: reference to a pre-defined Livy Connection.
   :type livy_conn_id: str
   :param polling_interval: time in seconds between polling for job completion. Don't poll for values >=0
   :type polling_interval: int

   .. attribute:: template_fields
      :annotation: = ['spark_params']

      

   
   .. method:: get_hook(self)

      Get valid hook.

      :return: hook
      :rtype: LivyHook



   
   .. method:: execute(self, context: Dict[Any, Any])



   
   .. method:: poll_for_termination(self, batch_id: Union[int, str])

      Pool Livy for batch termination.

      :param batch_id: id of the batch session to monitor.
      :type batch_id: int



   
   .. method:: on_kill(self)



   
   .. method:: kill(self)

      Delete the current batch session.




