:mod:`airflow.providers.apache.livy.hooks.livy`
===============================================

.. py:module:: airflow.providers.apache.livy.hooks.livy

.. autoapi-nested-parse::

   This module contains the Apache Livy hook.



Module Contents
---------------

.. py:class:: BatchState

   Bases: :class:`enum.Enum`

   Batch session states

   .. attribute:: NOT_STARTED
      :annotation: = not_started

      

   .. attribute:: STARTING
      :annotation: = starting

      

   .. attribute:: RUNNING
      :annotation: = running

      

   .. attribute:: IDLE
      :annotation: = idle

      

   .. attribute:: BUSY
      :annotation: = busy

      

   .. attribute:: SHUTTING_DOWN
      :annotation: = shutting_down

      

   .. attribute:: ERROR
      :annotation: = error

      

   .. attribute:: DEAD
      :annotation: = dead

      

   .. attribute:: KILLED
      :annotation: = killed

      

   .. attribute:: SUCCESS
      :annotation: = success

      


.. py:class:: LivyHook(livy_conn_id: str = 'livy_default')

   Bases: :class:`airflow.providers.http.hooks.http.HttpHook`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   Hook for Apache Livy through the REST API.

   :param livy_conn_id: reference to a pre-defined Livy Connection.
   :type livy_conn_id: str

   .. seealso::
       For more details refer to the Apache Livy API reference:
       https://livy.apache.org/docs/latest/rest-api.html

   .. attribute:: TERMINAL_STATES
      

      

   .. attribute:: _def_headers
      

      

   
   .. method:: get_conn(self, headers: Optional[Dict[str, Any]] = None)

      Returns http session for use with requests

      :param headers: additional headers to be passed through as a dictionary
      :type headers: dict
      :return: requests session
      :rtype: requests.Session



   
   .. method:: run_method(self, endpoint: str, method: str = 'GET', data: Optional[Any] = None, headers: Optional[Dict[str, Any]] = None, extra_options: Optional[Dict[Any, Any]] = None)

      Wrapper for HttpHook, allows to change method on the same HttpHook

      :param method: http method
      :type method: str
      :param endpoint: endpoint
      :type endpoint: str
      :param data: request payload
      :type data: dict
      :param headers: headers
      :type headers: dict
      :param extra_options: extra options
      :type extra_options: dict
      :return: http response
      :rtype: requests.Response



   
   .. method:: post_batch(self, *args, **kwargs)

      Perform request to submit batch

      :return: batch session id
      :rtype: int



   
   .. method:: get_batch(self, session_id: Union[int, str])

      Fetch info about the specified batch

      :param session_id: identifier of the batch sessions
      :type session_id: int
      :return: response body
      :rtype: dict



   
   .. method:: get_batch_state(self, session_id: Union[int, str])

      Fetch the state of the specified batch

      :param session_id: identifier of the batch sessions
      :type session_id: Union[int, str]
      :return: batch state
      :rtype: BatchState



   
   .. method:: delete_batch(self, session_id: Union[int, str])

      Delete the specified batch

      :param session_id: identifier of the batch sessions
      :type session_id: int
      :return: response body
      :rtype: dict



   
   .. staticmethod:: _validate_session_id(session_id: Union[int, str])

      Validate session id is a int

      :param session_id: session id
      :type session_id: Union[int, str]



   
   .. staticmethod:: _parse_post_response(response: Dict[Any, Any])

      Parse batch response for batch id

      :param response: response body
      :type response: dict
      :return: session id
      :rtype: int



   
   .. staticmethod:: build_post_batch_body(file: str, args: Optional[Sequence[Union[str, int, float]]] = None, class_name: Optional[str] = None, jars: Optional[List[str]] = None, py_files: Optional[List[str]] = None, files: Optional[List[str]] = None, archives: Optional[List[str]] = None, name: Optional[str] = None, driver_memory: Optional[str] = None, driver_cores: Optional[Union[int, str]] = None, executor_memory: Optional[str] = None, executor_cores: Optional[int] = None, num_executors: Optional[Union[int, str]] = None, queue: Optional[str] = None, proxy_user: Optional[str] = None, conf: Optional[Dict[Any, Any]] = None)

      Build the post batch request body.
      For more information about the format refer to
      .. seealso:: https://livy.apache.org/docs/latest/rest-api.html

      :param file: Path of the file containing the application to execute (required).
      :type file: str
      :param proxy_user: User to impersonate when running the job.
      :type proxy_user: str
      :param class_name: Application Java/Spark main class string.
      :type class_name: str
      :param args: Command line arguments for the application s.
      :type args: Sequence[Union[str, int, float]]
      :param jars: jars to be used in this sessions.
      :type jars: Sequence[str]
      :param py_files: Python files to be used in this session.
      :type py_files: Sequence[str]
      :param files: files to be used in this session.
      :type files: Sequence[str]
      :param driver_memory: Amount of memory to use for the driver process  string.
      :type driver_memory: str
      :param driver_cores: Number of cores to use for the driver process int.
      :type driver_cores: Union[str, int]
      :param executor_memory: Amount of memory to use per executor process  string.
      :type executor_memory: str
      :param executor_cores: Number of cores to use for each executor  int.
      :type executor_cores: Union[int, str]
      :param num_executors: Number of executors to launch for this session  int.
      :type num_executors: Union[str, int]
      :param archives: Archives to be used in this session.
      :type archives: Sequence[str]
      :param queue: The name of the YARN queue to which submitted string.
      :type queue: str
      :param name: The name of this session string.
      :type name: str
      :param conf: Spark configuration properties.
      :type conf: dict
      :return: request body
      :rtype: dict



   
   .. staticmethod:: _validate_size_format(size: str)

      Validate size format.

      :param size: size value
      :type size: str
      :return: true if valid format
      :rtype: bool



   
   .. staticmethod:: _validate_list_of_stringables(vals: Sequence[Union[str, int, float]])

      Check the values in the provided list can be converted to strings.

      :param vals: list to validate
      :type vals: Sequence[Union[str, int, float]]
      :return: true if valid
      :rtype: bool



   
   .. staticmethod:: _validate_extra_conf(conf: Dict[Any, Any])

      Check configuration values are either strings or ints.

      :param conf: configuration variable
      :type conf: dict
      :return: true if valid
      :rtype: bool




