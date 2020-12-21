:mod:`airflow.providers.google.cloud.log.stackdriver_task_handler`
==================================================================

.. py:module:: airflow.providers.google.cloud.log.stackdriver_task_handler

.. autoapi-nested-parse::

   Handler that integrates with Stackdriver



Module Contents
---------------

.. data:: DEFAULT_LOGGER_NAME
   :annotation: = airflow

   

.. data:: _GLOBAL_RESOURCE
   

   

.. data:: _DEFAULT_SCOPESS
   

   

.. py:class:: StackdriverTaskHandler(gcp_key_path: Optional[str] = None, scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS, name: str = DEFAULT_LOGGER_NAME, transport: Type[Transport] = BackgroundThreadTransport, resource: Resource = _GLOBAL_RESOURCE, labels: Optional[Dict[str, str]] = None)

   Bases: :class:`logging.Handler`

   Handler that directly makes Stackdriver logging API calls.

   This is a Python standard ``logging`` handler using that can be used to
   route Python standard logging messages directly to the Stackdriver
   Logging API.

   It can also be used to save logs for executing tasks. To do this, you should set as a handler with
   the name "tasks". In this case, it will also be used to read the log for display in Web UI.

   This handler supports both an asynchronous and synchronous transport.

   :param gcp_key_path: Path to Google Cloud Credential JSON file.
       If omitted, authorization based on `the Application Default Credentials
       <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
       be used.
   :type gcp_key_path: str
   :param scopes: OAuth scopes for the credentials,
   :type scopes: Sequence[str]
   :param name: the name of the custom log in Stackdriver Logging. Defaults
       to 'airflow'. The name of the Python logger will be represented
       in the ``python_logger`` field.
   :type name: str
   :param transport: Class for creating new transport objects. It should
       extend from the base :class:`google.cloud.logging.handlers.Transport` type and
       implement :meth`google.cloud.logging.handlers.Transport.send`. Defaults to
       :class:`google.cloud.logging.handlers.BackgroundThreadTransport`. The other
       option is :class:`google.cloud.logging.handlers.SyncTransport`.
   :type transport: :class:`type`
   :param resource: (Optional) Monitored resource of the entry, defaults
                    to the global resource type.
   :type resource: :class:`~google.cloud.logging.resource.Resource`
   :param labels: (Optional) Mapping of labels for the entry.
   :type labels: dict

   .. attribute:: LABEL_TASK_ID
      :annotation: = task_id

      

   .. attribute:: LABEL_DAG_ID
      :annotation: = dag_id

      

   .. attribute:: LABEL_EXECUTION_DATE
      :annotation: = execution_date

      

   .. attribute:: LABEL_TRY_NUMBER
      :annotation: = try_number

      

   .. attribute:: LOG_VIEWER_BASE_URL
      :annotation: = https://console.cloud.google.com/logs/viewer

      

   .. attribute:: LOG_NAME
      :annotation: = Google Stackdriver

      

   .. attribute:: log_name
      

      Return log name.


   
   .. method:: _client(self)

      Google Cloud Library API client



   
   .. method:: _transport(self)

      Object responsible for sending data to Stackdriver



   
   .. method:: emit(self, record: logging.LogRecord)

      Actually log the specified logging record.

      :param record: The record to be logged.
      :type record: logging.LogRecord



   
   .. method:: set_context(self, task_instance: TaskInstance)

      Configures the logger to add information with information about the current task

      :param task_instance: Currently executed task
      :type task_instance:  :class:`airflow.models.TaskInstance`



   
   .. method:: read(self, task_instance: TaskInstance, try_number: Optional[int] = None, metadata: Optional[Dict] = None)

      Read logs of given task instance from Stackdriver logging.

      :param task_instance: task instance object
      :type task_instance: :class:`airflow.models.TaskInstance`
      :param try_number: task instance try_number to read logs from. If None
         it returns all logs
      :type try_number: Optional[int]
      :param metadata: log metadata. It is used for steaming log reading and auto-tailing.
      :type metadata: Dict
      :return: a tuple of list of logs and list of metadata
      :rtype: Tuple[List[str], List[Dict]]



   
   .. method:: _prepare_log_filter(self, ti_labels: Dict[str, str])

      Prepares the filter that chooses which log entries to fetch.

      More information:
      https://cloud.google.com/logging/docs/reference/v2/rest/v2/entries/list#body.request_body.FIELDS.filter
      https://cloud.google.com/logging/docs/view/advanced-queries

      :param ti_labels: Task Instance's labels that will be used to search for logs
      :type: Dict[str, str]
      :return: logs filter



   
   .. method:: _read_logs(self, log_filter: str, next_page_token: Optional[str], all_pages: bool)

      Sends requests to the Stackdriver service and downloads logs.

      :param log_filter: Filter specifying the logs to be downloaded.
      :type log_filter: str
      :param next_page_token: The token of the page from which the log download will start.
          If None is passed, it will start from the first page.
      :param all_pages: If True is passed, all subpages will be downloaded. Otherwise, only the first
          page will be downloaded
      :return: A token that contains the following items:
          * string with logs
          * Boolean value describing whether there are more logs,
          * token of the next page
      :rtype: Tuple[str, bool, str]



   
   .. method:: _read_single_logs_page(self, log_filter: str, page_token: Optional[str] = None)

      Sends requests to the Stackdriver service and downloads single pages with logs.

      :param log_filter: Filter specifying the logs to be downloaded.
      :type log_filter: str
      :param page_token: The token of the page to be downloaded. If None is passed, the first page will be
          downloaded.
      :type page_token: str
      :return: Downloaded logs and next page token
      :rtype: Tuple[str, str]



   
   .. classmethod:: _task_instance_to_labels(cls, ti: TaskInstance)



   
   .. method:: _resource_path(self)



   
   .. method:: get_external_log_url(self, task_instance: TaskInstance, try_number: int)

      Creates an address for an external log collecting service.
      :param task_instance: task instance object
      :type: task_instance: TaskInstance
      :param try_number: task instance try_number to read logs from.
      :type try_number: Optional[int]
      :return: URL to the external log collection service
      :rtype: str




