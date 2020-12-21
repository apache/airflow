:mod:`airflow.utils.log.log_reader`
===================================

.. py:module:: airflow.utils.log.log_reader


Module Contents
---------------

.. py:class:: TaskLogReader

   Task log reader

   .. attribute:: supports_read
      

      Checks if a read operation is supported by a current log handler.


   .. attribute:: supports_external_link
      

      Check if the logging handler supports external links (e.g. to Elasticsearch, Stackdriver, etc).


   
   .. method:: read_log_chunks(self, ti: TaskInstance, try_number: Optional[int], metadata)

      Reads chunks of Task Instance logs.

      :param ti: The taskInstance
      :type ti: TaskInstance
      :param try_number: If provided, logs for the given try will be returned.
          Otherwise, logs from all attempts are returned.
      :type try_number: Optional[int]
      :param metadata: A dictionary containing information about how to read the task log
      :type metadata: dict
      :rtype: Tuple[List[str], Dict[str, Any]]

      The following is an example of how to use this method to read log:

      .. code-block:: python

          logs, metadata = task_log_reader.read_log_chunks(ti, try_number, metadata)
          logs = logs[0] if try_number is not None else logs

      where task_log_reader is an instance of TaskLogReader. The metadata will always
      contain information about the task log which can enable you read logs to the
      end.



   
   .. method:: read_log_stream(self, ti: TaskInstance, try_number: Optional[int], metadata: dict)

      Used to continuously read log to the end

      :param ti: The Task Instance
      :type ti: TaskInstance
      :param try_number: the task try number
      :type try_number: Optional[int]
      :param metadata: A dictionary containing information about how to read the task log
      :type metadata: dict
      :rtype: Iterator[str]



   
   .. method:: log_handler(self)

      Log handler, which is configured to read logs.



   
   .. method:: render_log_filename(self, ti: TaskInstance, try_number: Optional[int] = None)

      Renders the log attachment filename

      :param ti: The task instance
      :type ti: TaskInstance
      :param try_number: The task try number
      :type try_number: Optional[int]
      :rtype: str




