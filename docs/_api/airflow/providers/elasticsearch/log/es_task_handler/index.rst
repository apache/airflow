:mod:`airflow.providers.elasticsearch.log.es_task_handler`
==========================================================

.. py:module:: airflow.providers.elasticsearch.log.es_task_handler


Module Contents
---------------

.. data:: EsLogMsgType
   

   

.. py:class:: ElasticsearchTaskHandler(base_log_folder: str, filename_template: str, log_id_template: str, end_of_log_mark: str, write_stdout: bool, json_format: bool, json_fields: str, host: str = 'localhost:9200', frontend: str = 'localhost:5601', es_kwargs: Optional[dict] = conf.getsection('elasticsearch_configs'))

   Bases: :class:`airflow.utils.log.file_task_handler.FileTaskHandler`, :class:`airflow.utils.log.logging_mixin.LoggingMixin`

   ElasticsearchTaskHandler is a python log handler that
   reads logs from Elasticsearch. Note logs are not directly
   indexed into Elasticsearch. Instead, it flushes logs
   into local files. Additional software setup is required
   to index the log into Elasticsearch, such as using
   Filebeat and Logstash.
   To efficiently query and sort Elasticsearch results, we assume each
   log message has a field `log_id` consists of ti primary keys:
   `log_id = {dag_id}-{task_id}-{execution_date}-{try_number}`
   Log messages with specific log_id are sorted based on `offset`,
   which is a unique integer indicates log message's order.
   Timestamp here are unreliable because multiple log messages
   might have the same timestamp.

   .. attribute:: PAGE
      :annotation: = 0

      

   .. attribute:: MAX_LINE_PER_PAGE
      :annotation: = 1000

      

   .. attribute:: LOG_NAME
      :annotation: = Elasticsearch

      

   .. attribute:: log_name
      

      The log name


   
   .. method:: _render_log_id(self, ti: TaskInstance, try_number: int)



   
   .. staticmethod:: _clean_execution_date(execution_date: datetime)

      Clean up an execution date so that it is safe to query in elasticsearch
      by removing reserved characters.
      # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html#_reserved_characters

      :param execution_date: execution date of the dag run.



   
   .. staticmethod:: _group_logs_by_host(logs)



   
   .. method:: _read_grouped_logs(self)



   
   .. method:: _read(self, ti: TaskInstance, try_number: int, metadata: Optional[dict] = None)

      Endpoint for streaming log.

      :param ti: task instance object
      :param try_number: try_number of the task instance
      :param metadata: log metadata,
                       can be used for steaming log reading and auto-tailing.
      :return: a list of tuple with host and log documents, metadata.



   
   .. method:: es_read(self, log_id: str, offset: str, metadata: dict)

      Returns the logs matching log_id in Elasticsearch and next offset.
      Returns '' if no log is found or there was an error.

      :param log_id: the log_id of the log to read.
      :type log_id: str
      :param offset: the offset start to read log from.
      :type offset: str
      :param metadata: log metadata, used for steaming log download.
      :type metadata: dict



   
   .. method:: set_context(self, ti: TaskInstance)

      Provide task_instance context to airflow task handler.

      :param ti: task instance object



   
   .. method:: close(self)



   
   .. method:: get_external_log_url(self, task_instance: TaskInstance, try_number: int)

      Creates an address for an external log collecting service.

      :param task_instance: task instance object
      :type: task_instance: TaskInstance
      :param try_number: task instance try_number to read logs from.
      :type try_number: Optional[int]
      :return: URL to the external log collection service
      :rtype: str




