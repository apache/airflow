:mod:`airflow.providers.amazon.aws.operators.athena`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.athena


Module Contents
---------------

.. py:class:: AWSAthenaOperator(*, query: str, database: str, output_location: str, aws_conn_id: str = 'aws_default', client_request_token: Optional[str] = None, workgroup: str = 'primary', query_execution_context: Optional[Dict[str, str]] = None, result_configuration: Optional[Dict[str, Any]] = None, sleep_time: int = 30, max_tries: Optional[int] = None, **kwargs)

   Bases: :class:`airflow.models.BaseOperator`

   An operator that submits a presto query to athena.

   :param query: Presto to be run on athena. (templated)
   :type query: str
   :param database: Database to select. (templated)
   :type database: str
   :param output_location: s3 path to write the query results into. (templated)
   :type output_location: str
   :param aws_conn_id: aws connection to use
   :type aws_conn_id: str
   :param client_request_token: Unique token created by user to avoid multiple executions of same query
   :type client_request_token: str
   :param workgroup: Athena workgroup in which query will be run
   :type workgroup: str
   :param query_execution_context: Context in which query need to be run
   :type query_execution_context: dict
   :param result_configuration: Dict with path to store results in and config related to encryption
   :type result_configuration: dict
   :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
   :type sleep_time: int
   :param max_tries: Number of times to poll for query state before function exits
   :type max_tries: int

   .. attribute:: ui_color
      :annotation: = #44b5e2

      

   .. attribute:: template_fields
      :annotation: = ['query', 'database', 'output_location']

      

   .. attribute:: template_ext
      :annotation: = ['.sql']

      

   
   .. method:: hook(self)

      Create and return an AWSAthenaHook.



   
   .. method:: execute(self, context: dict)

      Run Presto Query on Athena



   
   .. method:: on_kill(self)

      Cancel the submitted athena query




