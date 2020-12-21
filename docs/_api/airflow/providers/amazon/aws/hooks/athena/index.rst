:mod:`airflow.providers.amazon.aws.hooks.athena`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.athena

.. autoapi-nested-parse::

   This module contains AWS Athena hook



Module Contents
---------------

.. py:class:: AWSAthenaHook(*args, sleep_time: int = 30, **kwargs)

   Bases: :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with AWS Athena to run, poll queries and return query results

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
   :type sleep_time: int

   .. attribute:: INTERMEDIATE_STATES
      :annotation: = ['QUEUED', 'RUNNING']

      

   .. attribute:: FAILURE_STATES
      :annotation: = ['FAILED', 'CANCELLED']

      

   .. attribute:: SUCCESS_STATES
      :annotation: = ['SUCCEEDED']

      

   
   .. method:: run_query(self, query: str, query_context: Dict[str, str], result_configuration: Dict[str, Any], client_request_token: Optional[str] = None, workgroup: str = 'primary')

      Run Presto query on athena with provided config and return submitted query_execution_id

      :param query: Presto query to run
      :type query: str
      :param query_context: Context in which query need to be run
      :type query_context: dict
      :param result_configuration: Dict with path to store results in and config related to encryption
      :type result_configuration: dict
      :param client_request_token: Unique token created by user to avoid multiple executions of same query
      :type client_request_token: str
      :param workgroup: Athena workgroup name, when not specified, will be 'primary'
      :type workgroup: str
      :return: str



   
   .. method:: check_query_status(self, query_execution_id: str)

      Fetch the status of submitted athena query. Returns None or one of valid query states.

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :return: str



   
   .. method:: get_state_change_reason(self, query_execution_id: str)

      Fetch the reason for a state change (e.g. error message). Returns None or reason string.

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :return: str



   
   .. method:: get_query_results(self, query_execution_id: str, next_token_id: Optional[str] = None, max_results: int = 1000)

      Fetch submitted athena query results. returns none if query is in intermediate state or
      failed/cancelled state else dict of query output

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :param next_token_id:  The token that specifies where to start pagination.
      :type next_token_id: str
      :param max_results: The maximum number of results (rows) to return in this request.
      :type max_results: int
      :return: dict



   
   .. method:: get_query_results_paginator(self, query_execution_id: str, max_items: Optional[int] = None, page_size: Optional[int] = None, starting_token: Optional[str] = None)

      Fetch submitted athena query results. returns none if query is in intermediate state or
      failed/cancelled state else a paginator to iterate through pages of results. If you
      wish to get all results at once, call build_full_result() on the returned PageIterator

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :param max_items: The total number of items to return.
      :type max_items: int
      :param page_size: The size of each page.
      :type page_size: int
      :param starting_token: A token to specify where to start paginating.
      :type starting_token: str
      :return: PageIterator



   
   .. method:: poll_query_status(self, query_execution_id: str, max_tries: Optional[int] = None)

      Poll the status of submitted athena query until query state reaches final state.
      Returns one of the final states

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :param max_tries: Number of times to poll for query state before function exits
      :type max_tries: int
      :return: str



   
   .. method:: stop_query(self, query_execution_id: str)

      Cancel the submitted athena query

      :param query_execution_id: Id of submitted athena query
      :type query_execution_id: str
      :return: dict




