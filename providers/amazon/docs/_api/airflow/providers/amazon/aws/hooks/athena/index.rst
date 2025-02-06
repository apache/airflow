 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

:py:mod:`airflow.providers.amazon.aws.hooks.athena`
===================================================

.. py:module:: airflow.providers.amazon.aws.hooks.athena

.. autoapi-nested-parse::

   This module contains AWS Athena hook.

   .. spelling:word-list::

       PageIterator



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.athena.AthenaHook




.. py:class:: AthenaHook(*args, sleep_time = None, log_query = True, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Athena.

   Provide thick wrapper around
   :external+boto3:py:class:`boto3.client("athena") <Athena.Client>`.

   :param sleep_time: obsolete, please use the parameter of `poll_query_status` method instead
   :param log_query: Whether to log athena query and other execution params
       when it's executed. Defaults to *True*.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:attribute:: INTERMEDIATE_STATES
      :value: ('QUEUED', 'RUNNING')



   .. py:attribute:: FAILURE_STATES
      :value: ('FAILED', 'CANCELLED')



   .. py:attribute:: SUCCESS_STATES
      :value: ('SUCCEEDED',)



   .. py:attribute:: TERMINAL_STATES
      :value: ('SUCCEEDED', 'FAILED', 'CANCELLED')



   .. py:method:: run_query(query, query_context, result_configuration, client_request_token = None, workgroup = 'primary')

      Run a Presto query on Athena with provided config.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.start_query_execution`

      :param query: Presto query to run.
      :param query_context: Context in which query need to be run.
      :param result_configuration: Dict with path to store results in and
          config related to encryption.
      :param client_request_token: Unique token created by user to avoid
          multiple executions of same query.
      :param workgroup: Athena workgroup name, when not specified, will be
          ``'primary'``.
      :return: Submitted query execution ID.


   .. py:method:: check_query_status(query_execution_id)

      Fetch the state of a submitted query.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.get_query_execution`

      :param query_execution_id: Id of submitted athena query
      :return: One of valid query states, or *None* if the response is
          malformed.


   .. py:method:: get_state_change_reason(query_execution_id)

      Fetch the reason for a state change (e.g. error message). Returns None or reason string.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.get_query_execution`

      :param query_execution_id: Id of submitted athena query


   .. py:method:: get_query_results(query_execution_id, next_token_id = None, max_results = 1000)

      Fetch submitted query results.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.get_query_results`

      :param query_execution_id: Id of submitted athena query
      :param next_token_id:  The token that specifies where to start pagination.
      :param max_results: The maximum number of results (rows) to return in this request.
      :return: *None* if the query is in intermediate, failed, or cancelled
          state. Otherwise a dict of query outputs.


   .. py:method:: get_query_results_paginator(query_execution_id, max_items = None, page_size = None, starting_token = None)

      Fetch submitted Athena query results.

      .. seealso::
          - :external+boto3:py:class:`Athena.Paginator.GetQueryResults`

      :param query_execution_id: Id of submitted athena query
      :param max_items: The total number of items to return.
      :param page_size: The size of each page.
      :param starting_token: A token to specify where to start paginating.
      :return: *None* if the query is in intermediate, failed, or cancelled
          state. Otherwise a paginator to iterate through pages of results.

      Call :meth`.build_full_result()` on the returned paginator to get all
      results at once.


   .. py:method:: poll_query_status(query_execution_id, max_polling_attempts = None, sleep_time = None)

      Poll the state of a submitted query until it reaches final state.

      :param query_execution_id: ID of submitted athena query
      :param max_polling_attempts: Number of times to poll for query state before function exits
      :param sleep_time: Time (in seconds) to wait between two consecutive query status checks.
      :return: One of the final states


   .. py:method:: get_output_location(query_execution_id)

      Get the output location of the query results in S3 URI format.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.get_query_execution`

      :param query_execution_id: Id of submitted athena query


   .. py:method:: stop_query(query_execution_id)

      Cancel the submitted query.

      .. seealso::
          - :external+boto3:py:meth:`Athena.Client.stop_query_execution`

      :param query_execution_id: Id of submitted athena query
