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

:py:mod:`airflow.providers.amazon.aws.operators.athena`
=======================================================

.. py:module:: airflow.providers.amazon.aws.operators.athena


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.athena.AthenaOperator




.. py:class:: AthenaOperator(*, query, database, output_location, aws_conn_id = 'aws_default', client_request_token = None, workgroup = 'primary', query_execution_context = None, result_configuration = None, sleep_time = 30, max_polling_attempts = None, log_query = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   An operator that submits a presto query to athena.

   .. note:: if the task is killed while it runs, it'll cancel the athena query that was launched,
       EXCEPT if running in deferrable mode.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:AthenaOperator`

   :param query: Presto to be run on athena. (templated)
   :param database: Database to select. (templated)
   :param output_location: s3 path to write the query results into. (templated)
   :param aws_conn_id: aws connection to use
   :param client_request_token: Unique token created by user to avoid multiple executions of same query
   :param workgroup: Athena workgroup in which query will be run. (templated)
   :param query_execution_context: Context in which query need to be run
   :param result_configuration: Dict with path to store results in and config related to encryption
   :param sleep_time: Time (in seconds) to wait between two consecutive calls to check query status on Athena
   :param max_polling_attempts: Number of times to poll for query state before function exits
       To limit task execution time, use execution_timeout.
   :param log_query: Whether to log athena query and other execution params when it's executed.
       Defaults to *True*.

   .. py:attribute:: ui_color
      :value: '#44b5e2'



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('query', 'database', 'output_location', 'workgroup')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ('.sql',)



   .. py:attribute:: template_fields_renderers



   .. py:method:: hook()

      Create and return an AthenaHook.


   .. py:method:: execute(context)

      Run Presto Query on Athena.


   .. py:method:: execute_complete(context, event=None)


   .. py:method:: on_kill()

      Cancel the submitted athena query.
