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

:py:mod:`airflow.providers.amazon.aws.utils.task_log_fetcher`
=============================================================

.. py:module:: airflow.providers.amazon.aws.utils.task_log_fetcher


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.task_log_fetcher.AwsTaskLogFetcher




.. py:class:: AwsTaskLogFetcher(*, log_group, log_stream_name, fetch_interval, logger, aws_conn_id = 'aws_default', region_name = None)


   Bases: :py:obj:`threading.Thread`

   Fetch Cloudwatch log events with specific interval and send the log events to the logger.info.

   .. py:method:: run()

      Method representing the thread's activity.

      You may override this method in a subclass. The standard run() method
      invokes the callable object passed to the object's constructor as the
      target argument, if any, with sequential and keyword arguments taken
      from the args and kwargs arguments, respectively.



   .. py:method:: event_to_str(event)
      :staticmethod:


   .. py:method:: get_last_log_messages(number_messages)

      Get the last logs messages in one single request.

       NOTE: some restrictions apply:
       - if logs are too old, the response will be empty
       - the max number of messages we can retrieve is constrained by cloudwatch limits (10,000).


   .. py:method:: get_last_log_message()


   .. py:method:: is_stopped()


   .. py:method:: stop()
