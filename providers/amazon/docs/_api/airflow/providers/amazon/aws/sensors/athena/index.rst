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

:py:mod:`airflow.providers.amazon.aws.sensors.athena`
=====================================================

.. py:module:: airflow.providers.amazon.aws.sensors.athena


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.athena.AthenaSensor




.. py:class:: AthenaSensor(*, query_execution_id, max_retries = None, aws_conn_id = 'aws_default', sleep_time = 10, **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Poll the state of the Query until it reaches a terminal state; fails if the query fails.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:AthenaSensor`


   :param query_execution_id: query_execution_id to check the state of
   :param max_retries: Number of times to poll for query state before
       returning the current state, defaults to None
   :param aws_conn_id: aws connection to use, defaults to 'aws_default'
   :param sleep_time: Time in seconds to wait between two consecutive call to
       check query status on athena, defaults to 10

   .. py:attribute:: INTERMEDIATE_STATES
      :value: ('QUEUED', 'RUNNING')



   .. py:attribute:: FAILURE_STATES
      :value: ('FAILED', 'CANCELLED')



   .. py:attribute:: SUCCESS_STATES
      :value: ('SUCCEEDED',)



   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('query_execution_id',)



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: ui_color
      :value: '#66c3ff'



   .. py:method:: poke(context)

      Override when deriving this class.


   .. py:method:: hook()

      Create and return an AthenaHook.
