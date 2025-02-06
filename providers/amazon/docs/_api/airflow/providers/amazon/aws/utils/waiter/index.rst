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

:py:mod:`airflow.providers.amazon.aws.utils.waiter`
===================================================

.. py:module:: airflow.providers.amazon.aws.utils.waiter


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.waiter.waiter
   airflow.providers.amazon.aws.utils.waiter.get_state



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.waiter.log


.. py:data:: log



.. py:function:: waiter(get_state_callable, get_state_args, parse_response, desired_state, failure_states, object_type, action, countdown = 25 * 60, check_interval_seconds = 60)

   Call get_state_callable until it reaches the desired_state or the failure_states.

   PLEASE NOTE:  While not yet deprecated, we are moving away from this method
                 and encourage using the custom boto waiters as explained in
                 https://github.com/apache/airflow/tree/main/airflow/providers/amazon/aws/waiters

   :param get_state_callable: A callable to run until it returns True
   :param get_state_args: Arguments to pass to get_state_callable
   :param parse_response: Dictionary keys to extract state from response of get_state_callable
   :param desired_state: Wait until the getter returns this value
   :param failure_states: A set of states which indicate failure and should throw an
       exception if any are reached before the desired_state
   :param object_type: Used for the reporting string. What are you waiting for? (application, job, etc)
   :param action: Used for the reporting string. What action are you waiting for? (created, deleted, etc)
   :param countdown: Number of seconds the waiter should wait for the desired state before timing out.
       Defaults to 25 * 60 seconds. None = infinite.
   :param check_interval_seconds: Number of seconds waiter should wait before attempting
       to retry get_state_callable. Defaults to 60 seconds.


.. py:function:: get_state(response, keys)
