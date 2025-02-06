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

:py:mod:`airflow.providers.amazon.aws.triggers.base`
====================================================

.. py:module:: airflow.providers.amazon.aws.triggers.base


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.base.AwsBaseWaiterTrigger




.. py:class:: AwsBaseWaiterTrigger(*, serialized_fields, waiter_name, waiter_args, failure_message, status_message, status_queries, return_key = 'value', return_value, waiter_delay, waiter_max_attempts, aws_conn_id, region_name = None)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Base class for all AWS Triggers that follow the "standard" model of just waiting on a waiter.

   Subclasses need to implement the hook() method.

   :param serialized_fields: Fields that are specific to the subclass trigger and need to be serialized
       to be passed to the __init__ method on deserialization.
       The conn id, region, and waiter delay & attempts are always serialized.
       format: {<parameter_name>: <parameter_value>}

   :param waiter_name: The name of the (possibly custom) boto waiter to use.

   :param waiter_args: The arguments to pass to the waiter.
   :param failure_message: The message to log if a failure state is reached.
   :param status_message: The message logged when printing the status of the service.
   :param status_queries: A list containing the JMESPath queries to retrieve status information from
       the waiter response. See https://jmespath.org/tutorial.html

   :param return_key: The key to use for the return_value in the TriggerEvent this emits on success.
       Defaults to "value".
   :param return_value: A value that'll be returned in the return_key field of the TriggerEvent.
       Set to None if there is nothing to return.

   :param waiter_delay: The amount of time in seconds to wait between attempts.
   :param waiter_max_attempts: The maximum number of attempts to be made.
   :param aws_conn_id: The Airflow connection used for AWS credentials. To be used to build the hook.
   :param region_name: The AWS region where the resources to watch are. To be used to build the hook.

   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: hook()
      :abstractmethod:

      Override in subclasses to return the right hook.


   .. py:method:: run()
      :async:

      Run the trigger in an asynchronous context.

      The trigger should yield an Event whenever it wants to fire off
      an event, and return None if it is finished. Single-event triggers
      should thus yield and then immediately return.

      If it yields, it is likely that it will be resumed very quickly,
      but it may not be (e.g. if the workload is being moved to another
      triggerer process, or a multi-event trigger was being used for a
      single-event task defer).

      In either case, Trigger classes should assume they will be persisted,
      and then rely on cleanup() being called when they are no longer needed.
