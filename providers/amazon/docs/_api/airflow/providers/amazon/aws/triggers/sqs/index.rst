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

:py:mod:`airflow.providers.amazon.aws.triggers.sqs`
===================================================

.. py:module:: airflow.providers.amazon.aws.triggers.sqs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.triggers.sqs.SqsSensorTrigger




.. py:class:: SqsSensorTrigger(sqs_queue, aws_conn_id = 'aws_default', max_messages = 5, num_batches = 1, wait_time_seconds = 1, visibility_timeout = None, message_filtering = None, message_filtering_match_values = None, message_filtering_config = None, delete_message_on_reception = True, waiter_delay = 60)


   Bases: :py:obj:`airflow.triggers.base.BaseTrigger`

   Asynchronously get messages from an Amazon SQS queue and then delete the messages from the queue.

   :param sqs_queue: The SQS queue url
   :param aws_conn_id: AWS connection id
   :param max_messages: The maximum number of messages to retrieve for each poke (templated)
   :param num_batches: The number of times the sensor will call the SQS API to receive messages (default: 1)
   :param wait_time_seconds: The time in seconds to wait for receiving messages (default: 1 second)
   :param visibility_timeout: Visibility timeout, a period of time during which
       Amazon SQS prevents other consumers from receiving and processing the message.
   :param message_filtering: Specified how received messages should be filtered. Supported options are:
       `None` (no filtering, default), `'literal'` (message Body literal match) or `'jsonpath'`
       (message Body filtered using a JSONPath expression).
       You may add further methods by overriding the relevant class methods.
   :param message_filtering_match_values: Optional value/s for the message filter to match on.
       For example, with literal matching, if a message body matches any of the specified values
       then it is included. For JSONPath matching, the result of the JSONPath expression is used
       and may match any of the specified values.
   :param message_filtering_config: Additional configuration to pass to the message filter.
       For example with JSONPath filtering you can pass a JSONPath expression string here,
       such as `'foo[*].baz'`. Messages with a Body which does not match are ignored.
   :param delete_message_on_reception: Default to `True`, the messages are deleted from the queue
       as soon as being consumed. Otherwise, the messages remain in the queue after consumption and
       should be deleted manually.
   :param waiter_delay: The time in seconds to wait between calls to the SQS API to receive messages.

   .. py:property:: hook
      :type: airflow.providers.amazon.aws.hooks.sqs.SqsHook


   .. py:method:: serialize()

      Return the information needed to reconstruct this Trigger.

      :return: Tuple of (class path, keyword arguments needed to re-instantiate).


   .. py:method:: poll_sqs(client)
      :async:

      Asynchronously poll SQS queue to retrieve messages.

      :param client: SQS connection
      :return: A list of messages retrieved from SQS


   .. py:method:: poke(client)
      :async:


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
