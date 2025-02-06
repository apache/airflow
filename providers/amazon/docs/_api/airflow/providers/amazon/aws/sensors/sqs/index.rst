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

:py:mod:`airflow.providers.amazon.aws.sensors.sqs`
==================================================

.. py:module:: airflow.providers.amazon.aws.sensors.sqs

.. autoapi-nested-parse::

   Reads and then deletes the message from SQS queue.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.sensors.sqs.SqsSensor




.. py:class:: SqsSensor(*, sqs_queue, aws_conn_id = 'aws_default', max_messages = 5, num_batches = 1, wait_time_seconds = 1, visibility_timeout = None, message_filtering = None, message_filtering_match_values = None, message_filtering_config = None, delete_message_on_reception = True, deferrable = conf.getboolean('operators', 'default_deferrable', fallback=False), **kwargs)


   Bases: :py:obj:`airflow.sensors.base.BaseSensorOperator`

   Get messages from an Amazon SQS queue and then delete the messages from the queue.

   If deletion of messages fails, an AirflowException is thrown. Otherwise, the messages
   are pushed through XCom with the key ``messages``.

   By default,the sensor performs one and only one SQS call per poke, which limits the result to
   a maximum of 10 messages. However, the total number of SQS API calls per poke can be controlled
   by num_batches param.

   .. seealso::
       For more information on how to use this sensor, take a look at the guide:
       :ref:`howto/sensor:SqsSensor`

   :param aws_conn_id: AWS connection id
   :param sqs_queue: The SQS queue url (templated)
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
   :param deferrable: If True, the sensor will operate in deferrable more. This mode requires aiobotocore
       module to be installed.
       (default: False, but can be overridden in config file by setting default_deferrable to True)


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('sqs_queue', 'max_messages', 'message_filtering_config')



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.


   .. py:method:: execute_complete(context, event = None)


   .. py:method:: poll_sqs(sqs_conn)

      Poll SQS queue to retrieve messages.

      :param sqs_conn: SQS connection
      :return: A list of messages retrieved from SQS


   .. py:method:: poke(context)

      Check subscribed queue for messages and write them to xcom with the ``messages`` key.

      :param context: the context object
      :return: ``True`` if message is available or ``False``


   .. py:method:: get_hook()

      Create and return an SqsHook.


   .. py:method:: hook()
