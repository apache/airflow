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

:py:mod:`airflow.providers.amazon.aws.hooks.sqs`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.sqs

.. autoapi-nested-parse::

   This module contains AWS SQS hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.sqs.SqsHook




.. py:class:: SqsHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Queue Service.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("sqs") <SQS.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: create_queue(queue_name, attributes = None)

      Create queue using connection object.

      .. seealso::
          - :external+boto3:py:meth:`SQS.Client.create_queue`

      :param queue_name: name of the queue.
      :param attributes: additional attributes for the queue (default: None)
      :return: dict with the information about the queue.


   .. py:method:: send_message(queue_url, message_body, delay_seconds = 0, message_attributes = None, message_group_id = None)

      Send message to the queue.

      .. seealso::
          - :external+boto3:py:meth:`SQS.Client.send_message`

      :param queue_url: queue url
      :param message_body: the contents of the message
      :param delay_seconds: seconds to delay the message
      :param message_attributes: additional attributes for the message (default: None)
      :param message_group_id: This applies only to FIFO (first-in-first-out) queues. (default: None)
      :return: dict with the information about the message sent
