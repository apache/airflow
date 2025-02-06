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

:py:mod:`airflow.providers.amazon.aws.notifications.sqs`
========================================================

.. py:module:: airflow.providers.amazon.aws.notifications.sqs


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.sqs.SqsNotifier




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.sqs.send_sqs_notification


.. py:class:: SqsNotifier(*, aws_conn_id = SqsHook.default_conn_name, queue_url, message_body, message_attributes = None, message_group_id = None, delay_seconds = 0, region_name = None)


   Bases: :py:obj:`airflow.notifications.basenotifier.BaseNotifier`

   Amazon SQS (Simple Queue Service) Notifier.

   .. seealso::
       For more information on how to use this notifier, take a look at the guide:
       :ref:`howto/notifier:SqsNotifier`

   :param aws_conn_id: The :ref:`Amazon Web Services Connection id <howto/connection:aws>`
       used for AWS credentials. If this is None or empty then the default boto3 behaviour is used.
   :param queue_url: The URL of the Amazon SQS queue to which a message is sent.
   :param message_body: The message to send.
   :param message_attributes: additional attributes for the message.
       For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`.
   :param message_group_id: This parameter applies only to FIFO (first-in-first-out) queues.
       For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`.
   :param delay_seconds: The length of time, in seconds, for which to delay a message.
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('queue_url', 'message_body', 'message_attributes', 'message_group_id', 'delay_seconds',...



   .. py:method:: hook()

      Amazon SQS Hook (cached).


   .. py:method:: notify(context)

      Publish the notification message to Amazon SQS queue.



.. py:data:: send_sqs_notification
