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

:py:mod:`airflow.providers.amazon.aws.operators.sqs`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.sqs

.. autoapi-nested-parse::

   Publish message to SQS queue.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.sqs.SqsPublishOperator




.. py:class:: SqsPublishOperator(*, sqs_queue, message_content, message_attributes = None, delay_seconds = 0, message_group_id = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Publish a message to an Amazon SQS queue.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SqsPublishOperator`

   :param sqs_queue: The SQS queue url (templated)
   :param message_content: The message content (templated)
   :param message_attributes: additional attributes for the message (default: None)
       For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
   :param delay_seconds: message delay (templated) (default: 1 second)
   :param message_group_id: This parameter applies only to FIFO (first-in-first-out) queues. (default: None)
       For details of the attributes parameter see :py:meth:`botocore.client.SQS.send_message`
   :param aws_conn_id: AWS connection id (default: aws_default)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('sqs_queue', 'message_content', 'delay_seconds', 'message_attributes', 'message_group_id')



   .. py:attribute:: template_fields_renderers



   .. py:attribute:: ui_color
      :value: '#6ad3fa'



   .. py:method:: execute(context)

      Publish the message to the Amazon SQS queue.

      :param context: the context object
      :return: dict with information about the message sent
          For details of the returned dict see :py:meth:`botocore.client.SQS.send_message`
