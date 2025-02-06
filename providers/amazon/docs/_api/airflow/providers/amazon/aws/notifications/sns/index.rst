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

:py:mod:`airflow.providers.amazon.aws.notifications.sns`
========================================================

.. py:module:: airflow.providers.amazon.aws.notifications.sns


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.sns.SnsNotifier




Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.notifications.sns.send_sns_notification


.. py:class:: SnsNotifier(*, aws_conn_id = SnsHook.default_conn_name, target_arn, message, subject = None, message_attributes = None, region_name = None)


   Bases: :py:obj:`airflow.notifications.basenotifier.BaseNotifier`

   Amazon SNS (Simple Notification Service) Notifier.

   .. seealso::
       For more information on how to use this notifier, take a look at the guide:
       :ref:`howto/notifier:SnsNotifier`

   :param aws_conn_id: The :ref:`Amazon Web Services Connection id <howto/connection:aws>`
       used for AWS credentials. If this is None or empty then the default boto3 behaviour is used.
   :param target_arn: Either a TopicArn or an EndpointArn.
   :param message: The message you want to send.
   :param subject: The message subject you want to send.
   :param message_attributes: The message attributes you want to send as a flat dict (data type will be
       determined automatically).
   :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('target_arn', 'message', 'subject', 'message_attributes', 'aws_conn_id', 'region_name')



   .. py:method:: hook()

      Amazon SNS Hook (cached).


   .. py:method:: notify(context)

      Publish the notification message to Amazon SNS.



.. py:data:: send_sns_notification
