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

:py:mod:`airflow.providers.amazon.aws.hooks.sns`
================================================

.. py:module:: airflow.providers.amazon.aws.hooks.sns

.. autoapi-nested-parse::

   This module contains AWS SNS hook.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.sns.SnsHook




.. py:class:: SnsHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Interact with Amazon Simple Notification Service.

   Provide thin wrapper around :external+boto3:py:class:`boto3.client("sns") <SNS.Client>`.

   Additional arguments (such as ``aws_conn_id``) may be specified and
   are passed down to the underlying AwsBaseHook.

   .. seealso::
       - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   .. py:method:: publish_to_target(target_arn, message, subject = None, message_attributes = None)

      Publish a message to a SNS topic or an endpoint.

      .. seealso::
          - :external+boto3:py:meth:`SNS.Client.publish`

      :param target_arn: either a TopicArn or an EndpointArn
      :param message: the default message you want to send
      :param subject: subject of message
      :param message_attributes: additional attributes to publish for message filtering. This should be
          a flat dict; the DataType to be sent depends on the type of the value:

          - bytes = Binary
          - str = String
          - int, float = Number
          - iterable = String.Array
