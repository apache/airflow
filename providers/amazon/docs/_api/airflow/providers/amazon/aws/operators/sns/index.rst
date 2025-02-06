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

:py:mod:`airflow.providers.amazon.aws.operators.sns`
====================================================

.. py:module:: airflow.providers.amazon.aws.operators.sns

.. autoapi-nested-parse::

   Publish message to SNS queue.



Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.sns.SnsPublishOperator




.. py:class:: SnsPublishOperator(*, target_arn, message, subject = None, message_attributes = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Publish a message to Amazon SNS.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:SnsPublishOperator`

   :param aws_conn_id: aws connection to use
   :param target_arn: either a TopicArn or an EndpointArn
   :param message: the default message you want to send (templated)
   :param subject: the message subject you want to send (templated)
   :param message_attributes: the message attributes you want to send as a flat dict (data type will be
       determined automatically)

   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('target_arn', 'message', 'subject', 'message_attributes', 'aws_conn_id')



   .. py:attribute:: template_ext
      :type: Sequence[str]
      :value: ()



   .. py:attribute:: template_fields_renderers



   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
