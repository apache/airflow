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

:py:mod:`airflow.providers.amazon.aws.utils.sqs`
================================================

.. py:module:: airflow.providers.amazon.aws.utils.sqs


Module Contents
---------------


Functions
~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.sqs.process_response
   airflow.providers.amazon.aws.utils.sqs.filter_messages
   airflow.providers.amazon.aws.utils.sqs.filter_messages_literal
   airflow.providers.amazon.aws.utils.sqs.filter_messages_jsonpath



Attributes
~~~~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.utils.sqs.log


.. py:data:: log



.. py:function:: process_response(response, message_filtering = None, message_filtering_match_values = None, message_filtering_config = None)

   Process the response from SQS.

   :param response: The response from SQS
   :return: The processed response


.. py:function:: filter_messages(messages, message_filtering, message_filtering_match_values, message_filtering_config)


.. py:function:: filter_messages_literal(messages, message_filtering_match_values)


.. py:function:: filter_messages_jsonpath(messages, message_filtering_match_values, message_filtering_config)
