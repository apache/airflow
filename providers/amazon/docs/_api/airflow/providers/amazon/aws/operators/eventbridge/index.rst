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

:py:mod:`airflow.providers.amazon.aws.operators.eventbridge`
============================================================

.. py:module:: airflow.providers.amazon.aws.operators.eventbridge


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutEventsOperator
   airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutRuleOperator
   airflow.providers.amazon.aws.operators.eventbridge.EventBridgeEnableRuleOperator
   airflow.providers.amazon.aws.operators.eventbridge.EventBridgeDisableRuleOperator




.. py:class:: EventBridgePutEventsOperator(*, entries, endpoint_id = None, aws_conn_id = 'aws_default', region_name = None, **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Put Events onto Amazon EventBridge.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EventBridgePutEventsOperator`

   :param entries: the list of events to be put onto EventBridge, each event is a dict (required)
   :param endpoint_id: the URL subdomain of the endpoint
   :param aws_conn_id: the AWS connection to use
   :param region_name: the region where events are to be sent


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('entries', 'endpoint_id', 'aws_conn_id', 'region_name')



   .. py:method:: hook()

      Create and return an EventBridgeHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EventBridgePutRuleOperator(*, name, description = None, event_bus_name = None, event_pattern = None, role_arn = None, schedule_expression = None, state = None, tags = None, region_name = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Create or update a specified EventBridge rule.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EventBridgePutRuleOperator`

   :param name: name of the rule to create or update (required)
   :param description: description of the rule
   :param event_bus_name: name or ARN of the event bus to associate with this rule
   :param event_pattern: pattern of events to be matched to this rule
   :param role_arn: the Amazon Resource Name of the IAM role associated with the rule
   :param schedule_expression: the scheduling expression (for example, a cron or rate expression)
   :param state: indicates whether rule is set to be "ENABLED" or "DISABLED"
   :param tags: list of key-value pairs to associate with the rule
   :param region: the region where rule is to be created or updated


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('aws_conn_id', 'name', 'description', 'event_bus_name', 'event_pattern', 'role_arn',...



   .. py:method:: hook()

      Create and return an EventBridgeHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EventBridgeEnableRuleOperator(*, name, event_bus_name = None, region_name = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Enable an EventBridge Rule.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EventBridgeEnableRuleOperator`

   :param name: the name of the rule to enable
   :param event_bus_name: the name or ARN of the event bus associated with the rule (default if omitted)
   :param aws_conn_id: the AWS connection to use
   :param region_name: the region of the rule to be enabled


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('name', 'event_bus_name', 'region_name', 'aws_conn_id')



   .. py:method:: hook()

      Create and return an EventBridgeHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.



.. py:class:: EventBridgeDisableRuleOperator(*, name, event_bus_name = None, region_name = None, aws_conn_id = 'aws_default', **kwargs)


   Bases: :py:obj:`airflow.models.BaseOperator`

   Disable an EventBridge Rule.

   .. seealso::
       For more information on how to use this operator, take a look at the guide:
       :ref:`howto/operator:EventBridgeDisableRuleOperator`

   :param name: the name of the rule to disable
   :param event_bus_name: the name or ARN of the event bus associated with the rule (default if omitted)
   :param aws_conn_id: the AWS connection to use
   :param region_name: the region of the rule to be disabled


   .. py:attribute:: template_fields
      :type: Sequence[str]
      :value: ('name', 'event_bus_name', 'region_name', 'aws_conn_id')



   .. py:method:: hook()

      Create and return an EventBridgeHook.


   .. py:method:: execute(context)

      Derive when creating an operator.

      Context is the same dictionary used as when rendering jinja templates.

      Refer to get_template_context for more context.
