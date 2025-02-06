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

:py:mod:`airflow.providers.amazon.aws.hooks.eventbridge`
========================================================

.. py:module:: airflow.providers.amazon.aws.hooks.eventbridge


Module Contents
---------------

Classes
~~~~~~~

.. autoapisummary::

   airflow.providers.amazon.aws.hooks.eventbridge.EventBridgeHook




.. py:class:: EventBridgeHook(*args, **kwargs)


   Bases: :py:obj:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

   Amazon EventBridge Hook.

   .. py:method:: put_rule(name, description = None, event_bus_name = None, event_pattern = None, role_arn = None, schedule_expression = None, state = None, tags = None, **kwargs)

      Create or update an EventBridge rule.

      :param name: name of the rule to create or update (required)
      :param description: description of the rule
      :param event_bus_name: name or ARN of the event bus to associate with this rule
      :param event_pattern: pattern of events to be matched to this rule
      :param role_arn: the Amazon Resource Name of the IAM role associated with the rule
      :param schedule_expression: the scheduling expression (for example, a cron or rate expression)
      :param state: indicates whether rule is set to be "ENABLED" or "DISABLED"
      :param tags: list of key-value pairs to associate with the rule
