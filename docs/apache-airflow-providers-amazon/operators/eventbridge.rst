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

==================
Amazon EventBridge
==================

`Amazon Eventbridge <https://docs.aws.amazon.com/eventbridge/>`__ is a serverless event bus service that makes it easy
to connect your applications with data from a variety of sources. EventBridge delivers a stream of real-time data from
your own applications, software-as-a-service (SaaS) applications, and AWS services and routes that data to targets such
as AWS Lambda. You can set up routing rules to determine where to send your data to build application architectures
that react in real time to all of your data sources. EventBridge enables you to build event-driven architectures
that are loosely coupled and distributed.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

.. _howto/operator:EventBridgePutEventsOperator:


Send events to Amazon EventBridge
=================================

To send custom events to Amazon EventBridge, use
:class:`~airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutEventsOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_eventbridge.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eventbridge_put_events]
    :end-before: [END howto_operator_eventbridge_put_events]

.. _howto/operator:EventBridgePutRuleOperator:


Create or update a rule on Amazon EventBridge
=============================================

To create or update a rule on EventBridge, use
:class:`~airflow.providers.amazon.aws.operators.eventbridge.EventBridgePutRuleOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_eventbridge.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eventbridge_put_rule]
    :end-before: [END howto_operator_eventbridge_put_rule]


.. _howto/operator:EventBridgeEnableRuleOperator:

Enable a rule on Amazon EventBridge
===================================

To enable an existing rule on EventBridge, use
:class:`~airflow.providers.amazon.aws.operators.eventbridge.EventBridgeEnableRuleOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_eventbridge.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eventbridge_enable_rule]
    :end-before: [END howto_operator_eventbridge_enable_rule]


.. _howto/operator:EventBridgeDisableRuleOperator:

Disable a rule on Amazon EventBridge
====================================

To disable an existing rule on EventBridge, use
:class:`~airflow.providers.amazon.aws.operators.eventbridge.EventBridgeDisableRuleOperator`.

.. exampleinclude:: /../../providers/tests/system/amazon/aws/example_eventbridge.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_eventbridge_disable_rule]
    :end-before: [END howto_operator_eventbridge_disable_rule]




Reference
---------

* `AWS boto3 library documentation for EventBridge <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/events.html>`__
