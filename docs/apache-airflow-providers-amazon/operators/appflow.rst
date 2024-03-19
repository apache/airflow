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

==============
Amazon AppFlow
==============

`Amazon AppFlow <https://aws.amazon.com/appflow/>`__ is a fully managed integration service
that enables you to securely transfer data between Software-as-a-Service (SaaS) applications
like Salesforce, SAP, Zendesk, Slack, and ServiceNow, and AWS services like Amazon S3 and
Amazon Redshift, in just a few clicks. With AppFlow, you can run data flows at enterprise
scale at the frequency you choose - on a schedule, in response to a business event, or on
demand. You can configure data transformation capabilities like filtering and validation to
generate rich, ready-to-use data as part of the flow itself, without additional steps.
AppFlow automatically encrypts data in motion, and allows users to restrict data from
flowing over the public Internet for SaaS applications that are integrated with
AWS PrivateLink, reducing exposure to security threats.

Prerequisite Tasks
------------------

.. include:: ../_partials/prerequisite_tasks.rst

Generic Parameters
------------------

.. include:: ../_partials/generic_parameters.rst

Operators
---------

Run Flow
========

To run an AppFlow flow keeping as is, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRunOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow_run.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_run]
    :end-before: [END howto_operator_appflow_run]

.. note::
  Supported sources: Salesforce, Zendesk

.. _howto/operator:AppflowRunOperator:

Run Flow Full
=============

To run an AppFlow flow removing all filters, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRunFullOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_run_full]
    :end-before: [END howto_operator_appflow_run_full]

.. note::
  Supported sources: Salesforce, Zendesk

.. _howto/operator:AppflowRunFullOperator:

Run Flow Daily
==============

To run an AppFlow flow filtering daily records, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRunDailyOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_run_daily]
    :end-before: [END howto_operator_appflow_run_daily]

.. note::
  Supported sources: Salesforce

.. _howto/operator:AppflowRunDailyOperator:

Run Flow Before
===============

To run an AppFlow flow filtering future records and selecting the past ones, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRunBeforeOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_run_before]
    :end-before: [END howto_operator_appflow_run_before]

.. note::
  Supported sources: Salesforce

.. _howto/operator:AppflowRunBeforeOperator:

Run Flow After
==============

To run an AppFlow flow filtering past records and selecting the future ones, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRunAfterOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_run_after]
    :end-before: [END howto_operator_appflow_run_after]

.. note::
  Supported sources: Salesforce, Zendesk

.. _howto/operator:AppflowRunAfterOperator:

Skipping Tasks For Empty Runs
=============================

To skip tasks when some AppFlow run return zero records, use:
:class:`~airflow.providers.amazon.aws.operators.appflow.AppflowRecordsShortCircuitOperator`.

.. exampleinclude:: /../../tests/system/providers/amazon/aws/example_appflow.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_appflow_shortcircuit]
    :end-before: [END howto_operator_appflow_shortcircuit]

.. note::
  Supported sources: Salesforce, Zendesk

.. _howto/operator:AppflowRecordsShortCircuitOperator:

Reference
---------

* `AWS boto3 library documentation for Amazon AppFlow <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/appflow.html>`__
