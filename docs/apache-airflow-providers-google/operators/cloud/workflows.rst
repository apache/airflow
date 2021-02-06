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

Google Cloud Workflows Operators
================================

You can use Workflows to create serverless workflows that link series of serverless tasks together
in an order you define. Combine the power of Google Cloud's APIs, serverless products like Cloud
Functions and Cloud Run, and calls to external APIs to create flexible serverless applications.

For more information about the service visit
`Workflows production documentation <Product documentation <https://cloud.google.com/workflows/docs/overview>`__.

.. contents::
  :depth: 1
  :local:

Prerequisite Tasks
------------------

.. include::/operators/_partials/prerequisite_tasks.rst


.. _howto/operator:WorkflowsCreateWorkflowOperator:

Create workflow
===============

To create a workflow use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsCreateWorkflowOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_create_workflow]
      :end-before: [END how_to_create_workflow]

The workflow should be define in similar why to this example:

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 0
      :start-after: [START how_to_define_workflow]
      :end-before: [END how_to_define_workflow]

For more information about authoring workflows check official
production documentation `<Product documentation <https://cloud.google.com/workflows/docs/overview>`__.


.. _howto/operator:WorkflowsUpdateWorkflowOperator:

Update workflow
===============

To update a workflow use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsUpdateWorkflowOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_update_workflow]
      :end-before: [END how_to_update_workflow]

.. _howto/operator:WorkflowsGetWorkflowOperator:

Get workflow
============

To get a workflow use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsGetWorkflowOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_get_workflow]
      :end-before: [END how_to_get_workflow]

.. _howto/operator:WorkflowsListWorkflowsOperator:

List workflows
==============

To list workflows use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsListWorkflowsOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_list_workflows]
      :end-before: [END how_to_list_workflows]

.. _howto/operator:WorkflowsDeleteWorkflowOperator:

Delete workflow
===============

To delete a workflow use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsDeleteWorkflowOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_delete_workflow]
      :end-before: [END how_to_delete_workflow]

.. _howto/operator:WorkflowsCreateExecutionOperator:

Create execution
================

To create an execution use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsCreateExecutionOperator`.
This operator is not idempotent due to API limitation.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_create_execution]
      :end-before: [END how_to_create_execution]

The create operator does not wait for execution to complete. To wait for execution result use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowExecutionSensor`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_wait_for_execution]
      :end-before: [END how_to_wait_for_execution]

.. _howto/operator:WorkflowsGetExecutionOperator:

Get execution
================

To get an execution use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsGetExecutionOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_get_execution]
      :end-before: [END how_to_get_execution]

.. _howto/operator:WorkflowsListExecutionsOperator:

List executions
===============

To list executions use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsListExecutionsOperator`.
By default this operator will return only executions for last 60 minutes.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_list_executions]
      :end-before: [END how_to_list_executions]

.. _howto/operator:WorkflowsCancelExecutionOperator:

Cancel execution
================

To cancel an execution use
:class:`~airflow.providers.google.cloud.operators.dataproc.WorkflowsCancelExecutionOperator`.

.. exampleinclude:: /../../airflow/providers/google/cloud/example_dags/example_workflows.py
      :language: python
      :dedent: 4
      :start-after: [START how_to_cancel_execution]
      :end-before: [END how_to_cancel_execution]
