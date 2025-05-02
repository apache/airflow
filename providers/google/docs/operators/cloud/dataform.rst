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

Google Dataform Operators
=========================

Dataform is a service for data analysts to develop, test, version control, and schedule complex SQL
workflows for data transformation in BigQuery.

Dataform lets you manage data transformation in the Extraction, Loading, and Transformation (ELT) process
for data integration. After raw data is extracted from source systems and loaded into BigQuery, Dataform
helps you to transform it into a well-defined, tested, and documented suite of data tables.

For more information about the task visit `Dataform documentation <https://cloud.google.com/dataform/docs>`__


Configuration
-------------

Before you can use the Dataform operators you need to initialize repository and workspace, for more information
about this visit `Dataform documentation <https://cloud.google.com/dataform/docs>`__

Create Repository
-----------------

To create a repository for tracking your code in Dataform service use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformCreateRepositoryOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_create_repository]
    :end-before: [END howto_operator_create_repository]


Create Workspace
----------------

To create a workspace for storing your code in Dataform service use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformCreateWorkspaceOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_create_workspace]
    :end-before: [END howto_operator_create_workspace]


Create Compilation Result
-------------------------

To create a Compilation Result use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformCreateCompilationResultOperator`.
A simple configuration can look as followed:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_create_compilation_result]
    :end-before: [END howto_operator_create_compilation_result]


Get Compilation Result
----------------------

To get a Compilation Result you can use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformGetCompilationResultOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_compilation_result]
    :end-before: [END howto_operator_get_compilation_result]


Create Workflow Invocation
--------------------------

To create a Workflow Invocation you can use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformCreateWorkflowInvocationOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_workflow_invocation]
    :end-before: [END howto_operator_create_workflow_invocation]


We have possibility to run this operation in the sync mode and async, for async operation we also have
a sensor
:class:`~airflow.providers.google.cloud.operators.dataform.DataformWorkflowInvocationStateSensor`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_workflow_invocation_async]
    :end-before: [END howto_operator_create_workflow_invocation_async]


We also have a sensor
:class:`~airflow.providers.google.cloud.operators.dataform.DataformWorkflowInvocationActionStateSensor`
to check the status of a particular action for a workflow invocation triggered asynchronously.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_create_workflow_invocation_action_async]
    :end-before: [END howto_operator_create_workflow_invocation_action_async]


Get Workflow Invocation
-----------------------

To get a Workflow Invocation you can use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformGetWorkflowInvocationOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_get_workflow_invocation]
    :end-before: [END howto_operator_get_workflow_invocation]


Query Workflow Invocation Action
--------------------------------

To query Workflow Invocation Actions you can use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformQueryWorkflowInvocationActionsOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_query_workflow_invocation_actions]
    :end-before: [END howto_operator_query_workflow_invocation_actions]


Cancel Workflow Invocation
--------------------------

To cancel a Workflow Invocation you can use
:class:`~airflow.providers.google.cloud.sensors.dataform.DataformCancelWorkflowInvocationOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_cancel_workflow_invocation]
    :end-before: [END howto_operator_cancel_workflow_invocation]


Delete Repository
-----------------

To delete a repository use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformDeleteRepositoryOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_delete_repository]
    :end-before: [END howto_operator_delete_repository]


Delete Workspace
----------------

To delete a workspace use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformDeleteWorkspaceOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_delete_workspace]
    :end-before: [END howto_operator_delete_workspace]


Remove file
-----------

To remove a file use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformRemoveFileOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_remove_file]
    :end-before: [END howto_operator_remove_file]


Remove directory
----------------

To remove a directory use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformRemoveDirectoryOperator`.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_operator_remove_directory]
    :end-before: [END howto_operator_remove_directory]


Initialize workspace
--------------------

Creates default projects structure for provided workspace. Before it can be done workspace and repository should be created.
Example of usage can be seen below:

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 0
    :start-after: [START howto_initialize_workspace]
    :end-before: [END howto_initialize_workspace]


Write file to workspace
-----------------------

To write a file with given content to specified workspace use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformWriteFileOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_write_file]
    :end-before: [END howto_operator_write_file]


Make directory in workspace
---------------------------

To make a directory with given path in specified workspace use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformMakeDirectoryOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_make_directory]
    :end-before: [END howto_operator_make_directory]


Install NPM packages
--------------------

To install npm packages for specified workspace use
:class:`~airflow.providers.google.cloud.operators.dataform.DataformInstallNpmPackagesOperator`.

.. exampleinclude:: /../../google/tests/system/google/cloud/dataform/example_dataform.py
    :language: python
    :dedent: 4
    :start-after: [START howto_operator_install_npm_packages]
    :end-before: [END howto_operator_install_npm_packages]
