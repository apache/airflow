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

Examples
========

.. note:: For a minimal quick start, see the `Getting Started <../index.rst#getting-started>`_ section.

Key Concepts
------------

Defining Dags
~~~~~~~~~~~~~

Example: Defining a Dag

Use the :func:`airflow.sdk.dag` decorator to convert a Python function into an Airflow Dag. All nested calls to :func:`airflow.sdk.task` within the function will become tasks in the Dag. For full parameters and usage, see the API reference for :func:`airflow.sdk.dag`.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_dag_decorator.py
   :language: python
   :start-after: [START dag_decorator_usage]
   :end-before: [END dag_decorator_usage]
   :caption: Using the :func:`@dag <airflow.sdk.dag>` decorator with custom tasks and operators.

Decorators
~~~~~~~~~~

Example: Using Task SDK decorators

The Task SDK provides decorators to simplify Dag definitions:

- :func:`airflow.sdk.task_group` groups related tasks into logical TaskGroups.
- :func:`airflow.sdk.setup` and :func:`airflow.sdk.teardown` define setup and teardown hooks for Dags or TaskGroups.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_task_group_decorator.py
   :language: python
   :start-after: [START howto_task_group_decorator]
   :end-before: [END howto_task_group_decorator]
   :caption: Group tasks using the :func:`@task_group <airflow.sdk.task_group>` decorator.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_setup_teardown_taskflow.py
   :language: python
   :start-after: [START example_setup_teardown_taskflow]
   :end-before: [END example_setup_teardown_taskflow]
   :caption: Define setup and teardown tasks with :func:`@setup <airflow.sdk.setup>` and :func:`@teardown <airflow.sdk.teardown>`.

Tasks and Operators
~~~~~~~~~~~~~~~~~~~

Example: Defining tasks and using operators

Use the :func:`airflow.sdk.task` decorator to wrap Python callables as tasks and leverage dynamic task mapping with the ``.expand()`` method. Tasks communicate via :class:`airflow.sdk.XComArg`. For traditional operators and sensors, import classes like :class:`airflow.sdk.BaseOperator` or :class:`airflow.sdk.Sensor`.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_dynamic_task_mapping.py
   :language: python
   :start-after: [START example_dynamic_task_mapping]
   :end-before: [END example_dynamic_task_mapping]
   :caption: Dynamic task mapping with ``expand()``

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_xcomargs.py
   :language: python
   :start-after: [START example_xcomargs]
   :end-before: [END example_xcomargs]
   :caption: Using ``XComArg`` to chain tasks based on return values.

Assets
~~~~~~

Example: Defining and aliasing assets

Model data artifacts using the Task SDK's asset API. Decorate functions with :func:`airflow.sdk.asset` and create aliases with :class:`airflow.sdk.AssetAlias`. See the API reference under assets for full guidance.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_assets.py
   :language: python
   :start-after: [START asset_def]
   :end-before: [END asset_def]
   :caption: Defining an :func:`@asset <airflow.sdk.asset>`

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_asset_alias.py
   :language: python
   :start-after: [START example_asset_alias]
   :end-before: [END example_asset_alias]
   :caption: Defining asset aliases with :class:`AssetAlias <airflow.sdk.AssetAlias>`.

TaskFlow API Tutorial
---------------------

This section provides a concise, code-first view. For the full tutorial and context,
see the `core TaskFlow tutorial <../../airflow-core/docs/tutorial/taskflow.rst>`_.

Step 1: Define the Dag
----------------------

In this step, define your Dag by applying the :func:`airflow.sdk.dag` decorator to a Python function. This registers the Dag with its schedule and default arguments. For more details, see :func:`airflow.sdk.dag`.

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :start-after: [START instantiate_dag]
   :end-before: [END instantiate_dag]
   :caption: Defining the Dag with the :func:`@dag <airflow.sdk.dag>` decorator

Step 2: Write your Tasks
------------------------

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :dedent: 4
   :start-after: [START extract]
   :end-before: [END extract]
   :caption: Extract task to load data

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :dedent: 4
   :start-after: [START transform]
   :end-before: [END transform]
   :caption: Transform task to process data

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :dedent: 4
   :start-after: [START load]
   :end-before: [END load]
   :caption: Load task to output results

Step 3: Build the Flow
----------------------

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :dedent: 4
   :start-after: [START main_flow]
   :end-before: [END main_flow]
   :caption: Connecting tasks by invoking them like normal Python functions

Step 4: Invoke the Dag
----------------------

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/tutorial_taskflow_api.py
   :language: python
   :start-after: [START dag_invocation]
   :end-before: [END dag_invocation]
   :caption: Registering the Dag by calling the decorated function
