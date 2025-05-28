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

Apache Airflow Task Execution SDK
=================================

:any:`DAG` is where to start. :any:`dag`

The Apache Airflow Task Execution SDK(Task SDK) provides Python-native interfaces for defining DAGs (via decorators),
executing tasks in isolated subprocesses and interacting with Airflow resources
(e.g., Connections, Variables, XComs, Metrics, Logs, and OpenLineage events) at runtime.
It also includes core execution-time components to manage communication between the worker
and the Airflow scheduler/backend.

This approach minimises boilerplate and keeps your DAG definitions concise and readable.


Installation
------------
To install the Task SDK, run:

.. code-block:: bash

   pip install apache-airflow-task-sdk

Getting Started
---------------
Define a basic DAG and task in just a few lines of Python:

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_simplest_dag.py
   :language: python
   :start-after: [START simplest_dag]
   :end-before: [END simplest_dag]
   :caption: Simplest DAG with ``@dag`` and ``@task``

Key Concepts
------------
Defining DAGs
~~~~~~~~~~~~~
Use ``@dag`` to convert a function into an Airflow DAG. All nested ``@task`` calls
become part of the workflow.

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_dag_decorator.py
   :language: python
   :start-after: [START dag_decorator_usage]
   :end-before: [END dag_decorator_usage]
   :caption: Using the ``@dag`` decorator with custom tasks and operators.

Decorators
~~~~~~~~~~
Simplify DAG and task definitions using decorators:

- ``@task``: define tasks.
- ``@task_group``: group related tasks into logical units.
- ``@setup`` and ``@teardown``: define setup and teardown tasks for DAGs and TaskGroups.

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_task_group_decorator.py
   :language: python
   :start-after: [START howto_task_group_decorator]
   :end-before: [END howto_task_group_decorator]
   :caption: Group tasks using the ``@task_group`` decorator.

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_setup_teardown_taskflow.py
   :language: python
   :caption: Define setup and teardown tasks with ``@setup`` and ``@teardown``.

Tasks and Operators
~~~~~~~~~~~~~~~~~~~
Wrap Python callables with ``@task`` to create tasks, leverage dynamic task mapping with
``.expand()``, and pass data via ``XComArg``. You can also create traditional Operators
(e.g., sensors) via classes imported from the SDK:

  - **BaseOperator**, **Sensor**, **OperatorLink**, **Notifier**, "XComArg", etc.
    (see the **api reference** section for details)

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_dynamic_task_mapping.py
   :language: python
   :start-after: [START example_dynamic_task_mapping]
   :end-before: [END example_dynamic_task_mapping]
   :caption: Dynamic task mapping with ``expand()``

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_xcomargs.py
   :language: python
   :caption: Using ``XComArg`` to chain tasks based on return values.

Assets
~~~~~~
Model data as assets and emit them to downstream tasks with the SDK's asset library under
``airflow.sdk.definitions.asset``. You can use:

  - ``@asset``, ``AssetAlias``, etc. (see the **api reference** section below)

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_assets.py
   :language: python
   :start-after: [START asset_def]
   :end-before: [END asset_def]
   :caption: Defining an ``Asset``

.. literalinclude:: ../../airflow-core/src/airflow/example_dags/example_asset_alias.py
   :language: python
   :caption: Defining asset aliases with ``AssetAlias``.

Execution Time Components
~~~~~~~~~~~~~~~~~~~~~~~~~
At runtime, tasks run in an isolated subprocess managed by the SDK:

  - **Supervisor** coordinates the worker's lifecycle.
  - **TaskRunner** actually executes the user's task code.
  - **Context** objects provide runtime metadata (e.g., connections, variables).
    (see the **Execution Time** section below for details)


Everything Else
---------------
In addition to "DAG" and "task"-level decorators, the Task SDK provides:

  * **Bases** (under ``airflow.sdk.bases``):
    - ``BaseOperator``
    - ``Sensor``
    - ``OperatorLink``
    - ``Notifier``
    - ``XComArg``

  * **Decorators**:
    - ``@task``, ``@task_group``, ``@setup``, ``@teardown`` (exported at top level).
    - These live in ``airflow.sdk.decorator`` but are re-exported in ``airflow.sdk``.

  * **Definitions** (under ``airflow.sdk.definitions``):
    - Core data models (e.g., assets, types, IO utilities, exceptions).

  * **Execution Time** (under ``airflow.sdk.execution_time``):
    - ``supervisor``
    - ``task_runner``

  * **I/O** helpers for managing runtime I/O serialization.

  * **API client** (under ``airflow.sdk.api.client``) for interacting with Airflow's REST endpoints.


Refer to `api.html`_ for the complete reference of all decorators and classes.

.. _api.html: api.html

.. toctree::
  :hidden:

  api
