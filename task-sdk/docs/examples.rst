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

Basic Examples
--------------

Define a basic DAG and task in just a few lines of Python:

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_simplest_dag.py
   :language: python
   :start-after: [START simplest_dag]
   :end-before: [END simplest_dag]
   :caption: Simplest DAG with :func:`@dag <airflow.sdk.dag>` and :func:`@task <airflow.sdk.task>`

Key Concepts
------------
Defining DAGs
~~~~~~~~~~~~~

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_dag_decorator.py
   :language: python
   :start-after: [START dag_decorator_usage]
   :end-before: [END dag_decorator_usage]
   :caption: Using the :func:`@dag <airflow.sdk.dag>` decorator with custom tasks and operators.

Decorators
~~~~~~~~~~

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
