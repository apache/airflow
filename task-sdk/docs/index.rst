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

Apache Airflow Task SDK
=================================

:any:`DAG` is where to start. :any:`dag`

The Apache Airflow Task SDK provides python-native interfaces for defining DAGs,
executing tasks in isolated subprocesses and interacting with Airflow resources
(e.g., Connections, Variables, XComs, Metrics, Logs, and OpenLineage events) at runtime.
It also includes core execution-time components to manage communication between the worker
and the Airflow scheduler/backend.

This approach reduces boilerplate and keeps your DAG definitions concise and readable.


Installation
------------
To install the Task SDK, run:

.. code-block:: bash

   pip install apache-airflow-task-sdk

Getting Started
---------------
Define a basic DAG and task in just a few lines of Python:

.. exampleinclude:: ../../airflow-core/src/airflow/example_dags/example_simplest_dag.py
   :language: python
   :start-after: [START simplest_dag]
   :end-before: [END simplest_dag]
   :caption: Simplest DAG with :func:`@dag <airflow.sdk.dag>`  and :func:`@task <airflow.sdk.task>`

Examples
--------

For more examples DAGs and patterns, see the :doc:`examples` page.

Key Concepts
------------
Defining DAGs
~~~~~~~~~~~~~
Use ``@dag`` to convert a function into an Airflow DAG. All nested ``@task`` calls
become part of the workflow.

Decorators
~~~~~~~~~~
Simplify task definitions using decorators:

- :func:`@task <airflow.sdk.task>` : define tasks.
- :func:`@task_group <airflow.sdk.task_group>`: group related tasks into logical units.
- :func:`@setup <airflow.sdk.setup>` and :func:`@teardown <airflow.sdk.teardown>`: define setup and teardown tasks for DAGs and TaskGroups.

Tasks and Operators
~~~~~~~~~~~~~~~~~~~
Wrap Python callables with :func:`@task <airflow.sdk.task>` to create tasks, leverage dynamic task mapping with
``.expand()``, and pass data via ``XComArg``. You can also create traditional Operators
(e.g., sensors) via classes imported from the SDK:

  - **BaseOperator**, **Sensor**, **OperatorLink**, **Notifier**, **XComArg**, etc.
    (see the **api reference** section for details)

Assets
~~~~~~
Model data as assets and emit them to downstream tasks with the SDK's asset library under
``airflow.sdk.definitions.asset``. You can use:

- :func:`@asset <airflow.sdk.asset>`, :class:`~airflow.sdk.AssetAlias`, etc. (see the **api reference** section below)

Refer to :doc:`api` for the complete reference of all decorators and classes.

.. toctree::
  :hidden:

  examples
  api
