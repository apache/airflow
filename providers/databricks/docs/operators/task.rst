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

.. _howto/operator:DatabricksTaskOperator:


DatabricksTaskOperator
======================

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksTaskOperator` to launch and monitor
task runs on Databricks as Airflow tasks. This can be used as a standalone operator in a Dag and as well as part of a
Databricks Workflow by using it as an operator(task) within the
:class:`~airflow.providers.databricks.operators.databricks_workflow.DatabricksWorkflowTaskGroup`.



Examples
--------

Running a notebook in Databricks using DatabricksTaskOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_task_notebook]
    :end-before: [END howto_operator_databricks_task_notebook]

Running a SQL query in Databricks using DatabricksTaskOperator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../databricks/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_task_sql]
    :end-before: [END howto_operator_databricks_task_sql]
