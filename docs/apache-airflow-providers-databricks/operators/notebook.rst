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

.. _howto/operator:DatabricksNotebookOperator:


DatabricksNotebookOperator
==========================

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksNotebookOperator` to launch and monitor
notebook job runs on Databricks as Airflow tasks.



Examples
--------

Running a notebook in Databricks on a new cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_notebook_new_cluster]
    :end-before: [END howto_operator_databricks_notebook_new_cluster]

Running a notebook in Databricks on an existing cluster
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_notebook_existing_cluster]
    :end-before: [END howto_operator_databricks_notebook_existing_cluster]
