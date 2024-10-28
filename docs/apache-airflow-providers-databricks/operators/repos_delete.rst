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



DatabricksReposDeleteOperator
=============================

Use the :class:`~airflow.providers.databricks.operators.DatabricksReposDeleteOperator` to delete an existing
`Databricks Repo <https://docs.databricks.com/repos/index.html>`_
via `api/2.0/repos/ <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/delete-repo>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

To use this operator you need to provide either ``repo_path`` or ``repo_id``.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - repo_path: str
     - Path to existing Databricks Repos, like, ``/Repos/<user_email>/repo_name`` (required if ``repo_id`` isn't provided).
   * - repo_id: str
     - ID of existing Databricks Repos (required if ``repo_path`` isn't provided).
   * - databricks_conn_id: string
     - the name of the Airflow connection to use.
   * - databricks_retry_limit: integer
     - amount of times retry if the Databricks backend is unreachable.
   * - databricks_retry_delay: decimal
     - number of seconds to wait between retries.

Examples
--------

Deleting Databricks Repo by specifying path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksReposDeleteOperator is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_repos.py
    :language: python
    :start-after: [START howto_operator_databricks_repo_delete]
    :end-before: [END howto_operator_databricks_repo_delete]
