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



DatabricksReposUpdateOperator
=============================

Use the :class:`~airflow.providers.databricks.operators.DatabricksReposUpdateOperator` to update code in an existing
`Databricks Repos <https://docs.databricks.com/repos/index.html>`_ to a given Git branch or tag
via `api/2.0/repos/ <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/update-repo>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

Usually this operator is used to update a source code of the Databricks job before its execution.
To use this operator you need to provide either ``branch`` or ``tag`` and either ``repo_path`` or ``repo_id``.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - branch: str
     - Name of the existing Git branch to update to (required if ``tag`` isn't provided).
   * - tag: str
     - Name of the existing Git tag to update to (required if ``branch`` isn't provided).
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

Updating Databricks Repo by specifying path
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksReposUpdateOperator is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_repos.py
    :language: python
    :start-after: [START howto_operator_databricks_repo_update]
    :end-before: [END howto_operator_databricks_repo_update]
