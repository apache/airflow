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



DatabricksReposCreateOperator
=============================

Use the :class:`~airflow.providers.databricks.operators.DatabricksReposCreateOperator` to create (and optionally checkout) a
`Databricks Repos <https://docs.databricks.com/repos/index.html>`_
via `api/2.0/repos <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/create-repo>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

To use this operator you need to provide at least ``git_url`` parameter.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - git_url: str
     - Required HTTPS URL of a Git repository
   * - git_provider: str
     - Optional name of Git provider. Must be provided if we can't guess its name from URL. See API documentation for actual list of supported Git providers.
   * - branch: str
     - Optional name of the existing Git branch to checkout.
   * - tag: str
     - Optional name of the existing Git tag to checkout.
   * - repo_path: str
     - Optional path to a Databricks Repos, like, ``/Repos/<user_email>/repo_name``. If not specified, it will be created in the user's directory.
   * - ignore_existing_repo: bool
     - Don't throw exception if repository with given path already exists.
   * - databricks_conn_id: string
     - the name of the Airflow connection to use.
   * - databricks_retry_limit: integer
     - amount of times retry if the Databricks backend is unreachable.
   * - databricks_retry_delay: decimal
     - number of seconds to wait between retries.

Examples
--------

Create a Databricks Repo
^^^^^^^^^^^^^^^^^^^^^^^^

An example usage of the DatabricksReposCreateOperator is as follows:

.. exampleinclude:: /../../providers/tests/system/databricks/example_databricks_repos.py
    :language: python
    :start-after: [START howto_operator_databricks_repo_create]
    :end-before: [END howto_operator_databricks_repo_create]
