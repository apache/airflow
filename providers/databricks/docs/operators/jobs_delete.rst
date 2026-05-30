.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.



DatabricksDeleteJobsOperator
============================

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksDeleteJobsOperator`
to delete an existing `Databricks job
<https://docs.databricks.com/api/workspace/jobs/delete>`_.

A common use case is cleaning up jobs that were created in a development
workspace after they have been deployed to a production workspace via the
Databricks Asset Bundle (DAB), so that no leftover jobs remain in the
development workspace.


Using the Operator
^^^^^^^^^^^^^^^^^^

The operator accepts either a ``job_id`` or a ``job_name``. They are mutually
exclusive — exactly one must be provided. When ``job_name`` is used, the
operator resolves the corresponding ``job_id`` via the Databricks REST API
(``find_job_id_by_name``). If no job matches the supplied name an
``AirflowException`` is raised.

Delete by ``job_id``:

.. code-block:: python

    from airflow.providers.databricks.operators.databricks import DatabricksDeleteJobsOperator

    delete_job_by_id = DatabricksDeleteJobsOperator(
        task_id="delete_job_by_id",
        job_id=12345,
    )

Delete by ``job_name`` (recommended — names are usually known up front):

.. code-block:: python

    delete_job_by_name = DatabricksDeleteJobsOperator(
        task_id="delete_job_by_name",
        job_name="my_databricks_job",
    )


Parameters
^^^^^^^^^^

The most common parameters are:

* ``job_id`` *(int | str | None)* — Databricks job ID to delete. Templated.
* ``job_name`` *(str | None)* — Name of the Databricks job to delete. Templated.
  Resolved to ``job_id`` at execution time. An ``AirflowException`` is raised
  if the job cannot be found.
* ``databricks_conn_id`` — Reference to the Databricks connection. Defaults to
  ``databricks_default``.
* ``databricks_retry_limit`` / ``databricks_retry_delay`` /
  ``databricks_retry_args`` — Retry behavior forwarded to ``tenacity.Retrying``.