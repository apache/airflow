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


Databricks Operators
====================

.. _howto/operator:DatabricksSubmitRunOperator:

DatabricksSubmitRunOperator
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` to submit
an existing Spark job run to Databricks `runs/submit <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runs-submit>`__ endpoint.

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the `runs/submit <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runs-submit>`__ endpoint and pass it directly
to the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator`
directly. Note that there is exactly one named parameter for each top level parameter in the `runs/submit <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#runs-submit>`__ endpoint.

.. list-table::  DatabricksSubmitRunOperator named parameters
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - spark_jar_task: dict
     - main class and parameters for the JAR task
   * - notebook_task: dict
     - notebook path and parameters for the task
   * - spark_python_task: dict
     - python file path and parameters to run the python file with
   * - spark_submit_task: dict
     - parameters needed to run a spark-submit command
   * - pipeline_task: dict
     - pipeline_id (full name of the Delta Live Tables pipeline) to run the pipeline
   * - new_cluster: dict
     - specs for a new cluster on which this task will be run
   * - existing_cluster_id: string
     - ID for existing cluster on which to run this task
   * - libraries: list of dict
     - libraries which this run will use
   * - run_name: string
     - run name used for this task
   * - timeout_seconds: integer
     - The timeout for this run
   * - databricks_conn_id: string
     - the name of the Airflow connection to use
   * - polling_period_seconds: integer
     - controls the rate which we poll for the result of this run
   * - databricks_retry_limit: integer
     - amount of times retry if the Databricks backend is unreachable
   * - databricks_retry_delay: decimal
     - number of seconds to wait between retries
   * - do_xcom_push: boolean
     - whether we should push run_id and run_page_url to xcom

Example Usage
"""""""""""""

An example usage of the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_submit_run.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

You can also use named parameters to initialize the :class:`~airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator` and run the job.

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_submit_run.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]

.. _howto/operator:DatabricksRunNowOperator:

DatabricksRunNowOperator
^^^^^^^^^^^^^^^^^^^^^^^^

Use the :class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` to submit
an existing Spark job run to Databricks `jobs/run-now <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now>`__  endpoint.

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the `jobs/run-now <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now>`__ endpoint and pass it directly
to the :class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the :class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator`
directly. Note that there is exactly one named parameter for each top level parameter in the `jobs/run-now <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now>`__ endpoint.

.. list-table:: DatabricksRunNowOperator named parameters
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - job_id: str
     - the job_id of the existing Databricks job
   * - jar_params: list[str]
     - additional list of parameters for jobs with JAR tasks
   * - notebook_params: dict
     - a dict from keys to values for jobs with notebook task
   * - python_params: list[str]
     - additional list of parameters for jobs with python tasks
   * - spark_submit_params: list[str]
     - a list of parameters for jobs with spark submit task

Example Usage
"""""""""""""

An example usage of the :class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_run_now.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

You can also use named parameters to initialize the :class:`~airflow.providers.databricks.operators.databricks.DatabricksRunNowOperator` and run the job.

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_run_now.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]
