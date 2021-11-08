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



.. _howto/operator:DatabricksSubmitRunOperator:


DatabricksSubmitRunOperator
===========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksSubmitRunOperator` to submit
a new Databricks job via Databricks `api/2.0/jobs/runs/submit <https://docs.databricks.com/api/latest/jobs.html#runs-submit>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.0/jobs/runs/submit`` endpoint and pass it directly to our ``DatabricksSubmitRunOperator`` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the ``DatabricksSubmitRunOperator`` directly. Note that there is exactly
one named parameter for each top level parameter in the ``runs/submit`` endpoint.

.. list-table:: Databricks Airflow Connection Metadata
   :widths: 25 25
   :header-rows: 1

   * - Parameter
     - Input
   * - spark_jar_task: dict
     - `main class and parameters for the JAR task <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkjartask>`_
   * - notebook_task: dict
     - `notebook path and parameters for the task <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsnotebooktask>`_
   * - spark_python_task: dict
     - `python file path and parameters to run the python file with <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparkpythontask>`_
   * - spark_submit_task: dict
     - `parameters needed to run a spark-submit command <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobssparksubmittask>`_
   * - pipeline_task: dict
     - `parameters needed to run a Delta Live Tables pipeline <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobspipelinetask>`_
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

An example usage of the DatabricksSubmitRunOperator is as follows:

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_json]
    :end-before: [END howto_operator_databricks_json]

You can also use named parameters to initialize the operator and run the job.

.. exampleinclude:: /../../airflow/providers/databricks/example_dags/example_databricks.py
    :language: python
    :start-after: [START howto_operator_databricks_named]
    :end-before: [END howto_operator_databricks_named]


DatabricksRunNowOperator
===========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` to trigger run of existing Databricks job
via `api/2.0/jobs/runs/run-now <https://docs.databricks.com/dev-tools/api/2.0/jobs.html#run-now>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.0/jobs/run-now`` endpoint and pass it directly to our ``DatabricksRunNowOperator`` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the ``DatabricksRunNowOperator`` directly.
Note that there is exactly one named parameter for each top level parameter in the ``jobs/run-now`` endpoint.

.. list-table:: Databricks Airflow Connection Metadata
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - job_id: str
     - ID of the existing Databricks jobs (required)
   * - jar_params: list[str]
     - A list of parameters for jobs with JAR tasks, e.g. ``"jar_params": ["john doe", "35"]``. The parameters will be passed to JAR file as command line parameters. If specified upon run-now, it would        overwrite the parameters specified in job setting. The json representation of this field (i.e. ``{"jar_params":["john doe","35"]}``) cannot exceed 10,000 bytes. This field will be templated.
   * - notebook_params: dict[str,str]
     - A dict from keys to values for jobs with notebook task, e.g.``"notebook_params": {"name": "john doe", "age":  "35"}```. The map is passed to the notebook and will be accessible through the ``dbutils.widgets.get function``. See `Widgets <https://docs.databricks.com/notebooks/widgets.html>`_ for more information. If not specified upon run-now, the triggered run will use the job’s base parameters. ``notebook_params`` cannot be specified in conjunction with ``jar_params``. The json representation of this field (i.e. ``{"notebook_params":{"name":"john doe","age":"35"}}``) cannot exceed 10,000 bytes. This field will be templated.
   * - python_params: list[str]
     - A list of parameters for jobs with python tasks, e.g. ``"python_params": ["john doe", "35"]``. The parameters will be passed to python file as command line parameters. If specified upon run-now, it would overwrite the parameters specified in job setting. The json representation of this field (i.e. ``{"python_params":["john doe","35"]}``) cannot exceed 10,000 bytes. This field will be templated.
   * - spark_submit_params: list[str]
     - A list of parameters for jobs with spark submit task,  e.g. ``"spark_submit_params": ["--class", "org.apache.spark.examples.SparkPi"]``. The parameters will be passed to spark-submit script as command line parameters. If specified upon run-now, it would overwrite the parameters specified in job setting. The json representation of this field cannot exceed 10,000 bytes. This field will be templated.
   * - timeout_seconds: int
     - The timeout for this run. By default a value of 0 is used  which means to have no timeout. This field will be templated.
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
