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



DatabricksRunNowOperator
========================

Use the :class:`~airflow.providers.databricks.operators.DatabricksRunNowOperator` to trigger a run of an existing Databricks job
via `api/2.1/jobs/run-now <https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow>`_ API endpoint.


Using the Operator
^^^^^^^^^^^^^^^^^^

There are two ways to instantiate this operator. In the first way, you can take the JSON payload that you typically use
to call the ``api/2.1/jobs/run-now`` endpoint and pass it directly to our ``DatabricksRunNowOperator`` through the ``json`` parameter.

Another way to accomplish the same thing is to use the named parameters of the ``DatabricksRunNowOperator`` directly.
Note that there is exactly one named parameter for each top level parameter in the ``jobs/run-now`` endpoint.

.. list-table::
   :widths: 15 25
   :header-rows: 1

   * - Parameter
     - Input
   * - job_id: str
     - ID of the existing Databricks jobs (required if ``job_name`` isn't provided).
   * - job_name: str
     - Name of the existing Databricks job (required if ``job_id`` isn't provided). It will throw exception if job isn't found, of if there are multiple jobs with the same name.
   * - jar_params: list[str]
     - A list of parameters for jobs with JAR tasks, e.g. ``"jar_params": ["john doe", "35"]``. The parameters will be passed to JAR file as command line parameters. If specified upon run-now, it would overwrite the parameters specified in job setting. The json representation of this field (i.e. ``{"jar_params":["john doe","35"]}``) cannot exceed 10,000 bytes. This field will be templated.
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
   * - databricks_retry_args: dict
     - An optional dictionary with arguments passed to ``tenacity.Retrying`` class.
   * - do_xcom_push: boolean
     - whether we should push run_id and run_page_url to xcom
