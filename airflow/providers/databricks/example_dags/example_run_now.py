#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
This is an example DAG which uses the DatabricksRunNowOperator.

Use this operator if you have already created a job in Databricks
and wish to trigger a run of that job.

See https://docs.databricks.com/dev-tools/api/2.0/jobs.html#create or
https://docs.databricks.com/jobs.html#create-a-job for details on how to
create a job.

In this example, we create two tasks which execute sequentially.
The first task is to run a notebook with additional notebook parameters,
and the second task is to run a JAR with additional parameters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/dev-tools/api/2.0/jobs.html#jobsrunstate
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id='example_databricks_run_now_operator',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_json]
    notebook_task_json = {
        'job_id': 1,
        'notebook_params': {
            'name': 'john doe',
            'age': '35',
        },
    }
    # Example of using the JSON parameter to initialize the operator.
    notebook_task = DatabricksRunNowOperator(task_id='notebook_task', json=notebook_task_json)
    # [END howto_operator_databricks_json]

    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksRunNowOperator
    # to initialize the operator.
    jar_task = DatabricksRunNowOperator(
        task_id='spark_jar_task',
        job_id=2,
        jar_params=["john doe", "35"],
    )
    # [END howto_operator_databricks_named]
    notebook_task >> jar_task
