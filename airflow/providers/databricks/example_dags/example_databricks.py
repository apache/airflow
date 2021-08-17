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
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
}

with DAG(
    dag_id='example_databricks_operator',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
) as dag:
    new_cluster = {
        'spark_version': '2.1.0-db3-scala2.11',
        'node_type_id': 'r3.xlarge',
        'aws_attributes': {'availability': 'ON_DEMAND'},
        'num_workers': 8,
    }

    notebook_task_params = {
        'new_cluster': new_cluster,
        'notebook_task': {
            'notebook_path': '/Users/airflow@example.com/PrepareData',
        },
    }
    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    notebook_task = DatabricksSubmitRunOperator(task_id='notebook_task', json=notebook_task_params)
    # [END howto_operator_databricks_json]

    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksSubmitRunOperator
    # to initialize the operator.
    spark_jar_task = DatabricksSubmitRunOperator(
        task_id='spark_jar_task',
        new_cluster=new_cluster,
        spark_jar_task={'main_class_name': 'com.example.ProcessData'},
        libraries=[{'jar': 'dbfs:/lib/etl-0.1.jar'}],
    )
    # [END howto_operator_databricks_named]
    notebook_task >> spark_jar_task
