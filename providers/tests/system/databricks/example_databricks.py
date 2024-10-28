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

from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksNotebookOperator,
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
    DatabricksTaskOperator,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_operator"

QUERY_ID = os.environ.get("QUERY_ID", "c9cf6468-babe-41a6-abc3-10ac358c71ee")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "cf414a2206dfb397")

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_jobs_create_json]
    # Example of using the JSON parameter to initialize the operator.
    job = {
        "tasks": [
            {
                "task_key": "test",
                "job_cluster_key": "job_cluster",
                "notebook_task": {
                    "notebook_path": "/Shared/test",
                },
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "job_cluster",
                "new_cluster": {
                    "spark_version": "7.3.x-scala2.12",
                    "node_type_id": "i3.xlarge",
                    "num_workers": 2,
                },
            },
        ],
    }

    jobs_create_json = DatabricksCreateJobsOperator(task_id="jobs_create_json", json=job)
    # [END howto_operator_databricks_jobs_create_json]

    # [START howto_operator_databricks_jobs_create_named]
    # Example of using the named parameters to initialize the operator.
    tasks = [
        {
            "task_key": "test",
            "job_cluster_key": "job_cluster",
            "notebook_task": {
                "notebook_path": "/Shared/test",
            },
        },
    ]
    job_clusters = [
        {
            "job_cluster_key": "job_cluster",
            "new_cluster": {
                "spark_version": "7.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2,
            },
        },
    ]

    jobs_create_named = DatabricksCreateJobsOperator(
        task_id="jobs_create_named", tasks=tasks, job_clusters=job_clusters
    )
    # [END howto_operator_databricks_jobs_create_named]

    # [START howto_operator_databricks_run_now]
    # Example of using the DatabricksRunNowOperator after creating a job with DatabricksCreateJobsOperator.
    run_now = DatabricksRunNowOperator(
        task_id="run_now", job_id="{{ ti.xcom_pull(task_ids='jobs_create_named') }}"
    )

    jobs_create_named >> run_now
    # [END howto_operator_databricks_run_now]

    # [START howto_operator_databricks_json]
    # Example of using the JSON parameter to initialize the operator.
    new_cluster = {
        "spark_version": "9.1.x-scala2.12",
        "node_type_id": "r3.xlarge",
        "aws_attributes": {"availability": "ON_DEMAND"},
        "num_workers": 8,
    }

    notebook_task_params = {
        "new_cluster": new_cluster,
        "notebook_task": {
            "notebook_path": "/Users/airflow@example.com/PrepareData",
        },
    }

    notebook_task = DatabricksSubmitRunOperator(
        task_id="notebook_task", json=notebook_task_params
    )
    # [END howto_operator_databricks_json]

    # [START howto_operator_databricks_named]
    # Example of using the named parameters of DatabricksSubmitRunOperator
    # to initialize the operator.
    spark_jar_task = DatabricksSubmitRunOperator(
        task_id="spark_jar_task",
        new_cluster=new_cluster,
        spark_jar_task={"main_class_name": "com.example.ProcessData"},
        libraries=[{"jar": "dbfs:/lib/etl-0.1.jar"}],
    )
    # [END howto_operator_databricks_named]
    notebook_task >> spark_jar_task

    # [START howto_operator_databricks_notebook_new_cluster]
    new_cluster_spec = {
        "cluster_name": "",
        "spark_version": "11.3.x-scala2.12",
        "aws_attributes": {
            "first_on_demand": 1,
            "availability": "SPOT_WITH_FALLBACK",
            "zone_id": "us-east-2b",
            "spot_bid_price_percent": 100,
            "ebs_volume_count": 0,
        },
        "node_type_id": "i3.xlarge",
        "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
        "enable_elastic_disk": False,
        "data_security_mode": "LEGACY_SINGLE_USER_STANDARD",
        "runtime_engine": "STANDARD",
        "num_workers": 8,
    }

    notebook_1 = DatabricksNotebookOperator(
        task_id="notebook_1",
        notebook_path="/Shared/Notebook_1",
        notebook_packages=[
            {
                "pypi": {
                    "package": "simplejson==3.18.0",
                    "repo": "https://pypi.org/simple",
                }
            },
            {"pypi": {"package": "Faker"}},
        ],
        source="WORKSPACE",
        new_cluster=new_cluster_spec,
    )
    # [END howto_operator_databricks_notebook_new_cluster]

    # [START howto_operator_databricks_notebook_existing_cluster]
    notebook_2 = DatabricksNotebookOperator(
        task_id="notebook_2",
        notebook_path="/Shared/Notebook_2",
        notebook_packages=[
            {
                "pypi": {
                    "package": "simplejson==3.18.0",
                    "repo": "https://pypi.org/simple",
                }
            },
        ],
        source="WORKSPACE",
        existing_cluster_id="existing_cluster_id",
    )
    # [END howto_operator_databricks_notebook_existing_cluster]

    # [START howto_operator_databricks_task_notebook]
    task_operator_nb_1 = DatabricksTaskOperator(
        task_id="nb_1",
        databricks_conn_id="databricks_conn",
        job_cluster_key="Shared_job_cluster",
        task_config={
            "notebook_task": {
                "notebook_path": "/Shared/Notebook_1",
                "source": "WORKSPACE",
            },
            "libraries": [
                {"pypi": {"package": "Faker"}},
                {"pypi": {"package": "simplejson"}},
            ],
        },
    )
    # [END howto_operator_databricks_task_notebook]

    # [START howto_operator_databricks_task_sql]
    task_operator_sql_query = DatabricksTaskOperator(
        task_id="sql_query",
        databricks_conn_id="databricks_conn",
        task_config={
            "sql_task": {
                "query": {
                    "query_id": QUERY_ID,
                },
                "warehouse_id": WAREHOUSE_ID,
            }
        },
    )
    # [END howto_operator_databricks_task_sql]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
