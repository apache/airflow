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

"""Example DAG for using the DatabricksWorkflowTaskGroup and DatabricksNotebookOperator."""

from __future__ import annotations

import os
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksNotebookOperator,
    DatabricksTaskOperator,
)
from airflow.providers.databricks.operators.databricks_workflow import DatabricksWorkflowTaskGroup
from airflow.utils.timezone import datetime

EXECUTION_TIMEOUT = int(os.getenv("EXECUTION_TIMEOUT", 6))

DATABRICKS_CONN_ID = os.getenv("DATABRICKS_CONN_ID", "databricks_conn")
DATABRICKS_NOTIFICATION_EMAIL = os.getenv("DATABRICKS_NOTIFICATION_EMAIL", "your_email@serviceprovider.com")

GROUP_ID = os.getenv("DATABRICKS_GROUP_ID", "1234").replace(".", "_")
USER = os.environ.get("USER")

QUERY_ID = os.environ.get("QUERY_ID", "c9cf6468-babe-41a6-abc3-10ac358c71ee")
WAREHOUSE_ID = os.environ.get("WAREHOUSE_ID", "cf414a2206dfb397")

job_cluster_spec = [
    {
        "job_cluster_key": "Shared_job_cluster",
        "new_cluster": {
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
        },
    }
]
dag = DAG(
    dag_id="example_databricks_workflow",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["example", "databricks"],
)
with dag:
    # [START howto_databricks_workflow_notebook]
    task_group = DatabricksWorkflowTaskGroup(
        group_id=f"test_workflow_{USER}_{GROUP_ID}",
        databricks_conn_id=DATABRICKS_CONN_ID,
        job_clusters=job_cluster_spec,
        notebook_params={"ts": "{{ ts }}"},
        notebook_packages=[
            {
                "pypi": {
                    "package": "simplejson==3.18.0",  # Pin specification version of a package like this.
                    "repo": "https://pypi.org/simple",  # You can specify your required Pypi index here.
                }
            },
        ],
        extra_job_params={
            "email_notifications": {
                "on_start": [DATABRICKS_NOTIFICATION_EMAIL],
            },
        },
    )
    with task_group:
        notebook_1 = DatabricksNotebookOperator(
            task_id="workflow_notebook_1",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Shared/Notebook_1",
            notebook_packages=[{"pypi": {"package": "Faker"}}],
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
            execution_timeout=timedelta(seconds=600),
        )

        notebook_2 = DatabricksNotebookOperator(
            task_id="workflow_notebook_2",
            databricks_conn_id=DATABRICKS_CONN_ID,
            notebook_path="/Shared/Notebook_2",
            source="WORKSPACE",
            job_cluster_key="Shared_job_cluster",
            notebook_params={"foo": "bar", "ds": "{{ ds }}"},
        )

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

        sql_query = DatabricksTaskOperator(
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

        notebook_1 >> notebook_2 >> task_operator_nb_1 >> sql_query
    # [END howto_databricks_workflow_notebook]

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
