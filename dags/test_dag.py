from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import (
    DatabricksCreateJobsOperator,
    DatabricksNotebookOperator,
    DatabricksRunNowOperator,
    DatabricksSQLStatementsOperator,
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
