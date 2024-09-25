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
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.operators.databricks_repos import (
    DatabricksReposCreateOperator,
    DatabricksReposDeleteOperator,
    DatabricksReposUpdateOperator,
)

default_args = {
    "owner": "airflow",
    "databricks_conn_id": "databricks",
}

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_databricks_repos_operator"

with DAG(
    dag_id=DAG_ID,
    schedule="@daily",
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    tags=["example"],
    catchup=False,
) as dag:
    # [START howto_operator_databricks_repo_create]
    # Example of creating a Databricks Repo
    repo_path = "/Repos/user@domain.com/demo-repo"
    git_url = "https://github.com/test/test"
    create_repo = DatabricksReposCreateOperator(task_id="create_repo", repo_path=repo_path, git_url=git_url)
    # [END howto_operator_databricks_repo_create]

    # [START howto_operator_databricks_repo_update]
    # Example of updating a Databricks Repo to the latest code
    repo_path = "/Repos/user@domain.com/demo-repo"
    update_repo = DatabricksReposUpdateOperator(task_id="update_repo", repo_path=repo_path, branch="releases")
    # [END howto_operator_databricks_repo_update]

    notebook_task_params = {
        "new_cluster": {
            "spark_version": "9.1.x-scala2.12",
            "node_type_id": "r3.xlarge",
            "aws_attributes": {"availability": "ON_DEMAND"},
            "num_workers": 8,
        },
        "notebook_task": {
            "notebook_path": f"{repo_path}/PrepareData",
        },
    }

    notebook_task = DatabricksSubmitRunOperator(task_id="notebook_task", json=notebook_task_params)

    # [START howto_operator_databricks_repo_delete]
    # Example of deleting a Databricks Repo
    repo_path = "/Repos/user@domain.com/demo-repo"
    delete_repo = DatabricksReposDeleteOperator(task_id="delete_repo", repo_path=repo_path)
    # [END howto_operator_databricks_repo_delete]

    (create_repo >> update_repo >> notebook_task >> delete_repo)

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
