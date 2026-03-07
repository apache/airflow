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
Example Airflow DAG that demonstrates Google Cloud quota project functionality.
This DAG shows how to use quota project settings with Google Cloud connections
and operators.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.email import send_email

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "example_quota_project"

def test_quota_project(gcp_conn_id: str = "google_cloud_default", quota_project_id: str | None = None, **context):
    """Test quota project configuration with Google Cloud credentials."""
    hook = BigQueryHook(gcp_conn_id=gcp_conn_id, quota_project_id=quota_project_id)
    credentials, project_id = hook.get_credentials_and_project_id()
    quota_project = getattr(credentials, "_quota_project_id", None)
    
    hook.log.info("Testing Google Cloud quota project configuration:")
    hook.log.info("Credentials project ID: %s", project_id)
    hook.log.info("Quota project ID: %s", quota_project)
    
    return {
        "project_id": project_id,
        "quota_project_id": quota_project,
    }

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "quota-project"],
) as dag:
    # [START howto_quota_project_from_connection]
    # This task uses quota project from connection if configured
    test_conn_quota = PythonOperator(
        task_id="test_connection_quota",
        python_callable=test_quota_project,
        op_kwargs={"gcp_conn_id": "google_cloud_default"},  # Assumes quota_project_id in connection extras
    )
    # [END howto_quota_project_from_connection]

    # [START howto_quota_project_from_param]
    # This task explicitly sets quota project
    test_param_quota = PythonOperator(
        task_id="test_parameter_quota",
        python_callable=test_quota_project,
        op_kwargs={
            "gcp_conn_id": "google_cloud_default",
            "quota_project_id": "explicit-quota-project",
        },
    )
    # [END howto_quota_project_from_param]

    # Task dependencies
    test_conn_quota >> test_param_quota

# [START howto_quota_project_email_on_failure]
def _handle_quota_failure(context):
    """Send an email if quota project test fails."""
    dag_id = context["task_instance"].dag_id
    task_id = context["task_instance"].task_id
    logs_url = context["task_instance"].log_url
    
    send_email(
        to="airflow-alerts@example.com",
        subject=f"[Airflow] Quota Project Test Failed - {dag_id}.{task_id}",
        html_content=f"""
        Quota project test failed.<br/>
        DAG: {dag_id}<br/>
        Task: {task_id}<br/>
        Logs: <a href="{logs_url}">Link</a>
        """,
    )
# [END howto_quota_project_email_on_failure]

for task in dag.tasks:
    task.on_failure_callback = _handle_quota_failure

# ### Everything below this line is not part of example ###
# ### Just for system tests purpose ###
from tests.system.utils.watcher import watcher

list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
