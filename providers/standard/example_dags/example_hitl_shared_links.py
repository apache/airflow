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
"""Example DAG demonstrating HITL shared links functionality."""

from __future__ import annotations

import datetime

import pendulum

from airflow.providers.standard.operators.hitl import ApprovalOperator
from airflow.providers.standard.utils.hitl_shared_links import hitl_shared_link_manager
from airflow.sdk import DAG, task

with DAG(
    dag_id="example_hitl_shared_links",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "HITL", "shared-links"],
):

    @task
    def generate_approval_links(**context):
        """Generate shared links for approval tasks."""
        ti = context["task_instance"]

        # Get database session
        from airflow.utils.session import create_session

        with create_session() as session:
            # Generate a direct action link for quick approval
            action_link = hitl_shared_link_manager.generate_action_link(
                dag_id=context["dag"].dag_id,
                dag_run_id=context["dag_run"].run_id,
                task_id="manager_approval",
                try_number=1,
                action="approve",
                chosen_options=["Approve"],
                params_input={"comment": "Auto-approved via shared link"},
                base_url="http://localhost:8080",
                session=session,
            )

            # Generate a redirect link for detailed review
            redirect_link = hitl_shared_link_manager.generate_redirect_link(
                dag_id=context["dag"].dag_id,
                dag_run_id=context["dag_run"].run_id,
                task_id="manager_approval",
                try_number=1,
                base_url="http://localhost:8080",
                session=session,
            )

        print(f"Action Link: {action_link['url']}")
        print(f"Redirect Link: {redirect_link['url']}")

        # Store links in XCom for later use
        ti.xcom_push(key="action_link", value=action_link["url"])
        ti.xcom_push(key="redirect_link", value=redirect_link["url"])

        return {
            "action_link": action_link["url"],
            "redirect_link": redirect_link["url"],
        }

    @task
    def process_data():
        """Simulate data processing."""
        return {"status": "processed", "records": 100}

    manager_approval = ApprovalOperator(
        task_id="manager_approval",
        subject="Please approve the data processing results",
        body="""
        The data processing task has completed successfully.

        **Summary:**
        - Records processed: {{ task_instance.xcom_pull(task_ids='process_data', key='return_value')['records'] }}
        - Status: {{ task_instance.xcom_pull(task_ids='process_data', key='return_value')['status'] }}

        **Shared Links:**
        - Quick Approval: {{ task_instance.xcom_pull(task_ids='generate_approval_links', key='action_link') }}
        - Detailed Review: {{ task_instance.xcom_pull(task_ids='generate_approval_links', key='redirect_link') }}

        Please review and approve or reject this processing.
        """,
        defaults="Reject",
        execution_timeout=datetime.timedelta(hours=24),
    )

    @task
    def approved_workflow():
        """Workflow for approved processing."""
        print("Processing approved - continuing with workflow")
        return {"status": "approved"}

    @task
    def rejected_workflow():
        """Workflow for rejected processing."""
        print("Processing rejected - stopping workflow")
        return {"status": "rejected"}

    # Define the workflow
    (
        generate_approval_links()
        >> process_data()
        >> manager_approval
        >> [approved_workflow(), rejected_workflow()]
    )
