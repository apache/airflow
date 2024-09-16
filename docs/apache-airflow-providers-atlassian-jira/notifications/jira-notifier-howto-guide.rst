
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

How-to guide for Atlassian Jira notifications
=============================================

Introduction
------------
The Atlassian Jira notifier (:class:`airflow.providers.atlassian.jira.notifications.jira.JiraNotifier`) allows users to create
issues in a Jira instance using the various ``on_*_callbacks`` available at both the DAG level and Task level

Example Code
------------

.. code-block:: python

    from datetime import datetime
    from airflow import DAG
    from airflow.providers.standard.core.operators.bash import BashOperator
    from airflow.providers.atlassian.jira.notifications.jira import send_jira_notification

    with DAG(
        "test-dag",
        start_date=datetime(2023, 11, 3),
        on_failure_callback=[
            send_jira_notification(
                jira_conn_id="my-jira-conn",
                description="Failure in the DAG {{ dag.dag_id }}",
                summary="Airflow DAG Issue",
                project_id=10000,
                issue_type_id=10003,
                labels=["airflow-dag-failure"],
            )
        ],
    ):
        BashOperator(
            task_id="mytask",
            on_failure_callback=[
                send_jira_notification(
                    jira_conn_id="my-jira-conn",
                    description="The task {{ ti.task_id }} failed",
                    summary="Airflow Task Issue",
                    project_id=10000,
                    issue_type_id=10003,
                    labels=["airflow-task-failure"],
                )
            ],
            bash_command="fail",
            retries=0,
        )
