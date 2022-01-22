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

# Ignore missing args provided by default_args
# type: ignore[call-arg]

"""
Example DAG showing how to use Asana TaskOperators.
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.asana.operators.asana_tasks import (
    AsanaCreateTaskOperator,
    AsanaDeleteTaskOperator,
    AsanaFindTaskOperator,
    AsanaUpdateTaskOperator,
)

ASANA_TASK_TO_UPDATE = os.environ.get("ASANA_TASK_TO_UPDATE", "update_task")
ASANA_TASK_TO_DELETE = os.environ.get("ASANA_TASK_TO_DELETE", "delete_task")
# This example assumes a default project ID has been specified in the connection. If you
# provide a different id in ASANA_PROJECT_ID_OVERRIDE, it will override this default
# project ID in the AsanaFindTaskOperator example below
ASANA_PROJECT_ID_OVERRIDE = os.environ.get("ASANA_PROJECT_ID_OVERRIDE", "test_project")
# This connection should specify a personal access token and a default project ID
CONN_ID = os.environ.get("ASANA_CONNECTION_ID")


with DAG(
    "example_asana",
    start_date=datetime(2021, 1, 1),
    default_args={"conn_id": CONN_ID},
    tags=["example"],
    catchup=False,
) as dag:
    # [START run_asana_create_task_operator]
    # Create a task. `task_parameters` is used to specify attributes the new task should have.
    # You must specify at least one of 'workspace', 'projects', or 'parent' in `task_parameters`
    # unless these are specified in the connection. Any attributes you specify in
    # `task_parameters` will override values from the connection.
    create = AsanaCreateTaskOperator(
        task_id="run_asana_create_task",
        task_parameters={"notes": "Some notes about the task."},
        name="New Task Name",
    )
    # [END run_asana_create_task_operator]

    # [START run_asana_find_task_operator]
    # Find tasks matching search criteria. `search_parameters` is used to specify these criteria.
    # You must specify `project`, `section`, `tag`, `user_task_list`, or both
    # `assignee` and `workspace` in `search_parameters` or in the connection.
    # This example shows how you can override a project specified in the connection by
    # passing a different value for project into `search_parameters`
    one_week_ago = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    find = AsanaFindTaskOperator(
        task_id="run_asana_find_task",
        search_parameters={"project": ASANA_PROJECT_ID_OVERRIDE, "modified_since": one_week_ago},
    )
    # [END run_asana_find_task_operator]

    # [START run_asana_update_task_operator]
    # Update a task. `task_parameters` is used to specify the new values of
    # task attributes you want to update.
    update = AsanaUpdateTaskOperator(
        task_id="run_asana_update_task",
        asana_task_gid=ASANA_TASK_TO_UPDATE,
        task_parameters={"notes": "This task was updated!", "completed": True},
    )
    # [END run_asana_update_task_operator]

    # [START run_asana_delete_task_operator]
    # Delete a task. This task will complete successfully even if `asana_task_gid` does not exist.
    delete = AsanaDeleteTaskOperator(
        task_id="run_asana_delete_task",
        asana_task_gid=ASANA_TASK_TO_DELETE,
    )
    # [END run_asana_delete_task_operator]

    create >> find >> update >> delete
