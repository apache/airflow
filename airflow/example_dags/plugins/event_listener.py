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

from typing import TYPE_CHECKING

from airflow.listeners import hookimpl

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
    from airflow.utils.state import TaskInstanceState


# [START howto_listen_ti_running_task]
@hookimpl
def on_task_instance_running(previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance):
    """
    This method is called when task state changes to RUNNING.
    Through callback, parameters like previous_task_state, task_instance object can be accessed.
    This will give more information about current task_instance that is running its dag_run,
    task and dag information.
    """
    print("Task instance is in running state")
    print(" Previous state of the Task instance:", previous_state)

    name: str = task_instance.task_id

    context = task_instance.get_template_context()

    task = context["task"]

    if TYPE_CHECKING:
        assert task

    dag = task.dag
    dag_name = None
    if dag:
        dag_name = dag.dag_id
    print(f"Current task name:{name}")
    print(f"Dag name:{dag_name}")


# [END howto_listen_ti_running_task]


# [START howto_listen_ti_success_task]
@hookimpl
def on_task_instance_success(previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance):
    """
    This method is called when task state changes to SUCCESS.
    Through callback, parameters like previous_task_state, task_instance object can be accessed.
    This will give more information about current task_instance that has succeeded its
    dag_run, task and dag information.
    """
    print("Task instance in success state")
    print(" Previous state of the Task instance:", previous_state)

    context = task_instance.get_template_context()
    operator = context["task"]

    print(f"Task operator:{operator}")


# [END howto_listen_ti_success_task]


# [START howto_listen_ti_failure_task]
@hookimpl
def on_task_instance_failed(
    previous_state: TaskInstanceState, task_instance: RuntimeTaskInstance, error: None | str | BaseException
):
    """
    This method is called when task state changes to FAILED.
    Through callback, parameters like previous_task_state, task_instance object can be accessed.
    This will give more information about current task_instance that has failed its dag_run,
    task and dag information.
    """
    print("Task instance in failure state")

    context = task_instance.get_template_context()
    task = context["task"]

    if TYPE_CHECKING:
        assert task

    print("Task start")
    print(f"Task:{task}")
    if error:
        print(f"Failure caused by {error}")


# [END howto_listen_ti_failure_task]


# [START howto_listen_dagrun_success_task]
@hookimpl
def on_dag_run_success(dag_run: DagRun, msg: str):
    """
    This method is called when dag run state changes to SUCCESS.
    """
    print("Dag run in success state")
    start_date = dag_run.start_date
    end_date = dag_run.end_date

    print(f"Dag run start:{start_date} end:{end_date}")


# [END howto_listen_dagrun_success_task]


# [START howto_listen_dagrun_failure_task]
@hookimpl
def on_dag_run_failed(dag_run: DagRun, msg: str):
    """
    This method is called when dag run state changes to FAILED.
    """
    print("Dag run  in failure state")
    dag_id = dag_run.dag_id
    run_id = dag_run.run_id
    external_trigger = dag_run.external_trigger

    print(f"Dag information:{dag_id} Run id: {run_id} external trigger: {external_trigger}")
    print(f"Failed with message: {msg}")


# [END howto_listen_dagrun_failure_task]


# [START howto_listen_dagrun_running_task]
@hookimpl
def on_dag_run_running(dag_run: DagRun, msg: str):
    """
    This method is called when dag run state changes to RUNNING.
    """
    print("Dag run  in running state")
    queued_at = dag_run.queued_at

    version = dag_run.dag_version.version

    print(f"Dag information Queued at: {queued_at} version: {version}")


# [END howto_listen_dagrun_running_task]
