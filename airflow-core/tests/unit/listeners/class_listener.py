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

from airflow.listeners import hookimpl
from airflow.utils.state import DagRunState, TaskInstanceState


class ClassBasedListener:
    def __init__(self):
        self.started_component = None
        self.stopped_component = None
        self.state = []
        self.dag_run_msg: str | None = None
        self.dag_has_dag_attr: bool | None = None
        self.dag_run_note_at_listener: str | None = None

    @hookimpl
    def on_starting(self, component):
        self.started_component = component
        self.state.append(DagRunState.RUNNING)

    @hookimpl
    def before_stopping(self, component):
        self.stopped_component = component
        self.state.append(DagRunState.SUCCESS)

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance):
        self.state.append(TaskInstanceState.RUNNING)

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance):
        self.state.append(TaskInstanceState.SUCCESS)

    @hookimpl
    def on_task_instance_failed(self, previous_state, task_instance, error: None | str | BaseException):
        self.state.append(TaskInstanceState.FAILED)

    @hookimpl
    def on_task_instance_skipped(self, previous_state, task_instance):
        self.state.append(TaskInstanceState.SKIPPED)

    @hookimpl
    def on_dag_run_running(self, dag_run, msg: str):
        self.state.append(DagRunState.RUNNING)

    @hookimpl
    def on_dag_run_success(self, dag_run, msg: str):
        self.state.append(DagRunState.SUCCESS)
        self.dag_run_msg = msg
        self.dag_has_dag_attr = dag_run.dag is not None
        self.dag_run_note_at_listener = dag_run.note

    @hookimpl
    def on_dag_run_failed(self, dag_run, msg: str):
        self.state.append(DagRunState.FAILED)
        self.dag_run_msg = msg
        self.dag_has_dag_attr = dag_run.dag is not None
        self.dag_run_note_at_listener = dag_run.note


def clear():
    pass
