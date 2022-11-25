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
from __future__ import annotations

from airflow.listeners import hookimpl
from airflow.utils.state import State

started_component = None
stopped_component = None
state = []


@hookimpl
def on_starting(component):
    global started_component
    started_component = component


@hookimpl
def before_stopping(component):
    global stopped_component
    stopped_component = component


@hookimpl
def on_task_instance_running(previous_state, task_instance, session):
    state.append(State.RUNNING)


@hookimpl
def on_task_instance_success(previous_state, task_instance, session):
    state.append(State.SUCCESS)


@hookimpl
def on_task_instance_failed(previous_state, task_instance, session):
    state.append(State.FAILED)


def clear():
    global started_component, stopped_component, state
    started_component = None
    stopped_component = None
    state = []
