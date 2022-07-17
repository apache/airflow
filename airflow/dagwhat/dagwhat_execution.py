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
import typing

from airflow.executors.debug_executor import DebugExecutor
from airflow.models.taskinstance import TaskInstance
from airflow.dagwhat import base

class HypothesisExecutor(DebugExecutor):
    def __init__(self, tasks_and_outcomes: typing.Dict[str, base.TaskOutcome]):
        self.tasks_and_outcomes = tasks_and_outcomes

    def _run_task(self, ti: TaskInstance) -> bool:
        if ti.task_id in self.tasks_and_outcomes:
            pass
        else:
            # If we don't have a pre-determined outcome for this task, we must
            # simulate a success and a failure downstream.
            pass
