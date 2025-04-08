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


class XComListener:
    def __init__(self, path: str, task_id: str):
        self.path = path
        self.task_id = task_id

    def write(self, line: str):
        with open(self.path, "a") as f:
            f.write(line + "\n")

    @hookimpl
    def on_task_instance_running(self, previous_state, task_instance):
        task_instance.xcom_push(key="listener", value="listener")
        task_instance.xcom_pull(task_ids=task_instance.task_id, key="listener")
        self.write("on_task_instance_running")

    @hookimpl
    def on_task_instance_success(self, previous_state, task_instance):
        read = task_instance.xcom_pull(task_ids=self.task_id, key="listener")
        self.write("on_task_instance_success")
        self.write(read)


def clear():
    pass
