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
A Python task on a queue that is routed to a coordinator, for the KubernetesExecutor lang-SDK system test.

``python_task_on_golang_queue`` is a plain Python task, not a ``@task.stub`` task, so the Dag processor binds
no artifact to it. The scheduler still queues it on the ``golang`` queue, which is routed to the
``ExecutableCoordinator``, so its pod runs the task without a task handler artifact, and the worker fails the
task because its Dag file is not an artifact that the coordinator runs. The task has no retries, so its
state reason is the reason of that single try.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@task(queue="golang", retries=0)
def python_task_on_golang_queue():
    print("The worker fails this task before it runs")


@dag(dag_id="lang_sdk_misrouted", schedule=None)
def lang_sdk_misrouted():
    python_task_on_golang_queue()


lang_sdk_misrouted()
