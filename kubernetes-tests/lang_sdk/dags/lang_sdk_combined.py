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
Combined Python + Go + Java stub Dag for the KubernetesExecutor lang-SDK system test.

A single ``lang_sdk_combined`` Dag chains native Python tasks, Go stub tasks, and
Java stub tasks so one run exercises all three runtimes on KubernetesExecutor::

    python_task_1 >> go_extract >> go_transform >> java_extract >> java_transform >> python_task_2

* ``go_extract`` / ``go_transform`` are ``@task.stub(queue="golang")``; the
  ``golang`` queue is routed to the ``ExecutableCoordinator``. Their Go
  implementations live in ``../go_example`` under this same dag_id.
* ``java_extract`` / ``java_transform`` are ``@task.stub(queue="java")``; the
  ``java`` queue is routed to the ``JavaCoordinator``. Their Java
  implementations live in ``../java_example`` under this same dag_id.
* The Python tasks run on the default Python path.

The dag_id and the Go/Java task ids MUST match the identities the bundles expose
so each coordinator can locate its artifact by dag_id and look up the task by id.
"""

from __future__ import annotations

from airflow.sdk import dag, task


@task()
def python_task_1():
    return "value_from_python_task_1"


@task.stub(queue="golang")
def go_extract(): ...


@task.stub(queue="golang")
def go_transform(): ...


@task.stub(queue="java")
def java_extract(): ...


@task.stub(queue="java")
def java_transform(): ...


@task()
def python_task_2():
    print("python_task_2")


@dag(dag_id="lang_sdk_combined")
def lang_sdk_combined():
    (
        python_task_1()
        >> go_extract()
        >> go_transform()
        >> java_extract()
        >> java_transform()
        >> python_task_2()
    )


lang_sdk_combined()
