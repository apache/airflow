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

import time

from airflow.sdk import DAG, task

dag = DAG("test_dag", description="Test DAG for Task SDK testing with long-running task", schedule=None)


@task(dag=dag)
def get_task_instance_id(ti=None):
    """Task that returns its own task instance ID"""
    return str(ti.id)


@task(dag=dag)
def long_running_task(ti=None):
    """Long-running task that sleeps for 5 minutes to allow testing"""
    print(f"Starting long-running task with TI ID: {ti.id}")
    print("This task will run for 5 minutes to allow API testing...")

    time.sleep(3000)

    print("Long-running task completed!")
    return "test completed"


get_ti_id = get_task_instance_id()
long_task = long_running_task()

get_ti_id >> long_task
