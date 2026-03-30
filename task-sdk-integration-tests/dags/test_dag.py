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
def return_tuple_task(ti=None):
    """Task that returns a tuple for testing XCom serialization/deserialization"""
    return 1, "test_value"


@task(dag=dag)
def mapped_task(value, ti=None):
    """Mapped task that processes individual values for testing XCom sequence operations"""
    print(f"Processing value: {value} with TI ID: {ti.id}, map_index: {ti.map_index}")
    # Return a modified value for XCom testing
    return f"processed_{value}"


@task(dag=dag)
def long_running_task(ti=None):
    """Long-running task that sleeps for 5 minutes to allow testing"""
    print(f"Starting long-running task with TI ID: {ti.id}")
    print("This task will run for 5 minutes to allow API testing...")

    time.sleep(3000)

    print("Long-running task completed!")
    return "test completed"


get_ti_id = get_task_instance_id()
tuple_task = return_tuple_task()
mapped_instances = mapped_task.expand(value=["alpha", "beta", "gamma", "delta"])
long_task = long_running_task()

get_ti_id >> tuple_task >> mapped_instances >> long_task
