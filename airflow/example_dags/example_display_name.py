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

import pendulum

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator


# [START dag_decorator_usage]
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    dag_display_name="Sample DAG with Display Name",
)
def example_display_name():
    sample_task_1 = EmptyOperator(
        task_id="sample_task_1",
        task_display_name="Sample Task 1",
    )

    @task(task_display_name="Sample Task 2")
    def sample_task_2():
        pass

    sample_task_1 >> sample_task_2()


example_dag = example_display_name()
# [END dag_decorator_usage]
