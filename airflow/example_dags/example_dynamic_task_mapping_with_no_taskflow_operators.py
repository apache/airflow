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
"""Example DAG demonstrating the usage of dynamic task mapping with non-TaskFlow operators."""

from __future__ import annotations

from datetime import datetime

from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG


class AddOneOperator(BaseOperator):
    """A custom operator that adds one to the input."""

    def __init__(self, value, **kwargs):
        super().__init__(**kwargs)
        self.value = value

    def execute(self, context):
        return self.value + 1


class SumItOperator(BaseOperator):
    """A custom operator that sums the input."""

    template_fields = ("values",)

    def __init__(self, values, **kwargs):
        super().__init__(**kwargs)
        self.values = values

    def execute(self, context):
        total = sum(self.values)
        print(f"Total was {total}")
        return total


with DAG(
    dag_id="example_dynamic_task_mapping_with_no_taskflow_operators",
    schedule=None,
    start_date=datetime(2022, 3, 4),
    catchup=False,
):
    # map the task to a list of values
    add_one_task = AddOneOperator.partial(task_id="add_one").expand(value=[1, 2, 3])

    # aggregate (reduce) the mapped tasks results
    sum_it_task = SumItOperator(task_id="sum_it", values=add_one_task.output)
