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

import datetime
from typing import Any

from airflow.decorators import task
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.utils.context import Context

custom_pool_name = "custom_pool_name"

@task
def make_arg_lists():
    return ["1", "2", "3"]

@task
def create_pool_if_not_exists():
    from airflow.models.pool import Pool

    Pool.create_or_update_pool(
        name=custom_pool_name,
        slots=2,
        description="Test pool",
        include_deferred=False
    )

class MyOperator(BaseOperator):
    template_fields = ("arg1",)

    def __init__(self, arg1, **kwargs):
        self.arg1 = arg1
        self.pool = custom_pool_name + f"_{str(arg1)}"
        kwargs["pool"] = self.pool
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        print(repr(self.arg1))

with DAG(dag_id="test_mapped_custom_operator", start_date=datetime.datetime(2023, 1, 1)) as dag:
    create_pool_if_not_exists_task = create_pool_if_not_exists()
    make_arg_list_task = make_arg_lists()
    mapped_task = MyOperator.partial(task_id="mapped_task").expand(arg1=make_arg_list_task)
    create_pool_if_not_exists_task >> make_arg_list_task
