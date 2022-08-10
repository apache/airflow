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
from datetime import datetime
from typing import Sequence

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

DEFAULT_DATE = datetime(2016, 1, 1)

default_args = {
    "owner": "airflow",
    "start_date": DEFAULT_DATE,
}

dag = DAG(dag_id="test_only_empty_tasks", default_args=default_args, schedule='@once')


class MyEmptyOperator(EmptyOperator):
    template_fields_renderers = {"body": "json"}
    template_fields: Sequence[str] = ("body",)

    def __init__(self, body, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.body = body


with dag:
    task_a = EmptyOperator(task_id="test_task_a")

    task_b = EmptyOperator(task_id="test_task_b")

    task_a >> task_b

    task_c = MyEmptyOperator(task_id="test_task_c", body={"hello": "world"})

    task_d = EmptyOperator(task_id="test_task_on_execute", on_execute_callback=lambda *args, **kwargs: None)

    task_e = EmptyOperator(task_id="test_task_on_success", on_success_callback=lambda *args, **kwargs: None)
