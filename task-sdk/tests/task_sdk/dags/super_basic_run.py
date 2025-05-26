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

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import dag


class CustomOperator(BaseOperator):
    def execute(self, context):
        task_id = context["task_instance"].task_id
        print(f"Hello World {task_id}!")
        assert context["task_instance"].try_number == 1
        assert context["dag"].dag_id == "super_basic_run"


@dag()
def super_basic_run():
    CustomOperator(task_id="hello")


super_basic_run()
