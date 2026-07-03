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

from datetime import datetime

from airflow.sdk import dag, task, task_group


@dag(
    dag_id="TEST_DTM",
    dag_display_name="TEST DTM",
    schedule=None,
    default_args={"owner": "airflow", "email": ""},
    start_date=datetime(2024, 1, 25),
)
def dtm_test(
    exponent: int = 2,
):

    @task
    def get_data():
        return [20, 100, 200, 222, 242, 272]

    @task
    def to_exp(number: int, exponent: int) -> float:
        return number**exponent

    @task
    def trunc(number: float, digits: int) -> float:
        return round(number / 22, digits)

    @task
    def save(number: list[float]):
        for n in number:
            print(f"Got number: {n}")

    @task_group  # type: ignore[type-var]
    def transform(number: int, exponent: int) -> float:
        a = to_exp(number, exponent)
        b = trunc(a, 2)
        return b

    data = get_data()
    result = transform.partial(exponent=exponent).expand(number=data)
    save(result)  # type: ignore[arg-type]


instance = dtm_test()
