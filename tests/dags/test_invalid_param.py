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

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

with DAG(
    "test_invalid_param",
    start_date=datetime(2021, 1, 1),
    schedule_interval="@once",
    params={
        # a mandatory str param
        "str_param": Param(type="string", minLength=2, maxLength=4),
    },
) as the_dag:

    def print_these(*params):
        for param in params:
            print(param)

    PythonOperator(
        task_id="ref_params",
        python_callable=print_these,
        op_args=[
            "{{ params.str_param }}",
        ],
    )
