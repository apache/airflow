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

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

with DAG(
    "test_valid_param2",
    start_date=datetime(2021, 1, 1),
    schedule="0 0 * * *",
    params={
        # mandatory string has default, this is how we want it!
        "str_param": Param("some_default", type="string", minLength=2, maxLength=12),
        # Field does not need to have a default if type is nullable
        "optional_str_param": Param(None, type=["null", "string"]),
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
