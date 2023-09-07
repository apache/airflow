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
"""
Example DAG demonstrating the ability to dynamically set operator attributes
"""
from __future__ import annotations

import pendulum

from airflow.decorators import dag
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.operators.smooth import SmoothOperator


def hello_world(*args, **kwargs):
    print("Hello, world!")
    return "No longer empty"


@dag(
    dag_id="example_dynamic_operator_attributes",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={"cwd": "./"},
)
def dynamic_operator_taskflow():
    # Typically, BashOperator does not template the "cwd" parameter
    # But we can override it and make it templated
    bash_operator = BashOperator(
        task_id="dynamically_templated_cwd",
        bash_command="pwd",
        cwd="{{ params.cwd }}",
    )
    bash_operator.template_fields = (*bash_operator.template_fields, "cwd")

    # Typically, BaseOperator returns NotImplementedError()
    # But we can make it do something by overriding execute()
    does_things = BaseOperator(task_id="does_things")
    does_things.execute = hello_world

    # DON'T override the class attribute. Only override instances of the class!
    # If you override the class attribute directly, it will impact other DAGs
    # using the operator in an unpredictable manner!
    # Don't do this:
    # SmoothOperator.yt_link = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"
    # Do this:
    smooth_operator = SmoothOperator(task_id="not_so_smooth")
    smooth_operator.yt_link = "https://www.youtube.com/watch?v=dQw4w9WgXcQ"


dynamic_operator_taskflow()
