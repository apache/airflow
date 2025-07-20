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

import pendulum

from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import DAG, Param, task

with DAG(
    dag_id="example_hitl_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "HITL"],
):
    wait_for_input = HITLEntryOperator(
        task_id="wait_for_input",
        subject="Please provide required information: ",
        params={"information": Param("", type="string")},
    )

    wait_for_option = HITLOperator(
        task_id="wait_for_option",
        subject="Please choose one option to proceeded: ",
        options=["option 1", "option 2", "option 3"],
    )

    valid_input_and_options = ApprovalOperator(
        task_id="valid_input_and_options",
        subject="Are the following input and options valid?",
        body="""
        Input: {{ task_instance.xcom_pull(task_ids='wait_for_input', key='return_value')["params_input"]["information"] }}
        Option: {{ task_instance.xcom_pull(task_ids='wait_for_option', key='return_value')["chosen_options"] }}
        """,
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
    )

    choose_a_branch_to_run = HITLBranchOperator(
        task_id="choose_a_branch_to_run",
        subject="You're now allowed to proceeded. Please choose one task to run: ",
        options=["task_1", "task_2", "task_3"],
    )

    @task
    def task_1(): ...

    @task
    def task_2(): ...

    @task
    def task_3(): ...

    (
        [wait_for_input, wait_for_option]
        >> valid_input_and_options
        >> choose_a_branch_to_run
        >> [task_1(), task_2(), task_3()]
    )
