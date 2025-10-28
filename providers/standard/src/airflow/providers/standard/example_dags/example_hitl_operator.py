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
from typing import TYPE_CHECKING

import pendulum

from airflow.providers.standard.operators.hitl import (
    ApprovalOperator,
    HITLBranchOperator,
    HITLEntryOperator,
    HITLOperator,
)
from airflow.sdk import DAG, Param, task
from airflow.sdk.bases.notifier import BaseNotifier

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

# [START hitl_tutorial]


# [START hitl_notifier]
class LocalLogNotifier(BaseNotifier):
    """Simple notifier to demonstrate HITL notification without setup any connection."""

    template_fields = ("message",)

    def __init__(self, message: str) -> None:
        self.message = message

    def notify(self, context: Context) -> None:
        url = HITLOperator.generate_link_to_ui_from_context(
            context=context,
            base_url="http://localhost:28080",
        )
        self.log.info(self.message)
        self.log.info("Url to respond %s", url)


hitl_request_callback = LocalLogNotifier(
    message="""
[HITL]
Subject: {{ task.subject }}
Body: {{ task.body }}
Options: {{ task.options }}
Is Multiple Option: {{ task.multiple }}
Default Options: {{ task.defaults }}
Params: {{ task.params }}
"""
)
hitl_success_callback = LocalLogNotifier(
    message="{% set task_id = task.task_id -%}{{ ti.xcom_pull(task_ids=task_id) }}"
)
hitl_failure_callback = LocalLogNotifier(message="Request to response to '{{ task.subject }}' failed")
# [END hitl_notifier]

with DAG(
    dag_id="example_hitl_operator",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "HITL"],
):
    # [START howto_hitl_entry_operator]
    wait_for_input = HITLEntryOperator(
        task_id="wait_for_input",
        subject="Please provide required information: ",
        params={"information": Param("", type="string")},
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    # [END howto_hitl_entry_operator]

    # [START howto_hitl_operator]
    wait_for_option = HITLOperator(
        task_id="wait_for_option",
        subject="Please choose one option to proceed: ",
        options=["option 1", "option 2", "option 3"],
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    # [END howto_hitl_operator]

    # [START howto_hitl_operator_multiple]
    wait_for_multiple_options = HITLOperator(
        task_id="wait_for_multiple_options",
        subject="Please choose option to proceed: ",
        options=["option 4", "option 5", "option 6"],
        multiple=True,
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    # [END howto_hitl_operator_multiple]

    # [START howto_hitl_operator_timeout]
    wait_for_default_option = HITLOperator(
        task_id="wait_for_default_option",
        subject="Please choose option to proceed: ",
        options=["option 7", "option 8", "option 9"],
        defaults=["option 7"],
        execution_timeout=datetime.timedelta(seconds=1),
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    # [END howto_hitl_operator_timeout]

    # [START howto_hitl_approval_operator]
    valid_input_and_options = ApprovalOperator(
        task_id="valid_input_and_options",
        subject="Are the following input and options valid?",
        body="""
        Input: {{ ti.xcom_pull(task_ids='wait_for_input')["params_input"]["information"] }}
        Option: {{ ti.xcom_pull(task_ids='wait_for_option')["chosen_options"] }}
        Multiple Options: {{ ti.xcom_pull(task_ids='wait_for_multiple_options')["chosen_options"] }}
        Timeout Option: {{ ti.xcom_pull(task_ids='wait_for_default_option')["chosen_options"] }}
        """,
        defaults="Reject",
        execution_timeout=datetime.timedelta(minutes=1),
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
        assigned_users=[{"id": "admin", "name": "admin"}],
    )
    # [END howto_hitl_approval_operator]

    # [START howto_hitl_branch_operator]
    choose_a_branch_to_run = HITLBranchOperator(
        task_id="choose_a_branch_to_run",
        subject="You're now allowed to proceeded. Please choose one task to run: ",
        options=["task_1", "task_2", "task_3"],
        notifiers=[hitl_request_callback],
        on_success_callback=hitl_success_callback,
        on_failure_callback=hitl_failure_callback,
    )
    # [END howto_hitl_branch_operator]

    # [START howto_hitl_workflow]
    @task
    def task_1(): ...

    @task
    def task_2(): ...

    @task
    def task_3(): ...

    (
        [wait_for_input, wait_for_option, wait_for_default_option, wait_for_multiple_options]
        >> valid_input_and_options
        >> choose_a_branch_to_run
        >> [task_1(), task_2(), task_3()]
    )
    # [END howto_hitl_workflow]

# [END hitl_tutorial]
