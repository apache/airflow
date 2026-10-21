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
"""Example Dag asking a reviewer for input from inside a ``@task`` with ``request_input()``."""

from __future__ import annotations

import datetime

from airflow.providers.standard.operators.hitl import get_input_response, request_input
from airflow.sdk import DAG, Param, task

with DAG(dag_id="example_hitl_request_input", tags=["example", "HITL"]):
    # [START howto_hitl_request_input]
    @task
    def refund(order_id: str, task_state_store=None) -> str:
        answer = get_input_response()
        if answer is None:
            # First run: do the expensive part once and keep what the second run needs.
            amount = 42.5
            task_state_store.set("amount", amount)
            request_input(
                subject=f"Refund {amount} for order {order_id}?",
                options=["Approve", "Reject"],
                params={"reason": Param("", type=["string", "null"])},
                timeout=datetime.timedelta(hours=4),
            )
        # Second run, after the reviewer answered: the callable starts again from the top.
        amount = task_state_store.get("amount")
        task_state_store.delete("amount")
        if answer["chosen_options"] == ["Approve"]:
            return f"Refunded {amount} for {order_id}"
        return f"Refund for {order_id} rejected: {answer['params_input'].get('reason')}"

    # [END howto_hitl_request_input]

    refund("o-1")
