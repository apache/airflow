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
"""Example DAGs demonstrating LLMBranchOperator and @task.llm_branch usage."""

from __future__ import annotations

from airflow.providers.common.ai.operators.llm_branch import LLMBranchOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_llm_branch_basic]
@dag
def example_llm_branch_operator():
    route = LLMBranchOperator(
        task_id="route_ticket",
        prompt="User says: 'My password reset email never arrived.'",
        llm_conn_id="pydanticai_default",
        system_prompt="Route support tickets to the right team.",
    )

    @task
    def handle_billing():
        return "Handling billing issue"

    @task
    def handle_auth():
        return "Handling auth issue"

    @task
    def handle_general():
        return "Handling general issue"

    route >> [handle_billing(), handle_auth(), handle_general()]


# [END howto_operator_llm_branch_basic]

example_llm_branch_operator()


# [START howto_operator_llm_branch_multi]
@dag
def example_llm_branch_multi():
    route = LLMBranchOperator(
        task_id="classify",
        prompt="This product is great but shipping was slow and the box was damaged.",
        llm_conn_id="pydanticai_default",
        system_prompt="Select all applicable categories for this customer review.",
        allow_multiple_branches=True,
    )

    @task
    def handle_positive():
        return "Processing positive feedback"

    @task
    def handle_shipping():
        return "Escalating shipping issue"

    @task
    def handle_packaging():
        return "Escalating packaging issue"

    route >> [handle_positive(), handle_shipping(), handle_packaging()]


# [END howto_operator_llm_branch_multi]

example_llm_branch_multi()


# [START howto_decorator_llm_branch]
@dag
def example_llm_branch_decorator():
    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        system_prompt="Route support tickets to the right team.",
    )
    def route_ticket(message: str):
        return f"Route this support ticket: {message}"

    @task
    def handle_billing():
        return "Handling billing issue"

    @task
    def handle_auth():
        return "Handling auth issue"

    @task
    def handle_general():
        return "Handling general issue"

    route_ticket("I was charged twice for my subscription.") >> [
        handle_billing(),
        handle_auth(),
        handle_general(),
    ]


# [END howto_decorator_llm_branch]

example_llm_branch_decorator()


# [START howto_decorator_llm_branch_multi]
@dag
def example_llm_branch_decorator_multi():
    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        system_prompt="Select all applicable categories for this customer review.",
        allow_multiple_branches=True,
    )
    def classify_review(review: str):
        return f"Classify this review: {review}"

    @task
    def handle_positive():
        return "Processing positive feedback"

    @task
    def handle_shipping():
        return "Escalating shipping issue"

    @task
    def handle_packaging():
        return "Escalating packaging issue"

    classify_review("Great product but shipping was slow.") >> [
        handle_positive(),
        handle_shipping(),
        handle_packaging(),
    ]


# [END howto_decorator_llm_branch_multi]

example_llm_branch_decorator_multi()
