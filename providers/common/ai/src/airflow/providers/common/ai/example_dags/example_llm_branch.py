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

from datetime import timedelta

from airflow.providers.common.ai.operators.llm_branch import BranchOption, DecisionPolicy, LLMBranchOperator
from airflow.providers.common.compat.sdk import dag, task


# [START howto_operator_llm_branch_basic]
@dag(tags=["example"])
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


# [START howto_operator_llm_branch_descriptions]
@dag(tags=["example"])
def example_llm_branch_descriptions():
    route = LLMBranchOperator(
        task_id="route_ticket",
        prompt="User says: 'My password reset email never arrived.'",
        llm_conn_id="pydanticai_default",
        system_prompt=(
            "Route the ticket to the team responsible for resolving it. "
            "Use the reported problem rather than the team the user asks for."
        ),
        # A string is shorthand for BranchOption(description=...).
        branches={
            "handle_auth": (
                "Sign-in, passwords, 2FA and account lockouts. This team owns missing password-reset emails."
            ),
            "handle_billing": "Invoices, charges, refunds and plan changes.",
            "handle_general": (
                "General support triage: product questions, issues outside the other "
                "teams' responsibilities, and tickets that need clarification."
            ),
        },
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


# [END howto_operator_llm_branch_descriptions]

example_llm_branch_descriptions()


# [START howto_operator_llm_branch_decision_policy]
@dag(tags=["example"])
def example_llm_branch_decision_policy():
    # A classifier model reports how sure it is of each pick; a text model does not, and
    # with a min_confidence set every pick would count as uncertain and go to review.
    route = LLMBranchOperator(
        task_id="triage_failure",
        prompt=(
            "Task load_orders failed: psycopg2.OperationalError: could not connect to server: "
            "Connection timed out. Is the server running on host db.internal (10.0.4.12)?"
        ),
        llm_conn_id="pydanticai_default",
        model_id="typesafe:jev-1.13.0",
        system_prompt="Pick the remediation that addresses the cause of the failure.",
        branches={
            "rerun": "The failure looks transient: a timeout, a dropped connection, a rate limit.",
            # Paging someone on a wrong pick costs more than an extra rerun, so this branch needs more.
            "page_oncall": BranchOption(
                "Something a person has to fix now: data corruption, an outage, a security issue.",
                min_confidence=0.9,
            ),
            "ignore": "Expected or harmless: a known flaky check, a duplicate alert.",
        },
        decision_policy=DecisionPolicy(min_confidence=0.6, on_uncertain="review"),
        approval_timeout=timedelta(hours=4),
        allow_modifications=True,
    )

    @task
    def rerun():
        return "Clearing the failed task"

    @task
    def page_oncall():
        return "Paging on-call"

    @task
    def ignore():
        return "Leaving it"

    route >> [rerun(), page_oncall(), ignore()]


# [END howto_operator_llm_branch_decision_policy]

example_llm_branch_decision_policy()


# [START howto_operator_llm_branch_multi]
@dag(tags=["example"])
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
@dag(tags=["example"])
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
@dag(tags=["example"])
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


# [START howto_operator_llm_branch_approval]
@dag(tags=["example"])
def example_llm_branch_approval():
    route = LLMBranchOperator(
        task_id="route_with_approval",
        prompt="User says: 'I was charged twice for my subscription.'",
        llm_conn_id="pydanticai_default",
        system_prompt="Route support tickets to the right team.",
        require_approval=True,
        approval_timeout=timedelta(hours=24),
        allow_modifications=True,
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


# [END howto_operator_llm_branch_approval]

example_llm_branch_approval()
