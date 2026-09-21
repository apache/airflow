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
Example DAG demonstrating LLM-powered retry policies.

The model names the kind of failure from the categories the policy offers it, each with a
description. Whether that category is retried, after how long, and how sure the model has
to be all come from the policy's category table in the worker.

Prerequisites:
  - Connection ``pydanticai_default`` with ``conn_type='pydanticai'``,
    ``password=<API key>``, ``extra='{"model": "anthropic:claude-haiku-4-5-20251001"}'``
  - ``pip install apache-airflow-providers-common-ai[anthropic]``
  - For the classifier-model Dag: connection ``jev_default`` with
    ``extra='{"model": "typesafe:jev-1.13.0"}'`` and
    ``pip install 'apache-airflow-providers-common-ai[typesafe]'``
"""

from __future__ import annotations

from dataclasses import replace
from datetime import timedelta

from airflow.providers.common.compat.sdk import dag, task

try:
    from airflow.providers.common.ai.policies.retry import DEFAULT_CATEGORIES, ErrorCategory, LLMRetryPolicy
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule

    llm_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,
        fallback_rules=[
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )

    # The default table fails ``resource``, on the grounds that a missing table needs a
    # human. Here the table is created upstream, so it is worth one more look.
    patient_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        categories={
            **DEFAULT_CATEGORIES,
            "resource": replace(DEFAULT_CATEGORIES["resource"], retry=True, delay=timedelta(minutes=5)),
        },
    )

    @dag(catchup=False, tags=["example", "retry_policy", "llm"])
    def example_llm_retry_policy():
        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_auth_error():
            """Should classify as ``auth``, which the table fails -> FAIL immediately."""
            raise PermissionError("403 Forbidden: API key expired for service account analytics@proj.iam")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_rate_limit():
            """Should classify as ``rate_limit``, which the table retries after 60s."""
            raise RuntimeError("429 Too Many Requests: Rate limit exceeded. Retry after 60 seconds.")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_data_error():
            """Should classify as ``data``, which the table fails -> FAIL immediately."""
            raise ValueError("Column 'user_id' expected type INT but got STRING in row 42.")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=patient_policy)
        def task_missing_table():
            """Should classify as ``resource``, which this policy's table retries after 5m."""
            raise FileNotFoundError("Table 'analytics.daily_orders' does not exist")

        task_auth_error()
        task_rate_limit()
        task_data_error()
        task_missing_table()

    example_llm_retry_policy()

    # [START howto_retry_policy_classifier]
    # A classifier model answers the same question in a few hundred milliseconds and reports
    # how sure it is. The categories are this pipeline's own; their descriptions are what the
    # model reads. ``permanent`` ends the task and costs it every retry it had left, so it
    # demands more certainty than the rest. Under a bar the answer is discarded and the
    # fallback rules, then the task's own retry settings, decide instead. The bars come from
    # a calibration run on jev-1.13.0: correct picks landed at 0.89 and above, wrong ones
    # at 0.47 to 0.69, with one wrong ``permanent`` at 0.90 that no sensible bar catches.
    snowflake_policy = LLMRetryPolicy(
        llm_conn_id="jev_default",
        min_confidence=0.8,
        categories={
            "queued": ErrorCategory(
                "Statement queued or a concurrency limit reached; the warehouse is busy.",
                delay=timedelta(seconds=120),
            ),
            "warehouse_suspended": ErrorCategory(
                "The warehouse is suspended and will auto-resume.", delay=timedelta(seconds=30)
            ),
            "token_expired": ErrorCategory(
                "A JWT or session token expired; the token rotates on its own.", delay=timedelta(seconds=30)
            ),
            "schema_drift": ErrorCategory(
                "A referenced column, table or view does not exist; a person has to fix the schema.",
                retry=False,
            ),
            "permanent": ErrorCategory(
                "A code or configuration error that will fail identically on every attempt.",
                retry=False,
                min_confidence=0.9,
            ),
        },
        fallback_rules=[
            RetryRule(exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=30)),
        ],
    )
    # [END howto_retry_policy_classifier]

    @dag(catchup=False, tags=["example", "retry_policy", "llm", "classifier"])
    def example_llm_retry_policy_classifier():
        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=snowflake_policy)
        def task_warehouse_suspended():
            """Should classify as ``warehouse_suspended`` -> RETRY after 30s."""
            raise RuntimeError("000606 (57P03): Warehouse 'ANALYTICS_WH' is suspended; auto-resume pending.")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=snowflake_policy)
        def task_schema_drift():
            """Should classify as ``schema_drift`` -> FAIL immediately."""
            raise RuntimeError("002003 (42S02): SQL compilation error: Object 'ORDERS_V2' does not exist.")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=snowflake_policy)
        def task_ambiguous():
            """Reads as more than one category; under the bar the task's own retry settings apply."""
            raise RuntimeError("Query failed: an unexpected error occurred while processing the request.")

        task_warehouse_suspended()
        task_schema_drift()
        task_ambiguous()

    example_llm_retry_policy_classifier()
except ImportError:
    # RetryPolicy requires Airflow 3.3+; example DAG is skipped on older versions.
    pass
