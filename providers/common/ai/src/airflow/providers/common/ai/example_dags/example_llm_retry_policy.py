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

Uses an LLM (via PydanticAIHook) to classify errors and decide whether
to retry, fail immediately, or retry with a custom delay.

Prerequisites:
  - Connection 'pydanticai_default' with conn_type='pydanticai',
    password=<API key>, extra='{"model": "anthropic:claude-haiku-4-5-20251001"}'
  - ``pip install apache-airflow-providers-common-ai[anthropic]``
"""
from __future__ import annotations

from datetime import timedelta

from airflow.providers.common.compat.sdk import dag, task

try:
    from airflow.sdk.definitions.retry_policy import RetryAction, RetryRule

    from airflow.providers.common.ai.policies.retry import LLMRetryPolicy

    llm_policy = LLMRetryPolicy(
        llm_conn_id="pydanticai_default",
        timeout=30.0,
        fallback_rules=[
            RetryRule(
                exception=ConnectionError, action=RetryAction.RETRY, retry_delay=timedelta(seconds=10)
            ),
            RetryRule(exception=PermissionError, action=RetryAction.FAIL),
        ],
    )
except ImportError:
    # RetryPolicy requires Airflow 3.3+
    llm_policy = None


if llm_policy is not None:

    @dag(catchup=False, tags=["example", "retry_policy", "llm"])
    def example_llm_retry_policy():
        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_auth_error():
            """LLM should classify as auth -> FAIL immediately."""
            raise PermissionError(
                "403 Forbidden: API key expired for service account analytics@proj.iam"
            )

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_rate_limit():
            """LLM should classify as rate_limit -> RETRY with ~60s delay."""
            raise RuntimeError("429 Too Many Requests: Rate limit exceeded. Retry after 60 seconds.")

        @task(retries=3, retry_delay=timedelta(minutes=1), retry_policy=llm_policy)
        def task_data_error():
            """LLM should classify as data -> FAIL immediately."""
            raise ValueError("Column 'user_id' expected type INT but got STRING in row 42.")

        task_auth_error()
        task_rate_limit()
        task_data_error()

    example_llm_retry_policy()
