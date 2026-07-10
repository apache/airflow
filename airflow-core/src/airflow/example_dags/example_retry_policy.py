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
Example DAG demonstrating pluggable retry policies.

A retry policy evaluates the actual exception at failure time and decides
whether to retry (with a custom delay), fail immediately, or fall through
to the standard retry logic.
"""

from __future__ import annotations

from datetime import timedelta

# [START retry_policy_definition]
from airflow.sdk import DAG, ExceptionRetryPolicy, RetryAction, RetryRule, task

API_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(
            exception="requests.exceptions.HTTPError",
            action=RetryAction.RETRY,
            retry_delay=timedelta(minutes=5),
            reason="Rate limit, backing off",
        ),
        RetryRule(
            exception="google.auth.exceptions.RefreshError",
            action=RetryAction.FAIL,
            reason="Auth failure, not retryable",
        ),
        RetryRule(
            exception=ConnectionError,
            action=RetryAction.RETRY,
            retry_delay=timedelta(seconds=30),
        ),
    ],
)
# [END retry_policy_definition]

# [START retry_policy_on_task]
with DAG(
    dag_id="example_retry_policy",
    schedule=None,
    catchup=False,
    tags=["example", "retry_policy"],
):

    @task(retries=5, retry_delay=timedelta(minutes=1), retry_policy=API_RETRY_POLICY)
    def call_external_api():
        import requests

        response = requests.get("https://api.example.com/data")
        response.raise_for_status()
        return response.json()

    call_external_api()
# [END retry_policy_on_task]

# [START retry_policy_reusable]
# policies.py -- import in any DAG
STANDARD_RETRY_POLICY = ExceptionRetryPolicy(
    rules=[
        RetryRule(exception="requests.exceptions.HTTPError", action=RetryAction.FAIL),
        RetryRule(exception=ConnectionError, retry_delay=timedelta(seconds=10)),
    ],
)
# [END retry_policy_reusable]
