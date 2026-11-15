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
"""Example Dag: LLM output that a person approves, edits or rejects before a downstream task uses it."""

from __future__ import annotations

# [START howto_operator_llm_approval_walkthrough]
from datetime import timedelta

from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.sdk import dag, task

CHANGE = """
Webhook deliveries that fail with a 5xx response are now retried three times
with exponential backoff before the delivery is logged as failed.
Previously a single failed delivery dropped the event.
"""


@dag(schedule=None, tags=["example"])
def example_llm_approval():
    draft = LLMOperator(
        task_id="draft_release_note",
        prompt=f"Write a one-sentence release note for this change:\n{CHANGE}",
        llm_conn_id="pydanticai_default",
        system_prompt="You write release notes for the users of a developer platform. Be plain and specific.",
        require_approval=True,
        allow_modifications=True,
        approval_timeout=timedelta(hours=24),
    )

    @task
    def publish(note: str) -> None:
        print(f"Publishing release note: {note}")

    publish(draft.output)


example_llm_approval()
# [END howto_operator_llm_approval_walkthrough]
