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
"""Example DAGs demonstrating input_guard variants and sanitized-preview logging."""

from __future__ import annotations

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.operators.llm import LLMOperator
from airflow.providers.common.ai.operators.llm_file_analysis import LLMFileAnalysisOperator
from airflow.providers.common.compat.sdk import dag

_PROMPT_WITH_PII = (
    "Customer Jane Doe can be reached at jane.doe@example.com, phone 555-123-4567, "
    "card 4111 1111 1111 1111, and SSN 123-45-6789. Summarize the support issue without "
    "exposing sensitive values."
)

_SYSTEM_PROMPT_WITH_PII = (
    "You are a support analyst. Internal escalation contact is agent.owner@example.com. "
    "Keep the response concise."
)

_BASE_INPUT_GUARD = {
    "enabled": True,
    "entities": ("EMAIL_ADDRESS", "PHONE_NUMBER", "CREDIT_CARD", "US_SSN"),
    "log_sanitized_text": True,
    "log_preview_chars": 240,
}


# [START howto_operator_llm_input_guard_modes]
@dag
def example_llm_input_guard_modes():
    """Run the same prompt with each Presidio anonymization mode."""

    LLMOperator(
        task_id="replace_sensitive_values",
        prompt=_PROMPT_WITH_PII,
        llm_conn_id="pydanticai_default",
        system_prompt=_SYSTEM_PROMPT_WITH_PII,
        input_guard={**_BASE_INPUT_GUARD, "mode": "replace"},
    )

    LLMOperator(
        task_id="mask_sensitive_values",
        prompt=_PROMPT_WITH_PII,
        llm_conn_id="pydanticai_default",
        system_prompt=_SYSTEM_PROMPT_WITH_PII,
        input_guard={**_BASE_INPUT_GUARD, "mode": "mask"},
    )

    LLMOperator(
        task_id="redact_sensitive_values",
        prompt=_PROMPT_WITH_PII,
        llm_conn_id="pydanticai_default",
        system_prompt=_SYSTEM_PROMPT_WITH_PII,
        input_guard={**_BASE_INPUT_GUARD, "mode": "redact"},
    )

    LLMOperator(
        task_id="hash_sensitive_values",
        prompt=_PROMPT_WITH_PII,
        llm_conn_id="pydanticai_default",
        system_prompt=_SYSTEM_PROMPT_WITH_PII,
        input_guard={**_BASE_INPUT_GUARD, "mode": "hash"},
    )


# [END howto_operator_llm_input_guard_modes]

example_llm_input_guard_modes()


# [START howto_operator_agent_input_guard]
@dag
def example_agent_input_guard():
    """AgentOperator example showing sanitized previews in the task log."""

    AgentOperator(
        task_id="support_agent_with_input_guard",
        prompt=_PROMPT_WITH_PII,
        llm_conn_id="pydanticai_default",
        system_prompt=_SYSTEM_PROMPT_WITH_PII,
        input_guard={**_BASE_INPUT_GUARD, "mode": "replace"},
    )


# [END howto_operator_agent_input_guard]

example_agent_input_guard()


# [START howto_operator_llm_file_analysis_input_guard]
@dag
def example_llm_file_analysis_input_guard():
    """File-analysis example that sanitizes prompt text and normalized file text."""

    LLMFileAnalysisOperator(
        task_id="analyze_masked_ticket_export",
        prompt=(
            "Review this support export. A customer message may mention jane.doe@example.com, "
            "555-123-4567, and 4111 1111 1111 1111. Summarize the findings safely."
        ),
        llm_conn_id="pydanticai_default",
        file_path="s3://support/tickets/2024-01-15/",
        file_conn_id="aws_default",
        input_guard={**_BASE_INPUT_GUARD, "mode": "replace"},
    )


# [END howto_operator_llm_file_analysis_input_guard]

example_llm_file_analysis_input_guard()
