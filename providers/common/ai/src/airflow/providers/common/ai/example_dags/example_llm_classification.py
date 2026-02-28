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
"""Example DAG: classify pipeline incidents by severity using @task.llm with Literal output."""

from __future__ import annotations

from typing import Literal

from airflow.providers.common.compat.sdk import dag, task


# [START howto_decorator_llm_classification]
@dag
def example_llm_classification():
    @task.llm(
        llm_conn_id="pydantic_ai_default",
        system_prompt=(
            "Classify the severity of the given pipeline incident. "
            "Use 'critical' for data loss or complete pipeline failure, "
            "'high' for significant delays or partial failures, "
            "'medium' for degraded performance, "
            "'low' for cosmetic issues or minor warnings."
        ),
        output_type=Literal["critical", "high", "medium", "low"],
    )
    def classify_incident(description: str):
        # Pre-process the description before sending to the LLM
        return f"Classify this incident:\n{description.strip()}"

    classify_incident(
        "Scheduler heartbeat lost for 15 minutes. "
        "Multiple DAG runs stuck in queued state. "
        "No new tasks being scheduled across all DAGs."
    )


# [END howto_decorator_llm_classification]

example_llm_classification()
