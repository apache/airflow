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
"""End-to-end system test for SandboxToolset with Docker Sandboxes."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from airflow.providers.common.compat.sdk import dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = f"common_ai_sandbox_toolset_sbx_{ENV_ID}" if ENV_ID else "common_ai_sandbox_toolset_sbx"


@dag(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["common.ai", "sandbox", "sbx", "system_test"],
)
def example_sandbox_toolset_sbx():
    @task
    def run_sandbox_agent() -> str:
        # Keep task-only dependencies out of the Dag-parsing process.
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import SbxSandboxBackend
        from airflow.providers.common.ai.toolsets import SandboxToolset

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            tool_returns = [
                part.content
                for message in messages
                for part in message.parts
                if part.part_kind == "tool-return"
            ]
            if not tool_returns:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="run_python_in_sandbox",
                            args={
                                "code": (
                                    "from pathlib import Path; "
                                    "Path('/tmp/airflow_sandbox_e2e').write_text('boundary-ok'); "
                                    "print(6 * 7)"
                                )
                            },
                            tool_call_id="write-state",
                        )
                    ]
                )

            first_result = json.loads(tool_returns[0])
            if first_result != {"exit_code": 0, "stdout": "42\n", "stderr": "", "timed_out": False}:
                raise RuntimeError(f"Unexpected first sandbox result: {first_result!r}")
            if len(tool_returns) == 1:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="run_python_in_sandbox",
                            args={
                                "code": (
                                    "from pathlib import Path; "
                                    "print(Path('/tmp/airflow_sandbox_e2e').read_text())"
                                )
                            },
                            tool_call_id="read-state",
                        )
                    ]
                )

            second_result = json.loads(tool_returns[1])
            if second_result != {
                "exit_code": 0,
                "stdout": "boundary-ok\n",
                "stderr": "",
                "timed_out": False,
            }:
                raise RuntimeError(f"Unexpected second sandbox result: {second_result!r}")
            return ModelResponse(parts=[TextPart(content="sandbox boundary e2e passed")])

        agent = Agent(
            FunctionModel(model_function),
            instructions="Use the sandbox tool as requested.",
            toolsets=[SandboxToolset(SbxSandboxBackend(), timeout=30.0)],
        )
        result = agent.run_sync("Run the sandbox boundary system test.")
        if result.output != "sandbox boundary e2e passed":
            raise RuntimeError(f"Unexpected agent output: {result.output!r}")
        return result.output

    run_sandbox_agent()


dag = example_sandbox_toolset_sbx()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
