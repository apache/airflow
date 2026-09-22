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
"""End-to-end system test for SandboxToolset with Islo."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from airflow.providers.common.compat.sdk import dag as airflow_dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = f"common_ai_sandbox_toolset_islo_{ENV_ID}" if ENV_ID else "common_ai_sandbox_toolset_islo"

MARKER = "boundary-ok"
STATE_PATH = "/tmp/airflow_sandbox_e2e"


@airflow_dag(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["common.ai", "sandbox", "islo", "system_test"],
)
def example_sandbox_toolset_islo():
    @task
    def run_sandbox_agent() -> str:
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import IsloSandboxBackend
        from airflow.providers.common.ai.toolsets import SandboxToolset

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            returns = [
                part.content
                for message in messages
                for part in message.parts
                if part.part_kind == "tool-return"
            ]

            if not returns:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="write_file",
                            args={"path": STATE_PATH, "content": MARKER},
                            tool_call_id="write",
                        )
                    ]
                )
            if "Wrote" not in str(returns[0]):
                raise RuntimeError(f"Unexpected write_file result: {returns[0]!r}")

            if len(returns) == 1:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="run_command",
                            args={"command": f"cat {STATE_PATH} && echo $((6 * 7))"},
                            tool_call_id="shell",
                        )
                    ]
                )
            shell_out = str(returns[1])
            if MARKER not in shell_out or "42" not in shell_out:
                raise RuntimeError(f"Unexpected run_command result: {shell_out!r}")

            if len(returns) == 2:
                return ModelResponse(
                    parts=[
                        ToolCallPart(tool_name="read_file", args={"path": STATE_PATH}, tool_call_id="read")
                    ]
                )
            if MARKER not in str(returns[2]):
                raise RuntimeError(f"Unexpected read_file result: {returns[2]!r}")

            if len(returns) == 3:
                return ModelResponse(
                    parts=[
                        ToolCallPart(
                            tool_name="run_command",
                            args={"command": "echo to-stderr >&2; exit 3"},
                            tool_call_id="fail",
                        )
                    ]
                )
            failed = str(returns[3])
            if "[exit code: 3]" not in failed or "to-stderr" not in failed:
                raise RuntimeError(f"Unexpected failure result: {failed!r}")

            if len(returns) == 4:
                return ModelResponse(
                    parts=[ToolCallPart(tool_name="list_directory", args={"path": "/tmp"}, tool_call_id="ls")]
                )
            if "airflow_sandbox_e2e" not in str(returns[4]):
                raise RuntimeError(f"Unexpected listing: {returns[4]!r}")

            return ModelResponse(parts=[TextPart(content="sandbox boundary e2e passed")])

        agent = Agent(
            FunctionModel(model_function),
            instructions="Use the sandbox tools as requested.",
            toolsets=[SandboxToolset(IsloSandboxBackend(islo_conn_id=None, delete_after=900))],
        )
        result = agent.run_sync("Run the sandbox boundary system test.")
        if result.output != "sandbox boundary e2e passed":
            raise RuntimeError(f"Unexpected agent output: {result.output!r}")
        return result.output

    run_sandbox_agent()


dag = example_sandbox_toolset_islo()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
