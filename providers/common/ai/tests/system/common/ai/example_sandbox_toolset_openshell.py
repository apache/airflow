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
"""End-to-end system test for SandboxToolset with NVIDIA OpenShell."""

from __future__ import annotations

import os
from datetime import UTC, datetime

from airflow.providers.common.compat.sdk import dag as airflow_dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = f"common_ai_sandbox_toolset_openshell_{ENV_ID}" if ENV_ID else "common_ai_sandbox_toolset_openshell"

MARKER = "boundary-ok"
STATE_PATH = "/tmp/airflow_sandbox_e2e"
# Counts processes whose command line is exactly "sleep 300", without needing ps in the image.
COUNT_SLEEPERS = (
    "n=0; for f in /proc/[0-9]*/cmdline; do "
    "[ \"$(tr '\\0' ' ' < \"$f\" 2>/dev/null)\" = 'sleep 300 ' ] && n=$((n + 1)); done; echo sleepers=$n"
)
CONNECT = (
    'python3 -c "import socket\n'
    "try:\n    socket.create_connection(('1.1.1.1', 443), 5)\n    print('egress=open')\n"
    "except OSError as e:\n    print('egress=denied', e.errno)\""
)


@airflow_dag(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["common.ai", "sandbox", "openshell", "system_test"],
)
def example_sandbox_toolset_openshell():
    @task
    def run_sandbox_agent() -> str:
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import OpenShellSandboxBackend
        from airflow.providers.common.ai.toolsets import SandboxToolset

        # Each step is a tool call and a check of what the previous one returned.
        steps: list[tuple[str, dict, str]] = [
            ("write_file", {"path": STATE_PATH, "content": MARKER}, "Wrote"),
            ("run_command", {"command": f"cat {STATE_PATH} && python3 -c 'print(6 * 7)'"}, "42"),
            ("read_file", {"path": STATE_PATH}, MARKER),
            ("run_command", {"command": "echo to-stderr >&2; exit 3"}, "[exit code: 3]"),
            ("list_directory", {"path": "/tmp"}, "airflow_sandbox_e2e"),
            (
                "run_command",
                {"command": "sleep 300 & sleep 300", "timeout_seconds": 3},
                "[timed out after 3s]",
            ),
            ("run_command", {"command": COUNT_SLEEPERS}, "sleepers=0"),
            ("run_command", {"command": CONNECT}, "egress=denied"),
        ]

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            returns = [
                part.content
                for message in messages
                for part in message.parts
                if part.part_kind == "tool-return"
            ]
            for (_, _, expected), returned in zip(steps, returns):
                if expected not in str(returned):
                    raise RuntimeError(f"Expected {expected!r} in {returned!r}")
            if len(returns) < len(steps):
                tool_name, args, _ = steps[len(returns)]
                return ModelResponse(
                    parts=[ToolCallPart(tool_name=tool_name, args=args, tool_call_id=f"step-{len(returns)}")]
                )
            return ModelResponse(parts=[TextPart(content="sandbox boundary e2e passed")])

        agent = Agent(
            FunctionModel(model_function),
            instructions="Use the sandbox tools as requested.",
            toolsets=[
                SandboxToolset(
                    OpenShellSandboxBackend(), default_command_timeout=30.0, max_command_timeout=30.0
                )
            ],
        )
        result = agent.run_sync("Run the sandbox boundary system test.")
        if result.output != "sandbox boundary e2e passed":
            raise RuntimeError(f"Unexpected agent output: {result.output!r}")
        return result.output

    run_sandbox_agent()


dag = example_sandbox_toolset_openshell()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

test_run = get_test_run(dag)
