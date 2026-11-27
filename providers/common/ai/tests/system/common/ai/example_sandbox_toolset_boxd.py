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
End-to-end system test for SandboxToolset with the boxd backend.

Needs ``BOXD_API_KEY`` in the worker environment.

Covers what only a live machine can show: that the four tools agree on one
filesystem, that a non-zero exit is output rather than a failure, that a command
hitting its deadline leaves the machine and its files in place, that the default
spec really does deny egress, and that teardown deletes the machine. A second task
provisions a machine itself, attaches two agent runs to it in turn, refuses a run
presenting the wrong owner, reads the agent's file out after the runs, and deletes
it.
"""

from __future__ import annotations

import os
import time
from datetime import UTC, datetime
from typing import Any

from airflow.providers.common.compat.sdk import dag as airflow_dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = f"common_ai_sandbox_toolset_boxd_{ENV_ID}" if ENV_ID else "common_ai_sandbox_toolset_boxd"

MARKER = "boundary-ok"
STATE_PATH = "state.txt"
ABSOLUTE_STATE_PATH = "/home/boxd/state.txt"


def _wait_until_deleted(machine_id: str) -> None:
    from boxd import Boxd, NotFoundError

    client = Boxd(api_key=os.environ["BOXD_API_KEY"])
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            client.machines.get(machine_id)
        except NotFoundError:
            return
        time.sleep(3)
    raise RuntimeError(f"Machine {machine_id} still exists 120s after the sandbox was destroyed")


@airflow_dag(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["common.ai", "sandbox", "boxd", "system_test"],
)
def example_sandbox_toolset_boxd():
    @task
    def run_sandbox_agent() -> str:
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import BoxdSandboxBackend
        from airflow.providers.common.ai.toolsets import SandboxToolset

        created: list[str] = []

        class RecordingBackend(BoxdSandboxBackend):
            def create(self, *, spec=None) -> str:
                created.append(super().create(spec=spec))
                return created[-1]

        steps: list[tuple[str, dict[str, Any], str]] = [
            ("write_file", {"path": STATE_PATH, "content": MARKER}, "Wrote"),
            ("run_command", {"command": f"cat {ABSOLUTE_STATE_PATH} && echo $((6 * 7))"}, "42"),
            ("read_file", {"path": ABSOLUTE_STATE_PATH}, MARKER),
            ("run_command", {"command": "echo to-stderr >&2; exit 3"}, "[exit code: 3]"),
            ("run_command", {"command": "echo started; sleep 60", "timeout_seconds": 2}, "timed out"),
            ("run_command", {"command": f"cat {STATE_PATH}"}, MARKER),
            ("run_command", {"command": "curl -sS -m 5 https://example.com || echo no-egress"}, "no-egress"),
            ("list_directory", {"path": "/home/boxd"}, STATE_PATH),
        ]

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            returns = [
                str(part.content)
                for message in messages
                for part in message.parts
                if part.part_kind in ("tool-return", "retry-prompt")
            ]
            for index, (tool, args, expected) in enumerate(steps):
                if len(returns) == index:
                    return ModelResponse(
                        parts=[ToolCallPart(tool_name=tool, args=args, tool_call_id=f"s{index}")]
                    )
                if expected not in returns[index]:
                    raise RuntimeError(f"Step {index} ({tool}) expected {expected!r} in {returns[index]!r}")
            return ModelResponse(parts=[TextPart(content="sandbox boundary e2e passed")])

        agent = Agent(
            FunctionModel(model_function),
            instructions="Use the sandbox tools as requested.",
            toolsets=[SandboxToolset(RecordingBackend(delete_after=900))],
        )
        result = agent.run_sync("Run the sandbox boundary system test.")
        if result.output != "sandbox boundary e2e passed":
            raise RuntimeError(f"Unexpected agent output: {result.output!r}")
        if len(created) != 1:
            raise RuntimeError(f"Expected the run to use one machine, it created {len(created)}")
        _wait_until_deleted(created[0])
        return result.output

    @task
    def run_attached_sandbox_agent(**context) -> str:
        """A task-owned machine: provisioned here, used by two agent runs, collected here."""
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import BoxdSandboxBackend, SandboxSpec, dag_run_owner
        from airflow.providers.common.ai.sandbox.base import HOLDER_TAG
        from airflow.providers.common.ai.toolsets import SandboxToolset

        injected = "injected-by-provision"
        report = "from-run-1.txt"

        def scripted(steps: list[tuple[str, str]]) -> FunctionModel:
            def model_function(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
                run_command = next(t for t in info.function_tools if t.name == "run_command")
                description = run_command.description or ""
                for expected in ("set up by an earlier task", "of its lifetime", "NO network access"):
                    if expected not in description:
                        raise RuntimeError(f"The tool description lacks {expected!r}: {description!r}")
                returns = [
                    str(part.content)
                    for message in messages
                    for part in message.parts
                    if part.part_kind in ("tool-return", "retry-prompt")
                ]
                for index, (command, expected) in enumerate(steps):
                    if len(returns) == index:
                        return ModelResponse(
                            parts=[
                                ToolCallPart(
                                    tool_name="run_command",
                                    args={"command": command},
                                    tool_call_id=f"c{index}",
                                )
                            ]
                        )
                    if expected not in returns[index]:
                        raise RuntimeError(f"Step {index} expected {expected!r} in {returns[index]!r}")
                return ModelResponse(parts=[TextPart(content="attached e2e passed")])

            return FunctionModel(model_function)

        def attached_agent(model: FunctionModel, **toolset_kwargs) -> Agent:
            return Agent(
                model,
                instructions="Use the sandbox tools as requested.",
                toolsets=[SandboxToolset(BoxdSandboxBackend(), max_command_timeout=60.0, **toolset_kwargs)],
            )

        backend = BoxdSandboxBackend(delete_after=900)
        sandbox = backend.create(spec=SandboxSpec(env={"INJECTED": injected}, owner=dag_run_owner(context)))
        try:
            first = attached_agent(
                scripted([(f"echo $INJECTED | tee {report}", injected)]), attach_to=sandbox
            ).run_sync("Record the injected value.")
            if first.output != "attached e2e passed":
                raise RuntimeError(f"Unexpected first output: {first.output!r}")
            tags = backend.read_tags(sandbox)
            if HOLDER_TAG in tags:
                raise RuntimeError(f"The run ended but the machine is still marked held: {tags}")

            second = attached_agent(scripted([(f"cat {report}", injected)]), attach_to=sandbox).run_sync(
                "Read the file the previous run wrote."
            )
            if second.output != "attached e2e passed":
                raise RuntimeError(f"Unexpected second output: {second.output!r}")

            try:
                attached_agent(scripted([("id", "uid")]), attach_to=sandbox, owner="someone-else").run_sync(
                    "Try."
                )
            except Exception as e:
                if "not owned by 'someone-else'" not in str(e):
                    raise RuntimeError(f"Expected an ownership refusal, got: {e!r}") from e
            else:
                raise RuntimeError("A run with the wrong owner attached to the machine")

            collected = backend.read_file(sandbox, report, max_bytes=1024).decode().strip()
            if collected != injected:
                raise RuntimeError(f"Collected {collected!r}, expected {injected!r}")
        finally:
            backend.destroy(sandbox)
        _wait_until_deleted(sandbox)
        return "attached e2e passed"

    run_sandbox_agent()
    run_attached_sandbox_agent()


dag = example_sandbox_toolset_boxd()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
