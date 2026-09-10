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
End-to-end system test for SandboxToolset with the Modal backend.

Needs Modal credentials on the worker: ``modal token new`` locally, or
``MODAL_TOKEN_ID`` and ``MODAL_TOKEN_SECRET`` in the environment.

Covers what only a live sandbox can show: that the four tools agree on one
filesystem, that a non-zero exit is output rather than a failure, that a command
hitting its deadline leaves the sandbox and its files intact (which is where Modal
differs from ``sbx``), that the default spec really does deny egress, that none of
Airflow's own environment crosses the boundary, and that teardown terminates the
sandbox.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

from airflow.providers.common.compat.sdk import dag as airflow_dag, task

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = f"common_ai_sandbox_toolset_modal_{ENV_ID}" if ENV_ID else "common_ai_sandbox_toolset_modal"

MARKER = "boundary-ok"
STATE_PATH = "state.txt"
ABSOLUTE_STATE_PATH = "/workspace/state.txt"


@airflow_dag(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["common.ai", "sandbox", "modal", "system_test"],
)
def example_sandbox_toolset_modal():
    @task
    def run_sandbox_agent() -> str:
        # Keep task-only dependencies out of the Dag-parsing process.
        from pydantic_ai import Agent
        from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
        from pydantic_ai.models.function import AgentInfo, FunctionModel

        from airflow.providers.common.ai.sandbox import ModalSandboxBackend
        from airflow.providers.common.ai.toolsets import SandboxToolset

        class RecordingBackend(ModalSandboxBackend):
            """Remembers the handles it created and destroyed, so teardown can be asserted."""

            def __init__(self, **kwargs) -> None:
                super().__init__(**kwargs)
                self.created: list[str] = []
                self.destroyed: list[str] = []

            def create(self, *, spec=None) -> str:
                handle = super().create(spec=spec)
                self.created.append(handle)
                return handle

            def destroy(self, sandbox: str) -> None:
                super().destroy(sandbox)
                self.destroyed.append(sandbox)

        def call(tool: str, call_id: str, **args) -> ModelResponse:
            return ModelResponse(parts=[ToolCallPart(tool_name=tool, args=args, tool_call_id=call_id)])

        def model_function(messages: list[ModelMessage], _info: AgentInfo) -> ModelResponse:
            # Both kinds count: a tool that raises ModelRetry -- which is how the toolset
            # reports a recoverable failure -- comes back as a retry-prompt part rather
            # than a tool-return, and counting only the latter would re-issue the same
            # call until the tool exhausted its retries.
            returns = [
                str(part.content)
                for message in messages
                for part in message.parts
                if part.part_kind in ("tool-return", "retry-prompt")
            ]

            # 1. Write through the file tool, with a relative path: the native API takes
            #    absolute paths only, so this also proves the workdir resolution.
            if not returns:
                return call("write_file", "write", path=STATE_PATH, content=MARKER)
            if "Wrote" not in str(returns[0]):
                raise RuntimeError(f"Unexpected write_file result: {returns[0]!r}")

            # 2. The shell sees the same file at the absolute path, and the image carries
            #    a real interpreter.
            if len(returns) == 1:
                return call(
                    "run_command",
                    "shell",
                    command=f"cat {ABSOLUTE_STATE_PATH} && python3 -c 'print(6 * 7)'",
                )
            shell_out = str(returns[1])
            if MARKER not in shell_out or "42" not in shell_out:
                raise RuntimeError(f"Unexpected run_command result: {shell_out!r}")

            # 3. And the file tool reads back what the shell just saw.
            if len(returns) == 2:
                return call("read_file", "read", path=STATE_PATH)
            if MARKER not in str(returns[2]):
                raise RuntimeError(f"Unexpected read_file result: {returns[2]!r}")

            # 4. A non-zero exit is output to react to, not an exception.
            if len(returns) == 3:
                return call("run_command", "fail", command="echo to-stderr >&2; exit 3")
            failed = str(returns[3])
            if "[exit code: 3]" not in failed or "to-stderr" not in failed:
                raise RuntimeError(f"Unexpected failure result: {failed!r}")

            # 5. A deadline is reported, and Modal does not have to destroy the sandbox
            #    to enforce it, so the toolset must not say the sandbox was replaced.
            if len(returns) == 4:
                return call("run_command", "slow", command="sleep 30", timeout_seconds=2)
            timed_out = str(returns[4])
            if "[timed out after 2s]" not in timed_out:
                raise RuntimeError(f"Expected a timeout, got: {timed_out!r}")
            if "sandbox was replaced" in timed_out:
                raise RuntimeError("Modal should survive a command timeout, but the sandbox was replaced")

            # 6. Which means the file from step 1 is still there afterwards.
            if len(returns) == 5:
                return call("run_command", "survived", command=f"cat {ABSOLUTE_STATE_PATH}")
            if MARKER not in str(returns[5]):
                raise RuntimeError(f"Files did not survive the timeout: {returns[5]!r}")

            # 7. Airflow puts none of its own environment into the sandbox.
            if len(returns) == 6:
                return call(
                    "run_command",
                    "env",
                    command="python3 -c \"import os; print(sorted(k for k in os.environ if 'AIRFLOW' in k))\"",
                )
            leaked = str(returns[6])
            if "[]" not in leaked:
                raise RuntimeError(f"Airflow environment crossed the boundary: {leaked!r}")

            # 8. The default spec denies egress, and Modal's block_network takes DNS with it.
            if len(returns) == 7:
                return call(
                    "run_command",
                    "egress",
                    command="python3 -c \"import socket; print(socket.gethostbyname('pypi.org'))\"",
                )
            egress = str(returns[7])
            if "[exit code:" not in egress:
                raise RuntimeError(f"Egress should have been denied, got: {egress!r}")

            # 9. The listing sees the file too, and marks a directory as one.
            if len(returns) == 8:
                return call("run_command", "mkdir", command="mkdir -p /workspace/sub")
            if len(returns) == 9:
                return call("list_directory", "ls", path="/workspace")
            listing = str(returns[9])
            if "state.txt" not in listing or "sub/" not in listing:
                raise RuntimeError(f"Unexpected listing: {listing!r}")

            # 10. The documented symlink difference: a native write replaces a link rather
            #     than following it, where a shell redirect would write through it. Only a
            #     live sandbox can show which one this backend does.
            if len(returns) == 10:
                return call(
                    "run_command",
                    "link",
                    command="printf original > /workspace/target.txt && ln -s /workspace/target.txt /workspace/link.txt",
                )
            if len(returns) == 11:
                return call("write_file", "through-link", path="link.txt", content="written-through-link")
            if len(returns) == 12:
                return call(
                    "run_command",
                    "link-check",
                    command="readlink /workspace/link.txt || echo NOT-A-LINK; cat /workspace/target.txt",
                )
            link_state = str(returns[12])
            if "NOT-A-LINK" not in link_state or "original" not in link_state:
                raise RuntimeError(
                    f"write_file should replace a symlink and leave its target alone: {link_state!r}"
                )

            # 11. And the reason read_file is not on the native API: a file that streams
            #     without end must be refused by the in-guest cap, not pulled into the
            #     worker. /dev/zero exists only in a real sandbox.
            if len(returns) == 13:
                return call("read_file", "streaming", path="/dev/zero")
            streaming = str(returns[13])
            if "read limit" not in streaming:
                raise RuntimeError(f"/dev/zero should be refused by the read budget: {streaming!r}")

            return ModelResponse(parts=[TextPart(content="sandbox boundary e2e passed")])

        # The default spec denies all egress, which Modal enforces exactly.
        backend = RecordingBackend(app_name="airflow-sandbox-system-test", sandbox_timeout=900)
        agent = Agent(
            FunctionModel(model_function),
            instructions="Use the sandbox tools as requested.",
            toolsets=[SandboxToolset(backend, max_command_timeout=60.0)],
        )
        result = agent.run_sync("Run the sandbox boundary system test.")
        if result.output != "sandbox boundary e2e passed":
            raise RuntimeError(f"Unexpected agent output: {result.output!r}")

        # Teardown: the toolset destroys the sandbox when the run ends. Modal's
        # termination is asynchronous, and an exit status can take some seconds to
        # show, so poll rather than assuming either way.
        if backend.created != backend.destroyed:
            raise RuntimeError(f"created {backend.created} but destroyed {backend.destroyed}")
        import modal

        handle = backend.created[0]
        # Termination showed an exit status 32-33s after the request when measured, so this
        # is a little over three times the observed value rather than under twice it.
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if modal.Sandbox.from_id(handle).poll() is not None:
                break
            time.sleep(5)
        else:
            raise RuntimeError(f"Sandbox {handle} was still running 60s after teardown")

        return result.output

    run_sandbox_agent()


dag = example_sandbox_toolset_modal()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
