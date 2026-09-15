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
Run code in a sandbox from a task, rather than handing the sandbox to an agent.

``SandboxToolset`` is the right shape when a *model* decides what to run, turn by
turn. It is the wrong shape when the Dag already knows what to run and needs the
result, because a toolset returns only what fits through the model's context, and
its ``SandboxSpec`` is built while the Dag file is being parsed.

Driving a backend from a ``@task`` removes both limits. The credential is read
from a connection at run time instead of being written into the Dag file, and the
artifact comes back through the worker instead of through a model.
"""

from __future__ import annotations

from datetime import datetime, timezone

from airflow.providers.common.compat.sdk import ObjectStoragePath, dag, task

SCRIPT = """
import json, platform

# Stands in for real work. What it produces is written to a file rather than
# printed, because printing is what forces a result through a context window.
report = {"python": platform.python_version()}
with open("/workspace/report.json", "w") as handle:
    json.dump(report, handle)
print("wrote /workspace/report.json")
"""

MIB = 1024 * 1024


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "sandbox"],
)
def example_sandbox_artifact():
    """Produce a file inside a sandbox and land it where a downstream task can read it."""

    # [START howto_sandbox_artifact_task]
    @task
    def build_report() -> str:
        # Task-only dependencies stay out of the Dag-parsing process.
        from airflow.providers.common.ai.sandbox import ModalSandboxBackend, SandboxSpec
        from airflow.providers.common.compat.sdk import BaseHook

        # Resolved at run time, so the credential lives in the connection rather
        # than in the Dag file. A toolset cannot do this: its spec is built
        # during parsing, before any connection is available.
        token = BaseHook.get_connection("my_api").password
        if token is None:
            # Fail here rather than handing the sandbox an empty credential and
            # letting the job fail somewhere less obvious.
            raise ValueError("Connection 'my_api' has no password set.")

        backend = ModalSandboxBackend()
        sandbox = backend.create(
            spec=SandboxSpec(
                env={"API_TOKEN": token},
                # Anything here is readable by everything in the sandbox, so give
                # it only what this one job needs.
                block_network=True,
            )
        )
        try:
            backend.write_file(sandbox, "/workspace/job.py", SCRIPT.encode())
            result = backend.run_command(
                sandbox,
                "python /workspace/job.py",
                timeout=120,
                max_output_bytes=64 * 1024,
            )
            if result.exit_code:
                raise RuntimeError(f"Sandbox job failed ({result.exit_code}): {result.stderr}")

            # Read the artifact back. No context window is involved, so the cap
            # is whatever the worker can afford to hold rather than a model's
            # budget. Above it, read_file raises SandboxFileTooLargeError.
            raw = backend.read_file(sandbox, "/workspace/report.json", max_bytes=8 * MIB)
        finally:
            # The toolset does this for you. Driving the backend yourself means
            # owning teardown, including when the block above raises.
            backend.destroy(sandbox)

        # Land the bytes somewhere durable and hand on the location. Returning the
        # artifact itself would push it through XCom, which stops working at the
        # sizes this pattern exists to serve.
        target = ObjectStoragePath("file:///tmp/airflow-sandbox-demo/report.json")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        return str(target)

    # [END howto_sandbox_artifact_task]

    build_report()


example_sandbox_artifact()
