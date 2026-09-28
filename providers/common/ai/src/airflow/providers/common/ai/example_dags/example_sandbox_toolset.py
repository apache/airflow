#
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
Example Dags for ``SandboxToolset``.

Three shapes, each a job a data team actually runs:

1. An agent investigates a revenue anomaly. It queries the warehouse through a
   ``SQLToolset`` (the credential stays in the task, the model only sees rows) and
   does the arithmetic in a Modal sandbox with pandas baked into the image, so a
   bad pivot or a runaway loop costs a 0.125-core sandbox rather than a worker slot.
2. An agent normalizes a vendor CSV whose columns drift every month, on the local
   ``sbx`` backend. Same toolset, same prompt, one constructor argument different,
   which is how the same Dag moves from a laptop to production.
3. A plain ``@task`` drives a backend directly to convert a file the Dag already
   knows how to convert. No model is involved. This is the shape for producing an
   artifact today, because a file inside an agent's sandbox can only leave through
   the model's context.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel

from airflow.providers.common.ai.operators.agent import AgentOperator
from airflow.providers.common.ai.sandbox import SandboxSpec, SbxSandboxBackend
from airflow.providers.common.ai.toolsets import SandboxToolset
from airflow.providers.common.compat.sdk import ObjectStoragePath, dag, task

try:
    from airflow.providers.common.ai.toolsets.sql import SQLToolset
except Exception:
    SQLToolset = None  # type: ignore[assignment,misc]

try:
    import modal

    from airflow.providers.common.ai.sandbox import ModalSandboxBackend
except Exception:
    modal = None  # type: ignore[assignment]
    ModalSandboxBackend = None  # type: ignore[assignment,misc]


# Pydantic output classes live at module scope so downstream tasks can re-import
# them when they deserialize the XCom payload.
class Findings(BaseModel):
    """What the investigation concluded, in a shape a downstream task can act on."""

    summary: str
    suspected_cause: str
    affected_segments: list[str]
    confidence: str


class ColumnMapping(BaseModel):
    """How the vendor's columns map onto the staging schema."""

    mapped: dict[str, str]
    unmapped: list[str]
    notes: str


# ---------------------------------------------------------------------------
# 1. Investigate a revenue anomaly: warehouse through a capability toolset,
#    arithmetic in a hosted sandbox.
# ---------------------------------------------------------------------------

if SQLToolset is not None and modal is not None:
    # [START howto_sandbox_agent_investigation]
    # Packages come from the image, so the sandbox needs no network at all. The
    # image is built by Modal the first time it is used and cached after that.
    ANALYSIS_IMAGE = modal.Image.from_registry("python:3.12-slim").pip_install("pandas")

    @dag(
        schedule=None,
        start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        tags=["example", "sandbox"],
    )
    def example_sandbox_agent_investigation():
        """Work out why daily revenue moved, with the warehouse and the workspace kept apart."""
        AgentOperator(
            task_id="investigate",
            prompt=(
                "Revenue for {{ ds }} moved more than 10% against the trailing seven-day "
                "average. Find out which region and channel explain the move, and how sure you are."
            ),
            system_prompt=(
                "You are a revenue analyst. Use the SQL tools to pull daily aggregates by region "
                "and channel for the two weeks up to and including the date in question. Write "
                "the rows into the sandbox as CSV and use pandas there for pivots and "
                "comparisons rather than doing arithmetic in your head. The sandbox has no "
                "network access and pandas is already installed; do not try to install anything. "
                "If a script fails, read the traceback, fix it and run it again."
            ),
            llm_conn_id="pydanticai_default",
            output_type=Findings,
            toolsets=[
                # The warehouse credential is held by the hook, in the task process. The
                # model calls named operations and sees rows; it never sees the password
                # and cannot pass it into the sandbox except by typing rows it was shown.
                SQLToolset(
                    db_conn_id="warehouse",
                    allowed_tables=["analytics.daily_revenue", "analytics.orders"],
                    max_rows=500,
                ),
                # Everything the model writes and runs happens here: off the worker, no
                # egress, nothing of Airflow's inside, destroyed when the run ends.
                SandboxToolset(
                    ModalSandboxBackend(image=ANALYSIS_IMAGE, cpu=1),
                    # The default spec already denies all egress and injects no
                    # environment. Stated here so the intent is visible in the Dag.
                    spec=SandboxSpec(block_network=True),
                ),
            ],
        )

    # [END howto_sandbox_agent_investigation]

    example_sandbox_agent_investigation()


# ---------------------------------------------------------------------------
# 2. Normalize a vendor file whose columns drift, on the local backend.
# ---------------------------------------------------------------------------

VENDOR_SAMPLE = """\
Order Ref,Customer,Order Dt,Amt (USD),Ship Country
A-1001,Acme Ltd,2026-03-01,1250.00,GB
A-1002,Globex,03/02/2026,980.50,US
A-1003,Initech,2 Mar 2026,"1,100.00",DE
"""


# [START howto_sandbox_agent_local]
@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "sandbox"],
)
def example_sandbox_agent_local():
    """Map a vendor's drifting columns onto the staging schema, developing against ``sbx``."""
    AgentOperator(
        task_id="map_columns",
        prompt=(
            "Here is a sample of this month's vendor export:\n\n"
            f"{VENDOR_SAMPLE}\n"
            "Our staging table has the columns order_id, customer_name, ordered_at (ISO "
            "date), amount_usd (decimal) and country_code. Write a Python script that reads "
            "the sample and produces a row for each order in the staging shape, run it, and "
            "fix it until every row parses. Then report the column mapping you settled on and "
            "anything you could not map."
        ),
        system_prompt=(
            "You have a sandbox with Python 3.12 and the standard library, no network, and "
            "an empty working directory. Write the sample to a file first, then iterate on a "
            "script until it runs cleanly. Read tracebacks and fix the code rather than "
            "guessing."
        ),
        llm_conn_id="pydanticai_default",
        output_type=ColumnMapping,
        toolsets=[
            # ``sbx`` runs a microVM on this machine and needs the ``sbx`` CLI, a Docker
            # login and KVM on Linux, which makes it a laptop and dev-host backend. Swap
            # it for ``ModalSandboxBackend()`` and nothing else in this Dag changes.
            SandboxToolset(SbxSandboxBackend(host_network_policy="deny-all")),
        ],
    )


# [END howto_sandbox_agent_local]

example_sandbox_agent_local()


# ---------------------------------------------------------------------------
# 3. Produce a file from a sandbox when the Dag already knows the job.
# ---------------------------------------------------------------------------

MIB = 1024 * 1024

CONVERT_SCRIPT = """\
import sys
import pandas as pd

frame = pd.read_csv(sys.argv[1])
frame.columns = [c.strip().lower().replace(" ", "_") for c in frame.columns]
frame = frame.drop_duplicates()
frame.to_parquet(sys.argv[2], index=False)
print(f"{len(frame)} rows, {len(frame.columns)} columns")
"""


@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["example", "sandbox"],
    params={
        # Point these at object storage (``s3://``, ``gs://``) in a real deployment.
        # ``file://`` keeps the example runnable on a laptop.
        "input_uri": "file:///tmp/airflow-sandbox-example/orders.csv",
        "output_uri": "file:///tmp/airflow-sandbox-example/orders.parquet",
    },
)
def example_sandbox_task_artifact():
    """
    Convert a CSV to parquet inside a sandbox and land the result in object storage.

    No agent is involved. The Dag knows exactly what to run, so a model would add
    nothing, and a file produced inside an *agent's* sandbox could only come back
    through the model's context, which is text-only and capped. Driving the
    backend from a task has neither limit: the bytes move through the worker, and
    the caller owns the sandbox's lifetime.
    """

    # [START howto_sandbox_task_artifact]
    @task
    def convert(input_uri: str, output_uri: str) -> str:
        # Task-only dependencies stay out of the Dag-parsing process.
        import modal

        from airflow.providers.common.ai.sandbox import ModalSandboxBackend, SandboxSpec

        source = ObjectStoragePath(input_uri).read_bytes()

        backend = ModalSandboxBackend(
            image=modal.Image.from_registry("python:3.12-slim").pip_install("pandas", "pyarrow"),
        )
        # No egress: the input arrives through write_file and the output leaves
        # through read_file, so the sandbox needs no network and no credentials.
        sandbox = backend.create(spec=SandboxSpec(block_network=True))
        try:
            backend.write_file(sandbox, "/workspace/orders.csv", source)
            backend.write_file(sandbox, "/workspace/convert.py", CONVERT_SCRIPT.encode())
            result = backend.run_command(
                sandbox,
                "python /workspace/convert.py /workspace/orders.csv /workspace/orders.parquet",
                timeout=300,
                max_output_bytes=64 * 1024,
            )
            if result.exit_code:
                raise RuntimeError(f"Conversion failed ({result.exit_code}): {result.stderr}")
            # The budget here is what the worker can hold, not a model's context.
            # Above it read_file raises SandboxFileTooLargeError rather than truncating.
            parquet = backend.read_file(sandbox, "/workspace/orders.parquet", max_bytes=256 * MIB)
        finally:
            # A toolset does this for you at the end of the run. Driving the backend
            # yourself means owning teardown, including when the block above raises.
            backend.destroy(sandbox)

        target = ObjectStoragePath(output_uri)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(parquet)
        # Hand the location downstream, not the bytes: XCom is the wrong place for
        # a file of the size this pattern exists to handle.
        return str(target)

    # [END howto_sandbox_task_artifact]

    convert("{{ params.input_uri }}", "{{ params.output_uri }}")


example_sandbox_task_artifact()
