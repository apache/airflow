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
"""Example DAG demonstrating multi-type Airflow Params.

Airflow supports parameters with more than one allowed type by passing a list
to the ``type`` field, e.g. ``type=["integer", "string"]``.  The trigger UI
renders a plain textarea for each such param so you can enter either a simple
scalar value or a structured JSON value.  The stored Python type is resolved
automatically at input time:

* If the input parses as valid JSON **and** the parsed type matches one of the
  declared schema types, the parsed value is stored (integer, boolean, object …).
* Otherwise, if ``"string"`` is one of the declared types, the raw text is stored
  as a string.
* If neither condition is met (e.g. ``type=["integer", "object"]`` and you type
  ``hello``), the field shows a validation error and the form cannot be submitted.

Try the following in the trigger form to see type resolution in action:

**batch_size** (``["integer", "string"]``)
  * Enter ``500`` → stored as integer ``500``
  * Enter ``all`` → stored as string ``"all"``

**notify** (``["boolean", "string"]``)
  * Enter ``true`` or ``false`` → stored as boolean
  * Enter ``ops@example.com`` → stored as string (custom recipient)

**pipeline** (``["string", "object"]``)
  * Enter ``nightly-export`` → stored as string shorthand
  * Paste ``{"name": "nightly-export", "retries": 3}`` → stored as dict

**destination** (``["string", "object"]``)
  * Enter ``reports.summary`` → stored as string (schema.table)
  * Paste ``{"connection": "pg_prod", "schema": "reports", "table": "summary"}``
    → stored as dict with explicit connection routing
"""

from __future__ import annotations

import json
from datetime import datetime

from airflow.sdk import DAG, Param, task

with DAG(
    dag_id="example_params_multi_type_tutorial",
    dag_display_name="Multi-type Params tutorial",
    description="Demonstrates multi-type Params where a field can hold a scalar value or a JSON object.",
    doc_md=__doc__,
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "params", "multi-type"],
    params={
        "run_label": Param(
            "nightly",
            type="string",
            title="Run label",
            description="Short human-readable label for this run (e.g. 'nightly', 'backfill-2025-01').",
            section="Run settings",
        ),
        # ── Multi-type params ──────────────────────────────────────────────
        # Each param below accepts two or more types.  The trigger UI renders
        # a textarea instead of a single-type widget so you can enter any
        # compatible value.
        "dry_run": Param(
            False,
            type=["boolean", "object"],
            title="Dry run",
            description="When enabled, the pipeline validates inputs and logs all actions but skips writes. "
            "You can provide boolean value or complete config json",
            section="Run settings",
        ),
        "batch_size": Param(
            1000,
            type=["integer", "string"],
            title="Batch size",
            description_md=(
                "Number of rows to process per batch (stored as an integer), **or** the string "
                "``all`` to process the entire dataset in one pass.\n\n"
                "Examples: `500` → integer; `all` → string."
            ),
            section="Multi-type params",
        ),
        "notify": Param(
            True,
            type=["boolean", "string"],
            title="Notification target",
            description=(
                "Set to true to notify the on-call team alias, false to suppress all notifications, "
                "or enter a specific email address to override the default recipient."
            ),
            section="Multi-type params",
        ),
        "pipeline": Param(
            {
                "name": "nightly-export",
                "retries": 2,
                "timeout_minutes": 30,
                "source": {"type": "s3", "bucket": "raw-data", "prefix": "events/"},
            },
            type=["string", "object"],
            title="Pipeline",
            description=(
                "Pipeline shorthand name (e.g. 'nightly-export'), or a full JSON configuration "
                "object.  When a dict is supplied, 'name', 'retries', and 'timeout_minutes' "
                "are read directly from it."
            ),
            section="Multi-type params",
        ),
        "destination": Param(
            "reports.summary",
            type=["string", "object"],
            title="Destination",
            description=(
                "Target table in 'schema.table' notation, or a JSON object with 'connection', "
                "'schema', and 'table' keys for explicit connection routing."
            ),
            section="Multi-type params",
        ),
    },
) as dag:

    @task(task_display_name="Log resolved param types")
    def log_params(**kwargs) -> None:
        """Print each param value with its resolved Python type."""
        params = kwargs["params"]
        lines = [f"  {k!r}: {type(v).__name__} = {json.dumps(v, default=str)}" for k, v in params.items()]
        print("Resolved param types:\n" + "\n".join(lines))

    @task(task_display_name="Run pipeline")
    def run_pipeline(**kwargs) -> None:
        params = kwargs["params"]
        label = params["run_label"]
        dry_run = params["dry_run"]
        batch_size = params["batch_size"]
        notify = params["notify"]
        pipeline = params["pipeline"]
        destination = params["destination"]

        # batch_size: int → fixed row limit; str → process everything
        if isinstance(batch_size, int):
            print(f"[{label}] Processing up to {batch_size:,} rows per batch")
        else:
            print(f"[{label}] Processing all rows (batch_size={batch_size!r})")

        # notify: bool → team alias; str → custom address
        if isinstance(notify, bool):
            print(f"[{label}] Notifications: {'team alias' if notify else 'disabled'}")
        else:
            print(f"[{label}] Notifications → {notify}")

        # pipeline: str → shorthand name; dict → full config
        if isinstance(pipeline, dict):
            name = pipeline.get("name", "unnamed")
            retries = pipeline.get("retries", 0)
            timeout = pipeline.get("timeout_minutes", "∞")
            print(f"[{label}] Pipeline '{name}' — retries={retries}, timeout={timeout}m")
        else:
            print(f"[{label}] Pipeline: {pipeline}")

        # destination: str → schema.table; dict → full connection config
        if isinstance(destination, dict):
            schema = destination.get("schema", "public")
            table = destination.get("table", "output")
            conn = destination.get("connection", "default")
            print(f"[{label}] Destination: {schema}.{table} via connection '{conn}'")
        else:
            print(f"[{label}] Destination: {destination}")

        if dry_run:
            print(f"[{label}] DRY RUN — all writes skipped")

    log_params() >> run_pipeline()
