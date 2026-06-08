#!/usr/bin/env python3
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
Standalone demo for the DAG Triage Assistant plugin.

Runs entirely in-process — no Airflow installation required.

Usage
-----
    python team_project/demo.py

What it shows
-------------
1. Plugin info  — simulates the /health endpoint.
2. Five failure scenarios exercising each classifier category.
3. Pretty-printed triage results with remediation steps.
4. Integration walk-through: how to call the REST API once the plugin
   is installed in a live Airflow environment.
"""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

# Make dag_triage and dag_triage_plugin importable without pip install.
_ROOT = Path(__file__).parent
sys.path.insert(0, str(_ROOT / "src"))
sys.path.insert(0, str(_ROOT / "plugin"))

from dag_triage_plugin.api import create_app  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

# ---------------------------------------------------------------------------
# ANSI colours
# ---------------------------------------------------------------------------
_RESET = "\033[0m"
_BOLD = "\033[1m"
_CYAN = "\033[36m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_MAGENTA = "\033[35m"
_BLUE = "\033[34m"

_CAT_COLOUR = {
    "TRANSIENT": _BLUE,
    "DATA_QUALITY": _YELLOW,
    "RESOURCE": _RED,
    "CODE": _MAGENTA,
    "EXTERNAL_DEPENDENCY": _GREEN,
    None: "\033[90m",  # grey
}

# ---------------------------------------------------------------------------
# Demo scenarios
# ---------------------------------------------------------------------------

_SCENARIOS: list[dict] = [
    {
        "name": "OOM-killed Spark job",
        "dag_id": "spark_etl",
        "run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "task_id": "run_spark_job",
        "log": textwrap.dedent("""\
            [2024-01-01T00:01:00.000+0000] {worker.py:42} INFO - Starting Spark submit
            [2024-01-01T00:04:30.000+0000] {worker.py:55} ERROR - Container OOMKilled: memory limit of 4Gi exceeded
            java.lang.OutOfMemoryError: Java heap space
                at org.apache.spark.storage.MemoryStore.putBytes(MemoryStore.scala:88)
        """),
    },
    {
        "name": "Missing Python dependency",
        "dag_id": "ml_pipeline",
        "run_id": "manual__2024-02-14T10:00:00+00:00",
        "task_id": "feature_engineering",
        "log": textwrap.dedent("""\
            [2024-02-14T10:00:01.000+0000] {worker.py:42} INFO - Starting task
            [2024-02-14T10:00:02.000+0000] {worker.py:99} ERROR - ImportError: No module named 'xgboost'
                File "/opt/airflow/dags/ml_dag.py", line 5, in <module>
                    import xgboost as xgb
        """),
    },
    {
        "name": "Network timeout to external API",
        "dag_id": "data_ingestion",
        "run_id": "scheduled__2024-03-10T08:00:00+00:00",
        "task_id": "fetch_partner_data",
        "log": textwrap.dedent("""\
            [2024-03-10T08:00:05.000+0000] {http_hook.py:200} INFO - Calling GET https://api.partner.example/v2/records
            [2024-03-10T08:00:35.000+0000] {http_hook.py:210} ERROR - Task failed after 30s: connection reset by peer, timeout exceeded
            Retry exhausted after 3 attempts.
        """),
    },
    {
        "name": "Schema mismatch in data load",
        "dag_id": "warehouse_load",
        "run_id": "scheduled__2024-04-01T00:00:00+00:00",
        "task_id": "load_orders",
        "log": textwrap.dedent("""\
            [2024-04-01T00:10:00.000+0000] {postgres_hook.py:128} INFO - Executing COPY into orders_staging
            [2024-04-01T00:10:05.000+0000] {postgres_hook.py:135} ERROR - Schema mismatch detected: expected INT got STRING for column 'order_value'
            Validation error on row 4427.
        """),
    },
    {
        "name": "DNS resolution failure for database host",
        "dag_id": "reporting",
        "run_id": "scheduled__2024-05-20T06:00:00+00:00",
        "task_id": "refresh_dashboards",
        "log": textwrap.dedent("""\
            [2024-05-20T06:00:01.000+0000] {mysql_hook.py:88} INFO - Connecting to reporting_db
            [2024-05-20T06:00:02.000+0000] {mysql_hook.py:95} ERROR - DNS resolution failed: host unreachable for endpoint reporting_db.internal
            SSL: CERTIFICATE_VERIFY_FAILED certificate verify failed (_ssl.c:1129)
        """),
    },
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _colour(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}"


def _section(title: str) -> None:
    print(f"\n{_BOLD}{_CYAN}{'─' * 60}{_RESET}")
    print(f"{_BOLD}{_CYAN}  {title}{_RESET}")
    print(f"{_BOLD}{_CYAN}{'─' * 60}{_RESET}")


def _print_triage(scenario: dict, result: dict) -> None:
    cat = result["failure_category"]
    conf = result["confidence"]
    cat_label = cat if cat else "UNRECOGNISED"
    col = _CAT_COLOUR.get(cat, "")

    print(f"\n{_BOLD}Scenario:{_RESET} {scenario['name']}")
    print(f"  Dag / task : {scenario['dag_id']} → {scenario['task_id']}")
    print(f"  Category   : {_colour(cat_label, col + _BOLD)}")
    print(f"  Confidence : {_colour(f'{conf:.0%}', col)}")
    print(f"  Summary    : {result['root_cause_summary']}")

    if result["remediations"]:
        print(f"\n  {_BOLD}Remediation steps:{_RESET}")
        for rem in result["remediations"]:
            print(f"  [{rem['title']}]")
            for step in rem["steps"]:
                wrapped = textwrap.fill(step, width=70, subsequent_indent="       ")
                print(f"    • {wrapped}")
            if rem["doc_links"]:
                print(f"    Docs: {rem['doc_links'][0]}")
    else:
        print("  No matching remediation in knowledge base — review logs manually.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    client = TestClient(create_app())

    _section("Plugin health")
    health = client.get("/health").json()
    print(f"  Status  : {_colour(health['status'], _GREEN + _BOLD)}")
    print(f"  Plugin  : {health['plugin']}")
    print(f"  Version : {health['version']}")

    _section("Triage scenarios")
    for scenario in _SCENARIOS:
        payload = {
            "dag_id": scenario["dag_id"],
            "run_id": scenario["run_id"],
            "task_id": scenario["task_id"],
            "log_content": scenario["log"],
        }
        resp = client.post("/api/v1/triage", json=payload)
        if resp.status_code != 200:
            print(f"  ERROR for scenario '{scenario['name']}': {resp.text}")
            continue
        _print_triage(scenario, resp.json())

    _section("How to call the live API")
    live_url = "http://localhost:8080/plugins/dag-triage/api/v1/triage"
    example_payload = {
        "dag_id": "my_dag",
        "run_id": "scheduled__2024-01-01T00:00:00+00:00",
        "task_id": "my_task",
        "log_content": "<paste your log here>",
    }
    print(f"\n  Endpoint : POST {live_url}")
    print("  Panel    : http://localhost:8080/plugins/dag-triage/")
    print("  API docs : http://localhost:8080/plugins/dag-triage/api/v1/docs")
    print("\n  curl example:\n")
    curl = (
        f"  curl -s -X POST '{live_url}' \\\n"
        f"    -H 'Content-Type: application/json' \\\n"
        f"    -d '{json.dumps(example_payload)}' | python3 -m json.tool"
    )
    print(curl)
    print()


if __name__ == "__main__":
    main()
