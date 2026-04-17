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
Serialize DAGs using the Python Airflow SDK for cross-language comparison.

Prerequisites:
  - Airflow core and task-sdk installed  (pip install -e …)
  - PyYAML installed                     (pip install pyyaml)

Usage:
    python serialize_python.py test_dags.yaml serialized_python.json
"""

from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# YAML params → Python DAG constructor kwargs
# ---------------------------------------------------------------------------


def _yaml_params_to_dag_kwargs(params: dict) -> dict:
    """Convert language-agnostic YAML params to Python DAG constructor kwargs."""
    kwargs: dict = {}
    for key, value in params.items():
        if key in ("start_date", "end_date") and isinstance(value, str):
            kwargs[key] = datetime.fromisoformat(value)
        elif key == "dagrun_timeout_seconds":
            kwargs["dagrun_timeout"] = timedelta(seconds=value)
        elif key == "tags" and isinstance(value, list):
            kwargs["tags"] = set(value)
        elif key == "access_control" and isinstance(value, dict):
            # Convert innermost lists → sets (permissions)
            kwargs["access_control"] = {
                role: {
                    resource: set(perms) if isinstance(perms, list) else perms
                    for resource, perms in resources.items()
                }
                for role, resources in value.items()
            }
        elif key == "params":
            kwargs["params"] = value
        else:
            kwargs[key] = value
    return kwargs


# ---------------------------------------------------------------------------
# JSON helper
# ---------------------------------------------------------------------------


def _make_json_safe(obj):
    """Handle types that json.dumps cannot serialise natively."""
    if isinstance(obj, (set, frozenset)):
        return sorted(obj)
    if isinstance(obj, bytes):
        return obj.decode("utf-8")
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <test_dags.yaml> <output.json>")
        sys.exit(1)

    yaml_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    with open(yaml_path) as fh:
        test_data = yaml.safe_load(fh)

    # Lazy-import Airflow so the script fails fast on missing args first.
    from airflow.sdk import DAG
    from airflow.serialization.serialized_objects import DagSerialization

    results: dict[str, dict] = {}
    for case in test_data["test_cases"]:
        name = case["name"]
        kwargs = _yaml_params_to_dag_kwargs(case["params"])
        print(f"  [{name}] ", end="")
        try:
            dag = DAG(**kwargs)
            serialized = DagSerialization.serialize_dag(dag)
            results[name] = serialized
            print("OK")
        except Exception as exc:
            print(f"ERROR: {exc}")
            results[name] = {"__error": str(exc)}

    with open(output_path, "w") as fh:
        json.dump(results, fh, indent=2, sort_keys=True, default=_make_json_safe)

    print(f"\nWrote {len(results)} serialised DAGs → {output_path}")


if __name__ == "__main__":
    main()
