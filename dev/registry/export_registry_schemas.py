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
"""Export registry JSON Schema and OpenAPI artifacts from shared contracts.

Usage:
    python dev/registry/export_registry_schemas.py
    python dev/registry/export_registry_schemas.py --check
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from registry_contract_models import build_openapi_document, build_schema_documents

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SCHEMA_DIR = AIRFLOW_ROOT / "registry" / "schemas"


def _to_json(value: dict) -> str:
    return json.dumps(value, indent=2, sort_keys=True) + "\n"


def build_artifacts() -> dict[str, str]:
    artifacts = {filename: _to_json(schema) for filename, schema in build_schema_documents().items()}
    artifacts["openapi.json"] = _to_json(build_openapi_document())
    return artifacts


def write_artifacts(schema_dir: Path, artifacts: dict[str, str]) -> None:
    schema_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in artifacts.items():
        output = schema_dir / filename
        output.write_text(content)
        print(f"  wrote {output}")


def check_artifacts(schema_dir: Path, artifacts: dict[str, str]) -> int:
    missing: list[Path] = []
    drifted: list[Path] = []
    for filename, expected in artifacts.items():
        output = schema_dir / filename
        if not output.exists():
            missing.append(output)
            continue
        current = output.read_text()
        if current != expected:
            drifted.append(output)

    if not missing and not drifted:
        print("Schema artifacts are up to date.")
        return 0

    for path in missing:
        print(f"MISSING: {path}")
    for path in drifted:
        print(f"DRIFT:   {path}")
    print("\nRun `uv run python dev/registry/export_registry_schemas.py` to regenerate artifacts.")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Export registry schema artifacts from contracts.")
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=DEFAULT_SCHEMA_DIR,
        help="Output directory for generated schema artifacts.",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Do not write files; fail if generated artifacts differ from checked-in files.",
    )
    args = parser.parse_args()

    artifacts = build_artifacts()
    if args.check:
        return check_artifacts(args.schema_dir, artifacts)

    write_artifacts(args.schema_dir, artifacts)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
