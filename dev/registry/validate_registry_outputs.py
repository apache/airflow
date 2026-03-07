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
"""Validate generated registry JSON output files against schema artifacts.

Usage:
    python dev/registry/validate_registry_outputs.py
    python dev/registry/validate_registry_outputs.py --data-dir dev/registry/output
    python dev/registry/validate_registry_outputs.py --strict --site-api-dir registry/_site/api
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from jsonschema import FormatChecker, validators

AIRFLOW_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = AIRFLOW_ROOT / "registry" / "src" / "_data"
DEFAULT_SCHEMA_DIR = AIRFLOW_ROOT / "registry" / "schemas"


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text())


def _build_validator(schema: dict[str, Any]):
    validator_cls = validators.validator_for(schema)
    validator_cls.check_schema(schema)
    return validator_cls(schema, format_checker=FormatChecker())


def _iter_validation_errors(validator, payload: Any) -> list[str]:
    errors = []
    for error in sorted(validator.iter_errors(payload), key=lambda e: str(list(e.path))):
        path = ".".join(str(p) for p in error.absolute_path) or "$"
        errors.append(f"{path}: {error.message}")
    return errors


def _validate_file(file_path: Path, validator) -> list[str]:
    try:
        payload = _load_json(file_path)
    except Exception as err:
        return [f"$: failed to parse JSON: {err}"]
    return _iter_validation_errors(validator, payload)


def _collect_output_files(data_dir: Path) -> dict[str, list[Path]]:
    versions_dir = data_dir / "versions"
    return {
        "providers": [data_dir / "providers.json"],
        "modules": [data_dir / "modules.json"],
        "metadata": sorted(versions_dir.rglob("metadata.json")) if versions_dir.exists() else [],
        "parameters": sorted(versions_dir.rglob("parameters.json")) if versions_dir.exists() else [],
        "connections": sorted(versions_dir.rglob("connections.json")) if versions_dir.exists() else [],
    }


def _collect_site_api_files(site_api_dir: Path) -> dict[str, list[Path]]:
    providers_dir = site_api_dir / "providers"
    return {
        "provider_versions": sorted(providers_dir.glob("*/versions.json")) if providers_dir.exists() else [],
    }


def validate_outputs(
    data_dir: Path,
    schema_dir: Path,
    *,
    strict: bool = False,
    site_api_dir: Path | None = None,
) -> int:
    schema_files = {
        "providers": schema_dir / "providers-catalog.schema.json",
        "modules": schema_dir / "modules-catalog.schema.json",
        "metadata": schema_dir / "provider-version-metadata.schema.json",
        "parameters": schema_dir / "provider-parameters.schema.json",
        "connections": schema_dir / "provider-connections.schema.json",
    }
    if site_api_dir is not None:
        schema_files["provider_versions"] = schema_dir / "provider-versions.schema.json"

    validators_by_type = {}
    for data_type, schema_file in schema_files.items():
        if not schema_file.exists():
            raise FileNotFoundError(f"Schema file missing: {schema_file}")
        validators_by_type[data_type] = _build_validator(_load_json(schema_file))

    files_by_type = _collect_output_files(data_dir)
    if site_api_dir is not None:
        files_by_type.update(_collect_site_api_files(site_api_dir))

    required_types = set(files_by_type.keys()) if strict else set()
    failures = 0
    validated = 0
    for data_type, files in files_by_type.items():
        if not files:
            if data_type in required_types:
                failures += 1
                print(f"FAIL {data_type}: no files found")
            else:
                print(f"SKIP {data_type}: no files found")
            continue
        for file_path in files:
            if not file_path.exists():
                failures += 1
                print(f"FAIL {file_path}: file is missing")
                continue
            errors = _validate_file(file_path, validators_by_type[data_type])
            if errors:
                failures += 1
                print(f"FAIL {file_path}")
                for err in errors:
                    print(f"  - {err}")
                continue
            validated += 1

    print(f"Validated {validated} registry JSON files.")
    if failures:
        print(f"Found {failures} invalid files.")
        return 1
    print("All validated files conform to schemas.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate generated registry JSON outputs.")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Directory containing generated registry JSON outputs.",
    )
    parser.add_argument(
        "--schema-dir",
        type=Path,
        default=DEFAULT_SCHEMA_DIR,
        help="Directory containing exported schema artifacts.",
    )
    parser.add_argument(
        "--site-api-dir",
        type=Path,
        default=None,
        help="Optional built site API directory (for example: registry/_site/api).",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Fail when expected file categories contain no files.",
    )
    args = parser.parse_args()
    return validate_outputs(
        data_dir=args.data_dir,
        schema_dir=args.schema_dir,
        strict=args.strict,
        site_api_dir=args.site_api_dir,
    )


if __name__ == "__main__":
    raise SystemExit(main())
