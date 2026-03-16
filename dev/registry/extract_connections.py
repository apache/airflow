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
Airflow Registry Connection Metadata Extractor

Uses ProvidersManager to extract connection form metadata (custom fields,
UI behaviour, standard field configuration) for all providers.
ProvidersManager handles both YAML-declared metadata and Python hook
fallback automatically.

Must be run inside breeze where all providers are installed.

Usage:
    breeze run python dev/registry/extract_connections.py

Output:
    - registry/src/_data/versions/{provider_id}/{version}/connections.json
    - dev/registry/output/versions/{provider_id}/{version}/connections.json
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from registry_contract_models import validate_provider_connections

AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent

PROVIDERS_JSON_CANDIDATES = [
    SCRIPT_DIR / "providers.json",
    AIRFLOW_ROOT / "registry" / "src" / "_data" / "providers.json",
]

OUTPUT_DIRS = [
    SCRIPT_DIR / "output",
    AIRFLOW_ROOT / "registry" / "src" / "_data",
]

STANDARD_FIELDS = ["host", "port", "login", "password", "schema", "extra", "description"]

DEFAULT_LABELS = {
    "host": "Host",
    "port": "Port",
    "login": "Login",
    "password": "Password",
    "schema": "Schema",
    "extra": "Extra",
    "description": "Description",
}


def package_name_to_provider_id(package_name: str) -> str:
    """Convert 'apache-airflow-providers-amazon' to 'amazon'."""
    prefix = "apache-airflow-providers-"
    if package_name.startswith(prefix):
        return package_name[len(prefix) :]
    return package_name


def build_standard_fields(field_behaviour: dict | None) -> dict:
    """
    Build standard_fields dict from field_behaviours, showing visibility,
    labels and placeholders for the 7 standard connection fields.
    """
    hidden: set[str] = set()
    relabeling: dict[str, str] = {}
    placeholders: dict[str, str] = {}

    if field_behaviour:
        hidden = set(field_behaviour.get("hidden_fields", []))
        relabeling = field_behaviour.get("relabeling", {})
        placeholders = field_behaviour.get("placeholders", {})

    result: dict[str, dict] = {}
    for field_name in STANDARD_FIELDS:
        entry: dict[str, Any] = {"visible": field_name not in hidden}
        if field_name in relabeling:
            entry["label"] = relabeling[field_name]
        else:
            entry["label"] = DEFAULT_LABELS[field_name]
        if field_name in placeholders:
            entry["placeholder"] = placeholders[field_name]
        result[field_name] = entry

    return result


def build_custom_fields(
    form_widgets: dict,
    connection_type: str,
) -> dict:
    """
    Convert ProvidersManager's connection_form_widgets for a connection type
    into the registry's custom_fields format.

    Handles both YAML-sourced fields (dicts) and Python-sourced fields
    (mocked WTForms objects with .param.dump()) via duck typing — no
    Airflow imports needed so tests can run on host without breeze.
    """
    prefix = f"extra__{connection_type}__"
    result: dict[str, dict] = {}

    for key, widget_info in form_widgets.items():
        if not key.startswith(prefix):
            continue

        field_name = widget_info.field_name

        if isinstance(widget_info.field, dict):
            # YAML path: already in SerializedParam.dump() format
            field_data = widget_info.field
        elif hasattr(widget_info.field, "param"):
            # Python path: mocked WTForms field with .param.dump()
            field_data = widget_info.field.param.dump()
        else:
            continue

        schema = field_data.get("schema", {})
        entry: dict[str, Any] = {
            "label": schema.get("title", field_name.replace("_", " ").title()),
            "type": schema.get("type", "string"),
            "default": field_data.get("value"),
            "format": schema.get("format"),
            "description": field_data.get("description"),
            "is_sensitive": widget_info.is_sensitive,
        }
        if "enum" in schema:
            entry["enum"] = schema["enum"]
        if "minimum" in schema:
            entry["minimum"] = schema["minimum"]
        if "maximum" in schema:
            entry["maximum"] = schema["maximum"]
        result[field_name] = entry

    return result


def main():
    parser = argparse.ArgumentParser(description="Extract provider connection metadata")
    parser.add_argument(
        "--provider",
        default=None,
        help="Only output connections for this provider ID (e.g. 'amazon').",
    )
    parser.add_argument(
        "--providers-json",
        default=None,
        help="Path to providers.json (overrides default search paths).",
    )
    args = parser.parse_args()

    print("Airflow Registry Connection Metadata Extractor")
    print("=" * 50)

    # Load providers.json for provider_id -> latest_version + name mapping
    if args.providers_json:
        providers_json_path = Path(args.providers_json)
    else:
        providers_json_path = None
        for candidate in PROVIDERS_JSON_CANDIDATES:
            if candidate.exists():
                providers_json_path = candidate
                break

    if providers_json_path is None:
        print("ERROR: providers.json not found. Run extract_metadata.py first.")
        sys.exit(1)

    with open(providers_json_path) as f:
        providers_data = json.load(f)

    provider_versions: dict[str, str] = {}
    provider_names: dict[str, str] = {}
    for p in providers_data.get("providers", []):
        provider_versions[p["id"]] = p["version"]
        provider_names[p["id"]] = p.get("name", p["id"])

    # Get connection data from ProvidersManager via HookMetaService
    # (reuses existing WTForms mocking instead of duplicating it)
    print("Loading ProvidersManager...")
    from airflow.api_fastapi.core_api.services.ui.connections import HookMetaService

    hooks, form_widgets, field_behaviours = HookMetaService._get_hooks_with_mocked_fab()

    generated_at = datetime.now(timezone.utc).isoformat()

    # Group connection types by provider
    provider_connections: dict[str, list[dict]] = defaultdict(list)
    total_conn_types = 0
    total_with_custom = 0
    total_with_ui = 0

    for conn_type, hook_info in sorted(hooks.items()):
        if hook_info is None or not hook_info.package_name:
            continue

        provider_id = package_name_to_provider_id(hook_info.package_name)

        standard_fields = build_standard_fields(field_behaviours.get(conn_type))
        custom_fields = build_custom_fields(form_widgets, conn_type)

        conn_data = {
            "connection_type": conn_type,
            "hook_class": hook_info.hook_class_name,
            "standard_fields": standard_fields,
            "custom_fields": custom_fields,
        }

        provider_connections[provider_id].append(conn_data)
        total_conn_types += 1

        if custom_fields:
            total_with_custom += 1
        has_ui = any(
            f.get("visible") is False or f.get("label") != DEFAULT_LABELS.get(fname)
            for fname, f in standard_fields.items()
        )
        if has_ui:
            total_with_ui += 1

    print(f"\nExtracted {total_conn_types} connection types across {len(provider_connections)} providers")
    print(f"  {total_with_custom} have custom fields")
    print(f"  {total_with_ui} have UI field customisation")

    # Filter to single provider if requested
    if args.provider:
        provider_connections = {
            pid: conns for pid, conns in provider_connections.items() if pid == args.provider
        }
        print(f"Filtering output to provider: {args.provider}")

    # Write per-provider files to versions/{pid}/{version}/connections.json
    for output_dir in OUTPUT_DIRS:
        if not output_dir.parent.exists():
            continue

        written = 0
        for pid, conns in provider_connections.items():
            version = provider_versions.get(pid)
            if not version:
                print(f"  WARN: no version found for {pid}, skipping")
                continue

            version_dir = output_dir / "versions" / pid / version
            version_dir.mkdir(parents=True, exist_ok=True)

            provider_data = validate_provider_connections(
                {
                    "provider_id": pid,
                    "provider_name": provider_names.get(pid, pid),
                    "version": version,
                    "generated_at": generated_at,
                    "connection_types": conns,
                }
            )
            with open(version_dir / "connections.json", "w") as f:
                json.dump(provider_data, f, separators=(",", ":"))
            written += 1

        print(f"Wrote {written} provider connection files to {output_dir}/versions/")

    print("\nDone!")


if __name__ == "__main__":
    sys.exit(main() or 0)
