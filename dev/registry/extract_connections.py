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

Extracts connection form metadata (custom fields, UI behaviour, standard field
configuration) for all providers.  Reads from provider.yaml first (conn-fields
and ui-field-behaviour), falling back to runtime inspection of hook classes for
providers that have not yet migrated to YAML.

Must be run inside breeze where all providers are installed.

Usage:
    breeze run python dev/registry/extract_connections.py

Output:
    - registry/src/_data/versions/{provider_id}/{version}/connections.json
    - dev/registry/output/versions/{provider_id}/{version}/connections.json
"""

from __future__ import annotations

import importlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml

AIRFLOW_ROOT = Path(__file__).parent.parent.parent
SCRIPT_DIR = Path(__file__).parent
PROVIDERS_DIR = AIRFLOW_ROOT / "providers"

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


def discover_provider_yamls() -> dict[str, tuple[str, dict]]:
    """
    Scan providers/ for all provider.yaml files.

    Returns {provider_id: (yaml_path, parsed_yaml)}.
    """
    results: dict[str, tuple[str, dict]] = {}

    for yaml_path in sorted(PROVIDERS_DIR.rglob("provider.yaml")):
        relative = yaml_path.relative_to(PROVIDERS_DIR)
        parts = relative.parts[:-1]
        provider_id = "-".join(parts)

        try:
            with open(yaml_path) as f:
                data = yaml.safe_load(f)
            results[provider_id] = (str(yaml_path), data)
        except Exception as e:
            print(f"  WARN: Failed to parse {yaml_path}: {e}")

    return results


def import_hook_class(hook_class_name: str) -> type | None:
    """Import a hook class by its fully-qualified name."""
    parts = hook_class_name.rsplit(".", 1)
    if len(parts) != 2:
        return None
    module_path, class_name = parts
    try:
        module = importlib.import_module(module_path)
        return getattr(module, class_name, None)
    except Exception as e:
        print(f"  WARN: Failed to import {hook_class_name}: {e}")
        return None


def extract_conn_fields_from_widgets(widgets: dict, connection_type: str) -> dict:
    """
    Convert WTForms widgets from get_connection_form_widgets() to the
    same schema as provider.yaml conn-fields.

    Mirrors logic from scripts/tools/generate_yaml_format_for_hooks.py.
    """
    conn_fields: dict[str, dict] = {}
    prefix = f"extra__{connection_type}__"

    for field_key, field_widget in widgets.items():
        field_name = field_key[len(prefix) :] if field_key.startswith(prefix) else field_key

        if hasattr(field_widget, "param"):
            field_data = field_widget.param.dump()
            schema = field_data.get("schema", {}).copy()
            label = schema.pop("title", field_name.replace("_", " ").title())

            field_class = getattr(field_widget, "field_class", None)
            if field_class and "Password" in field_class.__name__:
                if schema.get("format") != "password":
                    schema["format"] = "password"

            if field_data.get("value") is not None:
                schema["default"] = field_data["value"]

            entry: dict = {"label": label, "schema": schema}
            if field_data.get("description"):
                entry["description"] = field_data["description"]
        else:
            field_class = getattr(field_widget, "field_class", None)
            label_obj = (
                getattr(field_widget, "args", [None])[0] if getattr(field_widget, "args", None) else None
            )
            label = str(label_obj) if label_obj else field_name.replace("_", " ").title()
            description = getattr(field_widget, "description", None)

            schema = {"type": "string"}
            if field_class:
                cls_name = field_class.__name__
                if "Integer" in cls_name:
                    schema["type"] = "integer"
                elif "Boolean" in cls_name:
                    schema["type"] = "boolean"
                elif "Password" in cls_name:
                    schema["format"] = "password"

            default = getattr(field_widget, "default", None)
            if default is not None:
                schema["default"] = default

            entry = {"label": label, "schema": schema}
            if description:
                entry["description"] = str(description)

        conn_fields[field_name] = entry

    return conn_fields


def extract_ui_behaviour_from_hook(hook_class: type) -> dict | None:
    """
    Call get_ui_field_behaviour() on a hook class and convert to YAML-style keys.
    Only calls if the method is defined directly on this class (not just inherited).
    """
    if not hasattr(hook_class, "get_ui_field_behaviour"):
        return None
    if "get_ui_field_behaviour" not in hook_class.__dict__:
        return None

    try:
        behaviour = hook_class.get_ui_field_behaviour()
    except Exception as e:
        print(f"  WARN: get_ui_field_behaviour() failed for {hook_class.__name__}: {e}")
        return None

    if not behaviour:
        return None

    result: dict = {}
    if behaviour.get("hidden_fields"):
        result["hidden-fields"] = behaviour["hidden_fields"]
    if behaviour.get("relabeling"):
        result["relabeling"] = behaviour["relabeling"]
    if behaviour.get("placeholders"):
        result["placeholders"] = behaviour["placeholders"]

    return result or None


def build_standard_fields(ui_behaviour: dict | None) -> dict:
    """
    Build standard_fields dict from ui-field-behaviour, showing visibility,
    labels and placeholders for the 7 standard connection fields.
    """
    hidden = set()
    relabeling: dict[str, str] = {}
    placeholders: dict[str, str] = {}

    if ui_behaviour:
        hidden = set(ui_behaviour.get("hidden-fields", []))
        relabeling = ui_behaviour.get("relabeling", {})
        placeholders = ui_behaviour.get("placeholders", {})

    result: dict[str, dict] = {}
    for field_name in STANDARD_FIELDS:
        entry: dict = {"visible": field_name not in hidden}
        if field_name in relabeling:
            entry["label"] = relabeling[field_name]
        else:
            entry["label"] = DEFAULT_LABELS[field_name]
        if field_name in placeholders:
            entry["placeholder"] = placeholders[field_name]
        result[field_name] = entry

    return result


def build_custom_fields(conn_fields: dict | None) -> dict:
    """
    Normalise conn-fields (from YAML or Python extraction) into a flat
    output schema for each custom field.
    """
    if not conn_fields:
        return {}

    result: dict[str, dict] = {}
    for field_name, field_def in conn_fields.items():
        schema = field_def.get("schema", {})
        entry = {
            "label": field_def.get("label", field_name.replace("_", " ").title()),
            "type": schema.get("type", "string"),
            "default": schema.get("default"),
            "format": schema.get("format"),
            "description": field_def.get("description"),
            "is_sensitive": schema.get("format") == "password",
        }
        if "enum" in schema:
            entry["enum"] = schema["enum"]
        if "minimum" in schema:
            entry["minimum"] = schema["minimum"]
        if "maximum" in schema:
            entry["maximum"] = schema["maximum"]
        result[field_name] = entry

    return result


def extract_connection_type(
    conn_config: dict,
    provider_id: str,
) -> dict | None:
    """
    Extract full connection metadata for a single connection-type entry.
    Tries YAML data first, falls back to importing the hook.
    """
    connection_type = conn_config.get("connection-type", "")
    hook_class_name = conn_config.get("hook-class-name", "")

    if not connection_type:
        return None

    yaml_conn_fields = conn_config.get("conn-fields")
    yaml_ui_behaviour = conn_config.get("ui-field-behaviour")

    python_conn_fields = None
    python_ui_behaviour = None
    need_python = yaml_conn_fields is None or yaml_ui_behaviour is None

    if need_python and hook_class_name:
        hook_class = import_hook_class(hook_class_name)
        if hook_class:
            if yaml_conn_fields is None:
                if (
                    hasattr(hook_class, "get_connection_form_widgets")
                    and "get_connection_form_widgets" in hook_class.__dict__
                ):
                    try:
                        widgets = hook_class.get_connection_form_widgets()
                        if widgets:
                            python_conn_fields = extract_conn_fields_from_widgets(widgets, connection_type)
                    except Exception as e:
                        print(f"  WARN: get_connection_form_widgets() failed for {hook_class_name}: {e}")

            if yaml_ui_behaviour is None:
                python_ui_behaviour = extract_ui_behaviour_from_hook(hook_class)

    final_conn_fields = yaml_conn_fields if yaml_conn_fields is not None else python_conn_fields
    final_ui_behaviour = yaml_ui_behaviour if yaml_ui_behaviour is not None else python_ui_behaviour

    # Determine source for transparency
    source_parts = []
    if yaml_conn_fields is not None or yaml_ui_behaviour is not None:
        source_parts.append("yaml")
    if python_conn_fields is not None or python_ui_behaviour is not None:
        source_parts.append("python")
    source = "+".join(source_parts) if source_parts else "minimal"

    return {
        "connection_type": connection_type,
        "hook_class": hook_class_name,
        "source": source,
        "standard_fields": build_standard_fields(final_ui_behaviour),
        "custom_fields": build_custom_fields(final_conn_fields),
    }


def main():
    print("Airflow Registry Connection Metadata Extractor")
    print("=" * 50)

    # Load providers.json for provider_id -> latest_version mapping
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
    for p in providers_data.get("providers", []):
        provider_versions[p["id"]] = p["version"]

    provider_yamls = discover_provider_yamls()
    print(f"Found {len(provider_yamls)} provider.yaml files")

    generated_at = datetime.now(timezone.utc).isoformat()

    provider_connections: dict[str, list[dict]] = defaultdict(list)
    provider_names: dict[str, str] = {}

    total_conn_types = 0
    total_with_custom = 0
    total_with_ui = 0

    for provider_id, (_yaml_path, provider_yaml) in sorted(provider_yamls.items()):
        conn_types = provider_yaml.get("connection-types", [])
        if not conn_types:
            continue

        provider_name = provider_yaml.get("name", provider_id.replace("-", " ").title())
        provider_names[provider_id] = provider_name

        for conn_config in conn_types:
            result = extract_connection_type(conn_config, provider_id)
            if result is None:
                continue

            provider_connections[provider_id].append(result)
            total_conn_types += 1

            if result["custom_fields"]:
                total_with_custom += 1
            has_ui = any(
                f.get("visible") is False or f.get("label") != DEFAULT_LABELS.get(fname)
                for fname, f in result["standard_fields"].items()
            )
            if has_ui:
                total_with_ui += 1

    print(f"\nExtracted {total_conn_types} connection types across {len(provider_connections)} providers")
    print(f"  {total_with_custom} have custom fields")
    print(f"  {total_with_ui} have UI field customisation")

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

            provider_data = {
                "provider_id": pid,
                "provider_name": provider_names.get(pid, pid),
                "version": version,
                "generated_at": generated_at,
                "connection_types": conns,
            }
            with open(version_dir / "connections.json", "w") as f:
                json.dump(provider_data, f, separators=(",", ":"))
            written += 1

        print(f"Wrote {written} provider connection files to {output_dir}/versions/")

    print("\nDone!")


if __name__ == "__main__":
    sys.exit(main() or 0)
