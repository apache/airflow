#!/usr/bin/env python
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
Generate conn-fields and ui-field-behaviour for provider.yaml from Python hooks.

This script requires the Airflow development environment with all workspace
modules available.

Usage:
    python scripts/generate_yaml_format_for_hooks.py --provider google
    python scripts/generate_yaml_format_for_hooks.py --provider google --update-yaml
    python scripts/generate_yaml_format_for_hooks.py --hook-class airflow.providers.http.hooks.http.HttpHook
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml
from airflow_breeze.utils.console import get_console

AIRFLOW_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(AIRFLOW_ROOT))
sys.path.insert(0, str(AIRFLOW_ROOT / "dev" / "breeze" / "src"))

console = get_console()


def extract_conn_fields(widgets, connection_type):
    """Get conn-fields from hook widgets."""
    conn_fields = {}
    prefix = f"extra__{connection_type}__"

    for field_key, field_widget in widgets.items():
        field_name = field_key[len(prefix) :] if field_key.startswith(prefix) else field_key

        field_class = getattr(field_widget, "field_class", None)
        if not field_class:
            continue

        field_class_name = field_class.__name__
        args = getattr(field_widget, "args", ())
        kwargs = getattr(field_widget, "kwargs", {})

        label = str(args[0]) if args else field_name.replace("_", " ").title()

        # Use ["type", "null"] to make fields optional (nullable) by default.
        # This matches the legacy behavior where all extra fields are optional.
        if "Boolean" in field_class_name:
            schema = {"type": ["boolean", "null"]}
        elif "Integer" in field_class_name:
            schema = {"type": ["integer", "null"]}
        elif "Password" in field_class_name:
            schema = {"type": ["string", "null"], "format": "password"}
        else:
            schema = {"type": ["string", "null"]}

        if kwargs.get("default") is not None:
            schema["default"] = kwargs["default"]

        # Extract enum constraints from any_of() / AnyOf validators
        validators = kwargs.get("validators", [])
        for validator in validators:
            if hasattr(validator, "values_formatter") and hasattr(validator, "values"):
                schema["enum"] = list(validator.values)

        yaml_field = {"label": label, "schema": schema}

        if kwargs.get("description"):
            yaml_field["description"] = kwargs["description"]

        conn_fields[field_name] = yaml_field

    return conn_fields


def extract_ui_behaviour(hook_class):
    """Get ui-field-behaviour from hook."""
    if not hasattr(hook_class, "get_ui_field_behaviour"):
        return None
    if "get_ui_field_behaviour" not in hook_class.__dict__:
        return None

    behaviour = hook_class.get_ui_field_behaviour()
    if not behaviour:
        return None

    yaml_behaviour = {}
    if behaviour.get("hidden_fields"):
        yaml_behaviour["hidden-fields"] = behaviour["hidden_fields"]
    if behaviour.get("relabeling"):
        yaml_behaviour["relabeling"] = behaviour["relabeling"]
    if behaviour.get("placeholders"):
        yaml_behaviour["placeholders"] = behaviour["placeholders"]

    return yaml_behaviour or None


def extract_from_hook(hook_class_name):
    """Get metadata from hook class."""
    from airflow._shared.module_loading import import_string

    try:
        hook_class = import_string(hook_class_name)
    except Exception as e:
        console.print(f"Error importing {hook_class_name}: {e}")
        return None, None, None

    connection_type = getattr(hook_class, "conn_type", None)
    if not connection_type:
        return None, None, None

    conn_fields = None
    if (
        hasattr(hook_class, "get_connection_form_widgets")
        and "get_connection_form_widgets" in hook_class.__dict__
    ):
        try:
            widgets = hook_class.get_connection_form_widgets()
            if widgets:
                conn_fields = extract_conn_fields(widgets, connection_type)
        except Exception as e:
            console.print(f"Error extracting widgets from {hook_class_name}: {e}")

    ui_behaviour = extract_ui_behaviour(hook_class)

    return conn_fields, ui_behaviour, connection_type


def find_provider_yaml(provider_name):
    """Find provider.yaml file."""
    path = AIRFLOW_ROOT / "providers" / provider_name / "provider.yaml"
    return path if path.exists() else None


def get_hooks_from_provider(provider_name):
    """Get hook classes from provider.yaml."""
    provider_yaml = find_provider_yaml(provider_name)
    if not provider_yaml:
        return []

    with open(provider_yaml) as f:
        data = yaml.safe_load(f)

    return [conn["hook-class-name"] for conn in data.get("connection-types", []) if "hook-class-name" in conn]


def update_provider_yaml(provider_name, hook_metadata):
    """Update provider.yaml with extracted metadata."""
    provider_yaml = find_provider_yaml(provider_name)
    if not provider_yaml:
        console.print(f"Provider yaml not found for {provider_name}")
        return False

    with open(provider_yaml) as f:
        data = yaml.safe_load(f)

    updated = False
    for conn in data.get("connection-types", []):
        hook_class = conn.get("hook-class-name")
        if hook_class not in hook_metadata:
            continue

        conn_fields, ui_behaviour, _ = hook_metadata[hook_class]

        if ui_behaviour:
            conn["ui-field-behaviour"] = ui_behaviour
            updated = True

        if conn_fields:
            conn["conn-fields"] = conn_fields
            updated = True

    if updated:
        with open(provider_yaml, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
        console.print(f"Updated {provider_yaml}")
        return True

    return False


def format_output(hook_class_name, conn_fields, ui_behaviour, connection_type):
    """Format extracted metadata for display."""
    lines = [f"# {hook_class_name} (connection_type: {connection_type})"]

    if ui_behaviour:
        lines.append("ui-field-behaviour:")
        ui_yaml = yaml.dump(ui_behaviour, default_flow_style=False, sort_keys=False)
        lines.extend(f"  {line}" for line in ui_yaml.rstrip().split("\n"))
        lines.append("")

    if conn_fields:
        lines.append("conn-fields:")
        fields_yaml = yaml.dump(conn_fields, default_flow_style=False, sort_keys=False)
        lines.extend(f"  {line}" for line in fields_yaml.rstrip().split("\n"))

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate yaml from hook metadata")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--provider", help="Provider name (e.g., google, docker)")
    group.add_argument("--hook-class", help="Full hook class name")
    parser.add_argument(
        "--update-yaml", action="store_true", help="Update provider.yaml (only with --provider)"
    )

    args = parser.parse_args()

    if args.hook_class:
        hooks = [args.hook_class]
        provider_name = None
    else:
        hooks = get_hooks_from_provider(args.provider)
        provider_name = args.provider

    if not hooks:
        console.print(f"No hooks found for provider {args.provider}")
        return 1

    hook_metadata = {}
    output_lines = []

    for hook_class_name in hooks:
        console.print(f"\nProcessing: {hook_class_name}")
        conn_fields, ui_behaviour, connection_type = extract_from_hook(hook_class_name)

        if conn_fields or ui_behaviour:
            hook_metadata[hook_class_name] = (conn_fields, ui_behaviour, connection_type)
            output = format_output(hook_class_name, conn_fields, ui_behaviour, connection_type)
            output_lines.append(output)
        else:
            console.print("No metadata found")

    if not output_lines:
        console.print("\nNo metadata extracted")
        return 1

    full_output = "\n" * 2 + " ".join(output_lines)

    if args.update_yaml:
        if not provider_name:
            console.print("\n--update-yaml only works with --provider")
            return 1
        if update_provider_yaml(provider_name, hook_metadata):
            console.print("\nProvider yaml updated")
        else:
            console.print("\nFailed to update provider yaml")
    else:
        console.print(full_output)

    return 0


if __name__ == "__main__":
    sys.exit(main())
