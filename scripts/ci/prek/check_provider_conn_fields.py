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
Validation helpers for the conn-fields ↔ get_connection_form_widgets() check.

These functions have no third-party dependencies so they can be unit-tested
outside of the Breeze container without any stubbing.

Used by ``scripts/in_container/run_provider_yaml_files_check.py``.
"""

from __future__ import annotations

import json
from collections.abc import Callable


def check_conn_fields_for_entry(
    conn_type_entry: dict,
    yaml_file_path: str,
    get_widget_keys: Callable[[str], set[str] | None],
) -> list[str]:
    """
    Validate a single connection-type entry.  Returns a (possibly empty) list of error strings.

    *get_widget_keys(hook_class_name)* is a callable supplied by the caller that:

    - returns the set of field keys from ``get_connection_form_widgets()`` on success,
    - returns ``None`` to signal that the hook could not be imported, its UI
      dependencies are absent, or it does not implement ``get_connection_form_widgets()``
      at all — the entry is then skipped entirely (no ``conn-fields`` check), or
    - raises any other ``Exception`` to signal an unexpected failure (converted
      here into an error string so callers never need to catch it).

    When the hook returns widgets, the ``conn-fields`` section in the provider YAML
    must exactly match.  If ``conn-fields`` is absent, every widget key is treated
    as missing from YAML (because any custom field missing from ``conn-fields`` will
    be invisible in the new React connection UI).
    """
    hook_class_name: str = conn_type_entry["hook-class-name"]
    connection_type: str = conn_type_entry.get("connection-type", "?")

    try:
        widget_keys = get_widget_keys(hook_class_name)
    except Exception as exc:
        return [
            f"Failed to call `{hook_class_name}.get_connection_form_widgets()` "
            f"while checking {yaml_file_path}: {exc}"
        ]

    if widget_keys is None:
        return []

    # Treat absent conn-fields as an empty dict so any hook widget is flagged as missing.
    conn_fields: dict = conn_type_entry.get("conn-fields") or {}

    error = build_mismatch_error(
        set(conn_fields.keys()), widget_keys, connection_type, yaml_file_path, hook_class_name
    )
    return [error] if error else []


def build_mismatch_error(
    yaml_keys: set[str],
    hook_keys: set[str],
    connection_type: str,
    yaml_file_path: str,
    hook_class_name: str,
) -> str | None:
    """
    Check that ``conn-fields`` and ``get_connection_form_widgets()`` are in sync.

    Once a provider declares ``conn-fields``, those declarations must exactly
    match the hook's form widgets — we flag both directions:

    - Keys present in ``conn-fields`` but absent from the hook → stale / invalid
      declarations that should be removed.
    - Keys present in the hook but absent from ``conn-fields`` → fields that will
      be invisible in the new React connection UI, almost certainly unintentional.

    Return an error string when any mismatch is found, or ``None`` when the sets
    are identical.
    """
    only_in_yaml = yaml_keys - hook_keys
    only_in_hook = hook_keys - yaml_keys

    if not only_in_yaml and not only_in_hook:
        return None

    lines = [
        f"Mismatch between `conn-fields` in {yaml_file_path} and "
        f"`{hook_class_name}.get_connection_form_widgets()` "
        f"for connection-type '{connection_type}':"
    ]
    if only_in_yaml:
        lines.append(
            "  Fields in provider.yaml conn-fields but NOT in get_connection_form_widgets(): "
            + ", ".join(sorted(only_in_yaml))
        )
        lines.append("[yellow]How to fix it[/]: Remove the stale key(s) from conn-fields in provider.yaml.")
    if only_in_hook:
        lines.append(
            "  Fields in get_connection_form_widgets() but NOT in provider.yaml conn-fields: "
            + ", ".join(sorted(only_in_hook))
        )
        lines.append("[yellow]How to fix it[/]: Add the missing key(s) to conn-fields in provider.yaml.")
    return "\n".join(lines)


def normalize_behaviour_value(value: object) -> str:
    """
    Equality-normalize a relabeling/placeholder value.

    Placeholder values are display examples, so insignificant formatting must not count as
    drift: surrounding whitespace is stripped, and values that parse as JSON on both sides
    are compared structurally (hooks often build them with ``json.dumps`` at a different
    indent than the YAML block scalar, and ``|`` block scalars add a trailing newline).
    """
    text = str(value).strip()
    try:
        return json.dumps(json.loads(text), sort_keys=True)
    except (ValueError, TypeError):
        return text


def check_ui_field_behaviour_for_entry(
    conn_type_entry: dict,
    yaml_file_path: str,
    get_behaviour: Callable[[str], dict | None],
) -> list[str]:
    """
    Validate a connection-type entry's ``ui-field-behaviour`` against ``get_ui_field_behaviour()``.

    *get_behaviour(hook_class_name)* is a callable supplied by the caller that returns the
    hook's ``get_ui_field_behaviour()`` dict, or ``None`` to skip the entry (hook not
    importable, or it does not override the method). Any other exception is converted to an
    error string here so callers never need to catch it.

    Airflow reads ``ui-field-behaviour`` from the provider YAML and ignores the hook method
    when the YAML is present, so a hook-side edit that is not mirrored in the YAML never
    reaches the UI. Both a missing ``ui-field-behaviour`` section and any per-section
    difference are flagged.
    """
    hook_class_name: str = conn_type_entry["hook-class-name"]
    connection_type: str = conn_type_entry.get("connection-type", "?")

    try:
        hook_behaviour = get_behaviour(hook_class_name)
    except Exception as exc:
        return [
            f"Failed to call `{hook_class_name}.get_ui_field_behaviour()` "
            f"while checking {yaml_file_path}: {exc}"
        ]

    if hook_behaviour is None:
        return []

    header = (
        f"Mismatch between `ui-field-behaviour` in {yaml_file_path} and "
        f"`{hook_class_name}.get_ui_field_behaviour()` "
        f"for connection-type '{connection_type}':"
    )

    yaml_behaviour = conn_type_entry.get("ui-field-behaviour")
    if yaml_behaviour is None:
        return [
            f"{header}\n"
            "  The hook overrides get_ui_field_behaviour() but provider.yaml has no "
            "ui-field-behaviour section, so the hook's field behaviour is invisible in the UI.\n"
            "[yellow]How to fix it[/]: Declare ui-field-behaviour for this connection-type "
            "in provider.yaml."
        ]

    problems = []

    yaml_hidden = set(yaml_behaviour.get("hidden-fields") or [])
    hook_hidden = set(hook_behaviour.get("hidden_fields") or [])
    if yaml_hidden != hook_hidden:
        problems.append(
            "  hidden-fields differ."
            f" Only in provider.yaml: {sorted(yaml_hidden - hook_hidden) or '-'};"
            f" only in the hook: {sorted(hook_hidden - yaml_hidden) or '-'}"
        )

    for yaml_key, hook_key in (("relabeling", "relabeling"), ("placeholders", "placeholders")):
        yaml_section = {
            k: normalize_behaviour_value(v) for k, v in (yaml_behaviour.get(yaml_key) or {}).items()
        }
        hook_section = {
            k: normalize_behaviour_value(v) for k, v in (hook_behaviour.get(hook_key) or {}).items()
        }
        if yaml_section == hook_section:
            continue
        diff_keys = sorted(
            k for k in yaml_section.keys() | hook_section.keys() if yaml_section.get(k) != hook_section.get(k)
        )
        problems.append(f"  {yaml_key} differ for: {', '.join(diff_keys)}")

    if not problems:
        return []
    problems.append(
        "[yellow]How to fix it[/]: Make ui-field-behaviour in provider.yaml say the same "
        "thing as get_ui_field_behaviour(); the YAML is what the UI actually shows."
    )
    return ["\n".join([header, *problems])]
