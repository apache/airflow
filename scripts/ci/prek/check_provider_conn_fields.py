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

    conn_fields = conn_type_entry.get("conn-fields")
    if conn_fields is None:
        # No conn-fields declared: the new UI simply exposes no custom fields for this
        # connection type, which is intentional.  Nothing to validate.
        return []

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
    Check that every key declared in ``conn-fields`` exists in
    ``get_connection_form_widgets()``.

    ``conn-fields`` is the new React UI's view of a connection type and is
    intentionally a *subset* of the Flask form widgets — fields can be omitted
    from ``conn-fields`` on purpose.  We therefore only flag keys that appear in
    ``conn-fields`` but are absent from the hook's form (invalid / stale
    declarations).  The reverse direction (hook fields not listed in
    ``conn-fields``) is not an error.

    Return an error string when stale keys are found, or ``None`` when the
    declared keys are all valid.
    """
    only_in_yaml = yaml_keys - hook_keys

    if not only_in_yaml:
        return None

    lines = [
        f"Mismatch between `conn-fields` in {yaml_file_path} and "
        f"`{hook_class_name}.get_connection_form_widgets()` "
        f"for connection-type '{connection_type}':"
    ]
    lines.append(
        "  Fields in provider.yaml conn-fields but NOT in get_connection_form_widgets(): "
        + ", ".join(sorted(only_in_yaml))
    )
    lines.append("[yellow]How to fix it[/]: Remove the stale key(s) from conn-fields in provider.yaml.")
    return "\n".join(lines)
