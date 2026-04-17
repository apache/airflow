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
Compare serialised DAG output from Python and Java SDKs.

Usage:
    python compare.py serialized_python.json serialized_java.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Fields that are inherently environment-specific and should be ignored.
IGNORED_FIELDS = frozenset(
    {
        "fileloc",
        "relative_fileloc",
        "_processor_dags_folder",
    }
)

# Floating-point tolerance for timestamp / duration comparisons.
FLOAT_TOLERANCE = 1e-6


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def _normalise(obj, *, _depth: int = 0):
    """Recursively normalise an object for comparison."""
    if isinstance(obj, dict):
        return {
            k: _normalise(v, _depth=_depth + 1) for k, v in sorted(obj.items()) if k not in IGNORED_FIELDS
        }
    if isinstance(obj, list):
        return [_normalise(item, _depth=_depth + 1) for item in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj


# ---------------------------------------------------------------------------
# Deep diff
# ---------------------------------------------------------------------------


def _deep_diff(python_obj, java_obj, path: str = "") -> list[str]:
    """Return a list of human-readable difference descriptions."""
    diffs: list[str] = []

    if type(python_obj) is not type(java_obj):
        # Allow int ↔ float (e.g. 0 vs 0.0)
        if isinstance(python_obj, (int, float)) and isinstance(java_obj, (int, float)):
            if abs(float(python_obj) - float(java_obj)) > FLOAT_TOLERANCE:
                diffs.append(f"{path}: {python_obj!r} != {java_obj!r}")
            return diffs
        diffs.append(
            f"{path}: type mismatch — Python {type(python_obj).__name__}"
            f" vs Java {type(java_obj).__name__}"
            f"  (py={python_obj!r}, java={java_obj!r})"
        )
        return diffs

    if isinstance(python_obj, dict):
        all_keys = set(python_obj) | set(java_obj)
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in java_obj:
                diffs.append(f"{child_path}: present in Python but missing in Java")
            elif key not in python_obj:
                diffs.append(f"{child_path}: present in Java but missing in Python")
            else:
                diffs.extend(_deep_diff(python_obj[key], java_obj[key], child_path))
    elif isinstance(python_obj, list):
        if len(python_obj) != len(java_obj):
            diffs.append(f"{path}: list length — Python {len(python_obj)} vs Java {len(java_obj)}")
        for i, (p, j) in enumerate(zip(python_obj, java_obj)):
            diffs.extend(_deep_diff(p, j, f"{path}[{i}]"))
    elif isinstance(python_obj, float):
        if abs(python_obj - java_obj) > FLOAT_TOLERANCE:
            diffs.append(f"{path}: {python_obj!r} != {java_obj!r}")
    elif python_obj != java_obj:
        diffs.append(f"{path}: {python_obj!r} != {java_obj!r}")

    return diffs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <python.json> <java.json>")
        sys.exit(1)

    python_path = Path(sys.argv[1])
    java_path = Path(sys.argv[2])

    with open(python_path) as fh:
        python_data: dict = json.load(fh)
    with open(java_path) as fh:
        java_data: dict = json.load(fh)

    all_names = sorted(set(python_data) | set(java_data))
    total = len(all_names)
    passed = 0
    failed = 0

    for name in all_names:
        if name not in python_data:
            print(f"SKIP  {name}  (missing in Python output)")
            continue
        if name not in java_data:
            print(f"SKIP  {name}  (missing in Java output)")
            continue

        py_dag = python_data[name]
        jv_dag = java_data[name]

        # Skip error entries
        if isinstance(py_dag, dict) and "__error" in py_dag:
            print(f"SKIP  {name}  (Python error: {py_dag['__error']})")
            continue
        if isinstance(jv_dag, dict) and "__error" in jv_dag:
            print(f"SKIP  {name}  (Java error: {jv_dag['__error']})")
            continue

        py_norm = _normalise(py_dag)
        jv_norm = _normalise(jv_dag)

        diffs = _deep_diff(py_norm, jv_norm)
        if diffs:
            failed += 1
            print(f"FAIL  {name}")
            for d in diffs:
                print(f"        {d}")
        else:
            passed += 1
            print(f"PASS  {name}")

    print(f"\n{'=' * 60}")
    print(f"Total: {total}  |  Passed: {passed}  |  Failed: {failed}")
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()
