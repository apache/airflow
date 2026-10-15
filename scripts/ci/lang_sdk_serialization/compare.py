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
r"""
Check that a language SDK serializes Dags the way Airflow does.

Runs the SDK's serializer and serialize_python.py over test_dags.yaml, each writing its serialization
to a JSON file in a temporary directory. The Python side also takes the SDK's output as Airflow
receives it, filling in the fields the SDK leaves to Airflow's config, and loads it through Airflow's
deserializer. The two are then compared field by field, so this fails both when the serializers drift
apart and when Airflow cannot read what the SDK writes.

The SDK's serializer is the command after ``--``. It runs from the repository root, with the paths of
a copy of test_dags.yaml and of the JSON file to write appended, and writes each Dag as
``DagSerialization.to_dict`` would, keyed by Dag id. The Python side needs ``uv``::

    python3 scripts/ci/lang_sdk_serialization/compare.py --sdk typescript -- \
        pnpm --dir ts-sdk exec tsx tests/conformance/serialize_typescript.ts

A Dag of test_dags.yaml may list the features that it ``requires`` of an SDK. ``--supports`` names the
features the SDK has, as a comma-separated list or ``all``, and the copy of test_dags.yaml holds only
the Dags whose features are all among them. A Dag that requires nothing is always in it, and the SDK
has to write exactly the Dags of the copy.

Each SDK's own prek hook runs it this way, such as
java-sdk/scripts/ci/prek/check_serialization_conformance.py for the Java SDK.

On a failure the files are kept, and their directory is printed.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
TEST_DAGS = HERE / "test_dags.yaml"
SCHEMA = REPO_ROOT / "airflow-core" / "src" / "airflow" / "serialization" / "schema.json"

# The features that a Dag of test_dags.yaml may require of an SDK. The comment at the top of
# test_dags.yaml says what each one adds to a Dag.
FEATURES = frozenset(
    {"branch", "switch", "edge_labels", "group_options", "trigger_dag_run", "literal_inputs"}
)

# Name the file a Dag was declared in: Python's own Dag file, or the SDK's bundle.
DAG_KEYS_NOT_COMPARED = frozenset({"fileloc", "relative_fileloc", "_processor_dags_folder"})

TASK_KEYS_NOT_COMPARED = frozenset(
    {
        # Operator identity: Python names the Python class that ran, a language SDK a fixed pair.
        # Neither is ever imported on the Airflow side.
        "task_type",
        "_task_module",
        # The SDK's marker for a task in its language, which Python has no equivalent for.
        "language",
        # Python bookkeeping for mapped tasks and retry policies, neither of which a language SDK's
        # Dag can declare.
        "_needs_expansion",
        "has_retry_policy",
        # The stub contract every language SDK task carries, which the Python operator built here
        # has no counterpart for. Loading the SDK's output through Airflow checks them.
        "is_stub",
        "_arg_bindings",
    }
)


def parse_features(value: str) -> frozenset[str]:
    """Read the value of ``--supports``: ``all``, or feature names separated by commas."""
    if value == "all":
        return FEATURES
    features = frozenset(name for name in value.split(",") if name)
    if unknown := features - FEATURES:
        raise SystemExit(f"--supports names {sorted(unknown)}, which are not among {sorted(FEATURES)}")
    return features


CASE_START = re.compile(r"^  - dag_id: (\S+)\s*$")
CASE_REQUIRES = re.compile(r"^    requires: \[([^\]]*)\]\s*$")


def find_comment_start(lines: list[str], index: int) -> int:
    """Return the index of the first of the comment lines right above ``lines[index]``."""
    while index > 0 and lines[index - 1].lstrip().startswith("#"):
        index -= 1
    return index


def filter_cases(text: str, supported: frozenset[str]) -> tuple[str, list[str]]:
    """
    Keep the Dags of test_dags.yaml whose required features are all supported.

    Works on the text, so the copy keeps the tags and the comments of the original and needs no YAML
    parser. A Dag starts at its ``  - dag_id:`` line, with the comment lines right above it, and writes
    ``requires`` as a list on one line. Returns the copy and the ids of the Dags in it.
    """
    lines = text.splitlines(keepends=True)
    starts = [index for index, line in enumerate(lines) if CASE_START.match(line)]
    # The comment that explains a Dag sits right above it, so it goes with the Dag.
    starts = [find_comment_start(lines, index) for index in starts]
    kept = lines[: starts[0]]
    ids = []
    for position, start in enumerate(starts):
        block = lines[start : starts[position + 1] if position + 1 < len(starts) else len(lines)]
        dag_id = next(match.group(1) for line in block if (match := CASE_START.match(line)))
        required: set[str] = set()
        for line in block:
            if match := CASE_REQUIRES.match(line):
                required = {name.strip() for name in match.group(1).split(",") if name.strip()}
        if unknown := required - FEATURES:
            raise SystemExit(
                f"Dag {dag_id} requires {sorted(unknown)}, which are not among {sorted(FEATURES)}"
            )
        if required <= supported:
            kept.extend(block)
            ids.append(dag_id)
    return "".join(kept), ids


def run(command: list[str]) -> None:
    if subprocess.run(command, cwd=REPO_ROOT, check=False).returncode:
        raise SystemExit(f"`{' '.join(command)}` failed")


def get_task_defaults() -> dict[str, Any]:
    """Map each task field the Dag schema gives a default to that default."""
    fields = json.loads(SCHEMA.read_text())["definitions"]["operator"]["properties"]
    return {key: field["default"] for key, field in fields.items() if field.get("default") is not None}


def normalize_numbers(value: Any) -> Any:
    """
    Read a JSON value with one number type, and a bool that is not a number.

    An SDK may write ``2`` where Python writes ``2.0``, as JavaScript does, which has one number type.
    """
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, list):
        return [normalize_numbers(item) for item in value]
    if isinstance(value, dict):
        return {key: normalize_numbers(item) for key, item in value.items()}
    return value


def is_same_json(python: Any, sdk: Any) -> bool:
    return normalize_numbers(python) == normalize_numbers(sdk)


def find_differences(path: str, python: Any, sdk: Any) -> list[str]:
    """List where two JSON values differ, down to the innermost key or index."""
    if isinstance(python, dict) and isinstance(sdk, dict):
        problems = []
        for key in sorted(python.keys() | sdk.keys()):
            if key not in sdk:
                problems.append(f"{path}.{key} is missing, Python writes {python[key]!r}")
            elif key not in python:
                problems.append(f"{path}.{key} is {sdk[key]!r}, which Python does not write")
            else:
                problems.extend(find_differences(f"{path}.{key}", python[key], sdk[key]))
        return problems
    if isinstance(python, list) and isinstance(sdk, list) and len(python) == len(sdk):
        return [
            problem
            for index, (python_item, sdk_item) in enumerate(zip(python, sdk))
            for problem in find_differences(f"{path}[{index}]", python_item, sdk_item)
        ]
    if not is_same_json(python, sdk):
        return [f"{path} is {sdk!r}, Python writes {python!r}"]
    return []


def compare_fields(
    python: dict[str, Any], sdk: dict[str, Any], not_compared: frozenset[str], defaults: dict[str, Any]
) -> list[str]:
    """
    Compare two serialized objects key by key.

    A key the SDK leaves out is fine when Python wrote its schema default. Python keeps such a value
    when its ``client_defaults`` table disagrees with the schema, a table a language SDK is never sent,
    and Airflow reads a missing field as its default anyway.
    """
    problems = []
    for key in sorted((python.keys() | sdk.keys()) - not_compared):
        if key in python and key in sdk:
            problems.extend(find_differences(key, python[key], sdk[key]))
        elif key in sdk:
            problems.append(f"{key} is {sdk[key]!r}, which Python does not write")
        elif key not in defaults or not is_same_json(python[key], defaults[key]):
            problems.append(f"{key} is missing, Python writes {python[key]!r}")
    return problems


def compare_dag(python: dict[str, Any], sdk: dict[str, Any], defaults: dict[str, Any]) -> list[str]:
    problems = compare_fields(python, sdk, DAG_KEYS_NOT_COMPARED | {"tasks"}, {})
    python_ids = [task["__var"]["task_id"] for task in python["tasks"]]
    sdk_ids = [task["__var"]["task_id"] for task in sdk["tasks"]]
    if sdk_ids != python_ids:
        return [*problems, f"tasks are {sdk_ids}, Python writes {python_ids}"]
    for python_task, sdk_task in zip(python["tasks"], sdk["tasks"]):
        task_id = python_task["__var"]["task_id"]
        if sdk_task["__type"] != python_task["__type"]:
            problems.append(f"task {task_id} is a {sdk_task['__type']!r}, not a {python_task['__type']!r}")
        problems.extend(
            f"task {task_id}: {problem}"
            for problem in compare_fields(
                python_task["__var"], sdk_task["__var"], TASK_KEYS_NOT_COMPARED, defaults
            )
        )
    return problems


def compare(python: dict[str, Any], sdk: dict[str, Any], task_defaults: dict[str, Any]) -> list[str]:
    """List every way the SDK's serialization differs from Python's."""
    if sdk.keys() != python.keys():
        return [f"the Dags are {sorted(sdk)}, Python writes {sorted(python)}"]
    problems = []
    for dag_id, python_dag in python.items():
        sdk_dag = sdk[dag_id]
        if sdk_dag["__version"] != python_dag["__version"]:
            problems.append(
                f"{dag_id}: __version is {sdk_dag['__version']}, Python writes {python_dag['__version']}"
            )
        problems.extend(
            f"{dag_id}: {problem}"
            for problem in compare_dag(python_dag["dag"], sdk_dag["dag"], task_defaults)
        )
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--sdk", required=True, help="the SDK's name, as in serialized_<sdk>.json")
    parser.add_argument(
        "--supports",
        default="",
        type=parse_features,
        help="the features the SDK has, as names separated by commas or `all`; none by default",
    )
    parser.add_argument("command", nargs="+", help="the SDK's serializer, after --")
    args = parser.parse_args(argv)

    directory = Path(tempfile.mkdtemp(prefix=f"{args.sdk}-serialization-"))
    test_dags = directory / TEST_DAGS.name
    python_output = directory / "serialized_python.json"
    sdk_output = directory / f"serialized_{args.sdk}.json"
    received_output = directory / f"received_{args.sdk}.json"
    filtered, dag_ids = filter_cases(TEST_DAGS.read_text(), args.supports)
    test_dags.write_text(filtered)
    run([*args.command, str(test_dags), str(sdk_output)])
    if (written := sorted(json.loads(sdk_output.read_text()))) != sorted(dag_ids):
        print(
            f"The {args.sdk} SDK wrote the Dags {written}, but it supports {sorted(args.supports)} and so "
            f"should write {sorted(dag_ids)}. The files are kept in {directory}",
            file=sys.stderr,
        )
        return 1
    run(
        [
            "uv",
            "run",
            "--project",
            "airflow-core",
            # airflow-core's dev group pulls in providers and extras that build native code; the
            # serializer only needs airflow-core itself.
            "--no-dev",
            "python",
            str(HERE / "serialize_python.py"),
            str(test_dags),
            str(python_output),
            "--receive",
            str(sdk_output),
            str(received_output),
        ]
    )

    python = json.loads(python_output.read_text())
    problems = compare(python, json.loads(received_output.read_text()), get_task_defaults())
    if problems:
        print(
            f"The {args.sdk} serialization differs from Python's in {len(problems)} place(s):",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  {problem}", file=sys.stderr)
        print(f"The serializations are kept in {directory}", file=sys.stderr)
        return 1
    shutil.rmtree(directory)
    print(
        f"The {args.sdk} SDK serializes the {len(python)} Dags of {TEST_DAGS.name} it supports as Python does"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
