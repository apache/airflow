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
Serialize the Dags of test_dags.yaml with Airflow's own serializer.

Each Dag is built with the Python authoring API and written as ``DagSerialization.to_dict`` returns it,
keyed by Dag id. ``--receive`` also takes a language SDK's output as Airflow receives it: it fills in the
Dag fields the SDK leaves to Airflow's config with ``DagSerialization.fill_config_defaults``, writes the
result, and checks every Dag with ``DagSerialization.validate_serialized_dag``. compare.py runs it as::

    uv run --project airflow-core --no-dev python scripts/ci/lang_sdk_serialization/serialize_python.py \
        scripts/ci/lang_sdk_serialization/test_dags.yaml serialized_python.json \
        --receive serialized_typescript.json received_typescript.json
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Any

import yaml

from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, BaseOperator, Label, TaskGroup
from airflow.sdk.bases.branch import BaseBranchOperator
from airflow.serialization.serialized_objects import DagSerialization


class NoopOperator(BaseOperator):
    """Stands in for a language SDK's task: a task with no Python behaviour."""

    def execute(self, context):
        return None


class NoopBranchOperator(BaseBranchOperator):
    """Stands in for a language SDK's condition or switch, which skips the tasks it does not choose."""

    def choose_branch(self, context):
        return None


def construct_datetime(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> datetime.datetime:
    return datetime.datetime.fromisoformat(loader.construct_scalar(node))


def construct_timedelta(loader: yaml.SafeLoader, node: yaml.ScalarNode) -> datetime.timedelta:
    return datetime.timedelta(seconds=float(loader.construct_scalar(node)))


yaml.SafeLoader.add_constructor("!datetime", construct_datetime)
yaml.SafeLoader.add_constructor("!timedelta", construct_timedelta)


def build_task(dag: DAG, task: dict[str, Any], group: TaskGroup | None) -> BaseOperator:
    """Build one task of a case with the operator that stands for it."""
    task_id = task["task_id"]
    spec = task.get("spec", {})
    if "trigger_dag_run" in task:
        return TriggerDagRunOperator(
            task_id=task_id, dag=dag, task_group=group, **task["trigger_dag_run"], **spec
        )
    operator = NoopBranchOperator if "branch" in task else NoopOperator
    return operator(task_id=task_id, dag=dag, task_group=group, **spec)


def build_dag(case: dict[str, Any]) -> DAG:
    dag = DAG(case["dag_id"], **case.get("spec", {}))
    groups: dict[str, TaskGroup] = {}
    for entry in case.get("groups", []):
        # A group id is fully qualified, so its parent is whatever comes before the last dot.
        group_id, spec = (entry, {}) if isinstance(entry, str) else (entry["id"], entry.get("spec", {}))
        parent_id, _, local_id = group_id.rpartition(".")
        groups[group_id] = TaskGroup(
            local_id, dag=dag, parent_group=groups[parent_id] if parent_id else None, **spec
        )
    # A task is named by its group and its own id even when its group leaves the group id off the
    # id the task gets, as prefix_group_id=False does.
    tasks: dict[str, BaseOperator] = {}
    for task in case["tasks"]:
        group_id = task.get("group")
        qualified_id = f"{group_id}.{task['task_id']}" if group_id else task["task_id"]
        tasks[qualified_id] = build_task(dag, task, groups.get(group_id))
    for qualified_id, task in zip(tasks, case["tasks"]):
        for upstream in task.get("upstream", []):
            tasks[upstream] >> tasks[qualified_id]
        branch = task.get("branch", {})
        for chosen in [branch.get("then"), branch.get("else"), *branch.get("cases", [])]:
            if chosen is not None:
                tasks[qualified_id] >> tasks[chosen]
    for upstream, downstream, *label in case.get("order_edges", []):
        edge = get_node(tasks, groups, upstream)
        if label:
            edge = edge >> Label(label[0])
        edge >> get_node(tasks, groups, downstream)
    return dag


def get_node(tasks: dict[str, BaseOperator], groups: dict[str, TaskGroup], node_id: str):
    """Resolve an edge endpoint: the task group with that id if there is one, else the task."""
    return groups[node_id] if node_id in groups else tasks[node_id]


def receive(sdk_output: Path, received_output: Path) -> None:
    received = json.loads(sdk_output.read_text())
    for data in received.values():
        DagSerialization.fill_config_defaults(data)
    received_output.write_text(json.dumps(received, indent=2) + "\n")
    for dag_id, data in received.items():
        try:
            DagSerialization.validate_serialized_dag(data)
        except Exception:
            print(f"Airflow cannot load Dag {dag_id!r} as the SDK wrote it", file=sys.stderr)
            raise


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("test_dags", type=Path, help="the test cases, test_dags.yaml")
    parser.add_argument("output", type=Path, help="the JSON file to write")
    parser.add_argument(
        "--receive",
        nargs=2,
        type=Path,
        metavar=("SDK_OUTPUT", "RECEIVED_OUTPUT"),
        help="a JSON file a language SDK wrote, and where to write it as Airflow receives it",
    )
    args = parser.parse_args()

    cases = yaml.safe_load(args.test_dags.read_text())["dags"]
    serialized = {case["dag_id"]: DagSerialization.to_dict(build_dag(case)) for case in cases}
    args.output.write_text(json.dumps(serialized, indent=2) + "\n")
    if args.receive:
        receive(*args.receive)


if __name__ == "__main__":
    main()
