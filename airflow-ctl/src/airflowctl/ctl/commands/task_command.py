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

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

import rich
from rich.markup import escape

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import StructuredLogMessage, TaskInstanceState
from airflowctl.ctl.console_formatting import AirflowConsole
from airflowctl.ctl.utils.dag_run import resolve_dag_run_id

if TYPE_CHECKING:
    from argparse import Namespace

    from airflowctl.api.datamodels.generated import TaskInstanceResponse

# The API's answer when a task has several task instances in a task loop and the lookup did not pick one.
REGION_NOT_SELECTED_DETAIL = "Select a region and index for this task instance"


def _error_detail(error: ServerResponseError) -> str | None:
    try:
        detail = error.response.json().get("detail")
    except ValueError:
        return None
    return detail if isinstance(detail, str) else None


def _is_mapped_task_error(error: ServerResponseError) -> bool:
    """Whether the API rejected the lookup because the task is mapped and no map index was given."""
    detail = _error_detail(error)
    return detail is not None and "is mapped" in detail


def _region_selection(args: Namespace) -> dict[str, Any]:
    """Return the region a task instance lookup selects, exiting unless both or neither part is given."""
    if (args.region_id is None) != (args.region_index is None):
        rich.print("[red]Pass --region-id and --region-index together[/red]")
        sys.exit(1)
    if args.region_id is None:
        return {}
    return {"region_id": str(args.region_id), "region_index": args.region_index}


def _task_instance_not_found_message(
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int,
    error: ServerResponseError,
    region_id: str | None = None,
    region_index: int | None = None,
) -> str:
    """Build the message shown when a task instance is not found."""
    map_index_part = f" with map index {map_index}" if map_index >= 0 else ""
    if region_id is not None:
        map_index_part += f" in region {region_id} at index {region_index}"
    message = (
        f"Task instance for task {task_id!r}{map_index_part} in Dag run "
        f"{run_id!r} of Dag {dag_id!r} not found"
    )
    if map_index < 0 and _is_mapped_task_error(error):
        message += ". The task is mapped; pass --map-index to select one of its task instances"
    return message


def _exit_if_task_instance_not_selected(args: Namespace, run_id: str, error: ServerResponseError) -> None:
    """Print why the lookup did not find one task instance and exit, unless the error means something else."""
    if error.response.status_code == 404:
        message = _task_instance_not_found_message(
            args.dag_id, run_id, args.task_id, args.map_index, error, **_region_selection(args)
        )
    elif error.response.status_code == 400 and _error_detail(error) == REGION_NOT_SELECTED_DETAIL:
        message = (
            f"Task {args.task_id!r} in Dag run {run_id!r} of Dag {args.dag_id!r} runs inside a task loop; "
            "pass --region-id and --region-index to select one of its task instances. "
            "'airflowctl tasks states-for-dag-run' lists them"
        )
    else:
        return
    rich.print(f"[red]{escape(message)}[/red]")
    sys.exit(1)


def _format_task_instance(
    ti: TaskInstanceResponse, has_mapped_instances: bool, has_loop_instances: bool
) -> dict[str, str]:
    data = {
        "dag_id": ti.dag_id,
        "logical_date": ti.logical_date.isoformat() if ti.logical_date else "",
        "task_id": ti.task_id,
        "state": ti.state.value if ti.state else "",
        "start_date": ti.start_date.isoformat() if ti.start_date else "",
        "end_date": ti.end_date.isoformat() if ti.end_date else "",
    }
    if has_mapped_instances:
        data["map_index"] = str(ti.map_index) if ti.map_index >= 0 else ""
    if has_loop_instances:
        # The coordinates --region-id and --region-index take. Only task instances in a loop need them;
        # any other one is selected by its task id and map index.
        in_region = ti.in_loop and ti.region_id is not None and ti.region_index is not None
        data["region_id"] = str(ti.region_id) if in_region else ""
        data["region_index"] = str(ti.region_index) if in_region else ""
    return data


@provide_api_client(kind=ClientKind.CLI)
def failed_deps(args, api_client=NEW_API_CLIENT) -> None:
    """Get task instance dependencies that were not met, from the scheduler's perspective."""
    region = _region_selection(args)
    run_id = resolve_dag_run_id(api_client, args)

    try:
        response = api_client.task_instances.get_dependencies(
            dag_id=args.dag_id,
            dag_run_id=run_id,
            task_id=args.task_id,
            map_index=args.map_index,
            **region,
            suppress_error_log=True,
        )
        if response.dependencies:
            print("Task instance dependencies not met:")
            for dep in response.dependencies:
                print(f"{dep.name}: {dep.reason}")
            return

        # The API server evaluates scheduler dependencies only for task instances that have not been
        # queued yet, so an empty response for any other state does not mean the dependencies are met.
        task_instance = api_client.task_instances.get(
            dag_id=args.dag_id,
            dag_run_id=run_id,
            task_id=args.task_id,
            map_index=args.map_index,
            **region,
            suppress_error_log=True,
        )
    except ServerResponseError as e:
        _exit_if_task_instance_not_selected(args, run_id, e)
        raise

    if task_instance.state is None or task_instance.state == TaskInstanceState.SCHEDULED:
        print("Task instance dependencies are all met.")
    else:
        print(
            f"Task instance is in the '{task_instance.state.value}' state; scheduler dependencies "
            "are only evaluated for task instances that have not yet been queued."
        )


@provide_api_client(kind=ClientKind.CLI)
def states_for_dag_run(args, api_client=NEW_API_CLIENT) -> None:
    """Get the status of all task instances in a Dag run."""
    run_id = resolve_dag_run_id(api_client, args)

    try:
        task_instances = api_client.task_instances.list(dag_id=args.dag_id, dag_run_id=run_id).task_instances
    except ServerResponseError as e:
        if e.response.status_code == 404:
            rich.print(
                f"[red]Dag run {escape(repr(run_id))} of Dag {escape(repr(args.dag_id))} not found[/red]"
            )
            sys.exit(1)
        raise

    has_mapped_instances = any(ti.map_index >= 0 for ti in task_instances)
    has_loop_instances = any(ti.in_loop for ti in task_instances)

    AirflowConsole().print_as(
        data=[_format_task_instance(ti, has_mapped_instances, has_loop_instances) for ti in task_instances],
        output=args.output,
    )


@provide_api_client(kind=ClientKind.CLI)
def state(args, api_client=NEW_API_CLIENT) -> None:
    """Get the state of a task instance."""
    region = _region_selection(args)
    run_id = resolve_dag_run_id(api_client, args)

    try:
        task_instance = api_client.task_instances.get(
            dag_id=args.dag_id,
            dag_run_id=run_id,
            task_id=args.task_id,
            map_index=args.map_index,
            **region,
            suppress_error_log=True,
        )
    except ServerResponseError as e:
        _exit_if_task_instance_not_selected(args, run_id, e)
        raise

    # Unset states print as None to stay drop-in compatible with the deprecated
    # `airflow tasks state`, which prints its nullable `ti.state` column directly.
    print(task_instance.state.value if task_instance.state else None)


@provide_api_client(kind=ClientKind.CLI)
def logs(args, api_client=NEW_API_CLIENT) -> None:
    """Get the logs of a task instance for a given try."""
    region = _region_selection(args)
    try:
        response = api_client.task_instances.logs(
            dag_id=args.dag_id,
            dag_run_id=args.dag_run_id,
            task_id=args.task_id,
            try_number=args.try_number,
            map_index=args.map_index,
            **region,
            suppress_error_log=True,
        )
    except ServerResponseError as e:
        _exit_if_task_instance_not_selected(args, args.dag_run_id, e)
        raise

    for entry in response.content:
        print(entry.event if isinstance(entry, StructuredLogMessage) else entry)
