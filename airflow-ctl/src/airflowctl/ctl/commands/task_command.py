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

import datetime
import sys
from typing import TYPE_CHECKING

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import TaskInstanceState
from airflowctl.ctl.console_formatting import AirflowConsole

if TYPE_CHECKING:
    from airflowctl.api.datamodels.generated import TaskInstanceResponse


def _find_run_id_by_logical_date(api_client, dag_id: str, value: str) -> str:
    """Find the run ID of the Dag run with an exact logical date match."""
    try:
        logical_date = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        rich.print(f"[red]Invalid --logical-date: {value!r}[/red]")
        sys.exit(1)
    if logical_date.tzinfo is None:
        rich.print("[red]--logical-date must include a timezone offset[/red]")
        sys.exit(1)

    dag_runs = []
    try:
        dag_runs = api_client.dag_runs.list(
            dag_id=dag_id,
            logical_date_gte=logical_date,
            logical_date_lte=logical_date,
            order_by="-id",
            limit=1,
            suppress_error_log=True,
        ).dag_runs
    except ServerResponseError as e:
        if e.response.status_code != 404:
            raise
    if not dag_runs:
        rich.print(f"[red]Dag run for {dag_id} with logical date {value!r} not found[/red]")
        sys.exit(1)
    return dag_runs[0].dag_run_id


def _format_task_instance(ti: TaskInstanceResponse, has_mapped_instances: bool) -> dict[str, str]:
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
    return data


@provide_api_client(kind=ClientKind.CLI)
def failed_deps(args, api_client=NEW_API_CLIENT) -> None:
    """Get task instance dependencies that were not met, from the scheduler's perspective."""
    if (args.run_id is None) == (args.logical_date is None):
        rich.print("[red]Provide either run_id or --logical-date, but not both[/red]")
        sys.exit(1)

    run_id = args.run_id or _find_run_id_by_logical_date(api_client, args.dag_id, args.logical_date)

    try:
        response = api_client.task_instances.get_dependencies(
            dag_id=args.dag_id,
            dag_run_id=run_id,
            task_id=args.task_id,
            map_index=args.map_index,
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
            suppress_error_log=True,
        )
    except ServerResponseError as e:
        if e.response.status_code == 404:
            map_index_part = f" with map index {args.map_index}" if args.map_index >= 0 else ""
            rich.print(
                f"[red]Task instance for task {args.task_id!r}{map_index_part} in Dag run "
                f"{run_id!r} of Dag {args.dag_id!r} not found[/red]"
            )
            sys.exit(1)
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
    if (args.run_id is None) == (args.logical_date is None):
        rich.print("[red]Provide either run_id or --logical-date, but not both[/red]")
        sys.exit(1)

    run_id = args.run_id or _find_run_id_by_logical_date(api_client, args.dag_id, args.logical_date)

    try:
        task_instances = api_client.task_instances.list(dag_id=args.dag_id, dag_run_id=run_id).task_instances
    except ServerResponseError as e:
        if e.response.status_code == 404:
            rich.print(f"[red]Dag run {run_id!r} of Dag {args.dag_id!r} not found[/red]")
            sys.exit(1)
        raise

    has_mapped_instances = any(ti.map_index >= 0 for ti in task_instances)

    AirflowConsole().print_as(
        data=[_format_task_instance(ti, has_mapped_instances) for ti in task_instances],
        output=args.output,
    )
