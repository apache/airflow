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
"""Command functions for managing Airflow task instances via airflowctl."""

from __future__ import annotations

import sys

import rich

from airflowctl.api.datamodels.generated import ClearTaskInstancesBody
from airflowctl.api.client import (
    NEW_API_CLIENT,
    ClientKind,
    ServerResponseError,
    provide_api_client,
)
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def state(args, api_client=NEW_API_CLIENT) -> None:
    """
    Get the state of a single task instance.

    Implements the `airflowctl tasks state` command (issue #66174).
    """
    try:
        response = api_client.tasks.get_ti_state(
            dag_id=args.dag_id,
            dag_run_id=args.dag_run_id,
            task_id=args.task_id,
            map_index=args.map_index,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error fetching task state: {e}[/red]")
        sys.exit(1)

    AirflowConsole().print_as(
        data=[{"state": response.state.value if response.state else None, "task_id": response.task_id}],
        output=args.output,
    )

@provide_api_client(kind=ClientKind.CLI)
def states_for_dag_run(args, api_client=NEW_API_CLIENT) -> None:
    """Get the states of all task instances in a DAG run.

    Implements the `airflowctl tasks states-for-dag-run` command (issue #66175).
    """
    try:
        response = api_client.tasks.list_states_for_dag_run(
            dag_id=args.dag_id,
            dag_run_id=args.dag_run_id,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error fetching task states: {e}[/red]")
        sys.exit(1)

    rows = [
        {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": ti.dag_run_id,
            "state": ti.state.value if ti.state else "",
            "start_date": ti.start_date.isoformat() if ti.start_date else "",
            "end_date": ti.end_date.isoformat() if ti.end_date else "",
            "map_index": ti.map_index,
        }
        for ti in response.task_instances
    ]
    AirflowConsole().print_as(data=rows, output=args.output)

@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> None:
    """Clear task instances matching the given filters.

    Implements the `airflowctl tasks clear` command (issue #66176).
    """
    body = ClearTaskInstancesBody(
        dry_run=args.dry_run,
        only_failed=args.only_failed,
        only_running=args.only_running,
        include_upstream=args.upstream,
        include_downstream=args.downstream,
        include_past=args.include_past,
        include_future=args.include_future,
        reset_dag_runs=args.reset_dag_runs,
        dag_run_id=args.dag_run_id,
        task_ids=args.task_ids if args.task_ids else None,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    try:
        response = api_client.tasks.clear_tis(dag_id=args.dag_id, body=body)
    except ServerResponseError as e:
        rich.print(f"[red]Error clearing task instances: {e}[/red]")
        sys.exit(1)

    action_word = "Would clear" if args.dry_run else "Cleared"
    rich.print(
        f"[green]{action_word} {response.total_entries} task instance(s).[/green]"
    )
    if args.dry_run and response.task_instances:
        AirflowConsole().print_as(
            data=[
                {
                    "task_id": ti.task_id,
                    "run_id": ti.dag_run_id,
                    "state": ti.state.value if ti.state else "",
                }
                for ti in response.task_instances
            ],
            output=args.output,
        )