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
from typing import Literal

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import DAGPatchBody
from airflowctl.ctl.console_formatting import AirflowConsole


def update_dag_state(
    dag_id: str,
    operation: Literal["pause", "unpause"],
    api_client,
    output: str,
):
    """Update Dag state (pause/unpause)."""
    try:
        response = api_client.dags.update(
            dag_id=dag_id, dag_body=DAGPatchBody(is_paused=operation == "pause")
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error while trying to {operation} Dag {dag_id}: {e}[/red]")
        sys.exit(1)

    response_dict = response.model_dump()
    rich.print(f"[green]Dag {operation} successful {dag_id}[/green]")
    rich.print("[green]Further Dag details:[/green]")
    AirflowConsole().print_as(
        data=[response_dict],
        output=output,
    )
    return response_dict


@provide_api_client(kind=ClientKind.CLI)
def pause(args, api_client=NEW_API_CLIENT) -> None:
    """Pause a Dag."""
    return update_dag_state(
        dag_id=args.dag_id,
        operation="pause",
        api_client=api_client,
        output=args.output,
    )


@provide_api_client(kind=ClientKind.CLI)
def unpause(args, api_client=NEW_API_CLIENT) -> None:
    """Unpause a Dag."""
    return update_dag_state(
        dag_id=args.dag_id,
        operation="unpause",
        api_client=api_client,
        output=args.output,
    )


_NEXT_EXECUTION_FIELDS = (
    "next_dagrun_logical_date",
    "next_dagrun_data_interval_start",
    "next_dagrun_data_interval_end",
    "next_dagrun_run_after",
)


@provide_api_client(kind=ClientKind.CLI)
def next_execution(args, api_client=NEW_API_CLIENT) -> dict | None:
    """Show next scheduled execution time for a Dag."""
    try:
        response = api_client.dags.get(dag_id=args.dag_id)
    except ServerResponseError as e:
        rich.print(f"[red]Error retrieving Dag {args.dag_id}: {e}[/red]")
        sys.exit(1)

    next_exec_data = {field: getattr(response, field) for field in _NEXT_EXECUTION_FIELDS}

    if all(value is None for value in next_exec_data.values()):
        rich.print(f"[yellow]No upcoming run scheduled for Dag {args.dag_id}.[/yellow]")
        return None

    result = next_exec_data
    AirflowConsole().print_as(
        data=[result],
        output=args.output,
    )
    return result


def _parse_partition_date(value: str | None, *, option: str) -> datetime.date | None:
    if value is None:
        return None

    try:
        if "T" not in value and " " not in value:
            return datetime.date.fromisoformat(value)

        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        raise SystemExit(
            f"Invalid {option}: {value!r}. Use YYYY-MM-DD or ISO 8601 datetime; only the date is used."
        ) from None


def _validate_clear_args(args) -> None:
    has_run_id = args.run_id is not None
    has_partition_key = args.partition_key is not None
    has_partition_date = args.partition_date_start is not None or args.partition_date_end is not None

    if sum([has_run_id, has_partition_key, has_partition_date]) != 1:
        raise SystemExit(
            "Exactly one selector is required: --run-id, --partition-key, "
            "or --partition-date-start with --partition-date-end."
        )
    if has_partition_date and (args.partition_date_start is None or args.partition_date_end is None):
        raise SystemExit("--partition-date-start and --partition-date-end must be provided together.")
    if args.only_failed and args.only_running:
        raise SystemExit("--only-failed and --only-running are mutually exclusive.")


def _list_dag_runs(api_client, dag_id: str, *, order_by: str = "logical_date", **filters) -> list:
    dag_runs = []
    offset = 0
    while True:
        response = api_client.dag_runs.list(
            dag_id=dag_id,
            offset=offset,
            order_by=order_by,
            **filters,
        )
        page_dag_runs = response.dag_runs
        dag_runs.extend(page_dag_runs)

        offset += len(page_dag_runs)
        if not page_dag_runs or response.total_entries is None or offset >= response.total_entries:
            return dag_runs


def _get_dag_runs_to_clear(args, api_client) -> list:
    if args.run_id is not None:
        return [api_client.dag_runs.get(dag_id=args.dag_id, dag_run_id=args.run_id)]

    if args.partition_key is not None:
        return [
            dag_run
            for dag_run in _list_dag_runs(
                api_client,
                args.dag_id,
                order_by="partition_date",
                partition_key_pattern=args.partition_key,
            )
            if dag_run.partition_key == args.partition_key
        ]

    partition_date_start = _parse_partition_date(args.partition_date_start, option="--partition-date-start")
    partition_date_end = _parse_partition_date(args.partition_date_end, option="--partition-date-end")
    if partition_date_start is not None and partition_date_end is not None:
        if partition_date_start > partition_date_end:
            raise SystemExit("--partition-date-start must be before or equal to --partition-date-end.")
        return _list_dag_runs(
            api_client,
            args.dag_id,
            order_by="partition_date",
            partition_date_start=partition_date_start,
            partition_date_end=partition_date_end,
        )

    return []


def _print_dag_runs_to_clear(dag_id: str, dag_runs: list) -> None:
    rich.print(f"[yellow]Dag:[/yellow] {dag_id}")
    rich.print(f"[yellow]Dag runs to clear:[/yellow] {len(dag_runs)}")
    for dag_run in dag_runs:
        logical_date = dag_run.logical_date.isoformat() if dag_run.logical_date is not None else "-"
        partition_date = getattr(dag_run, "partition_date", None)
        partition_date_display = partition_date.isoformat() if partition_date is not None else "-"
        rich.print(
            f"  - {dag_run.dag_run_id} (logical date: {logical_date}, partition date: {partition_date_display})"
        )


def _confirm_clear(dag_id: str, dag_runs: list) -> bool:
    _print_dag_runs_to_clear(dag_id, dag_runs)
    answer = input("Clear task instances for these Dag runs? [y/N] ")
    return answer.strip().lower() in {"y", "yes"}


def _get_dag_run_sort_key(dag_run) -> tuple[str, str, str]:
    partition_date: datetime.datetime | None = getattr(dag_run, "partition_date", None)
    logical_date: datetime.datetime | None = dag_run.logical_date
    return (
        partition_date.isoformat() if partition_date is not None else "",
        logical_date.isoformat() if logical_date is not None else "",
        dag_run.dag_run_id,
    )


@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> dict[str, int | bool]:
    """Clear task instances for selected Dag runs."""
    _validate_clear_args(args)

    dag_runs = _get_dag_runs_to_clear(args, api_client)
    if not dag_runs:
        rich.print(f"[yellow]No matching Dag runs found for {args.dag_id}.[/yellow]")
        return {"dag_run_count": 0, "cleared_task_instances": 0}

    dag_runs = sorted(dag_runs, key=_get_dag_run_sort_key)

    if not args.yes and not _confirm_clear(args.dag_id, dag_runs):
        rich.print("[yellow]Cancelled.[/yellow]")
        return {"dag_run_count": len(dag_runs), "cleared_task_instances": 0, "cancelled": True}

    cleared_task_instances = 0
    for dag_run in dag_runs:
        response = api_client.dag_runs._clear_task_instances(
            dag_id=args.dag_id,
            dag_run_id=dag_run.dag_run_id,
            dry_run=False,
            only_failed=args.only_failed,
            only_running=args.only_running,
        )
        cleared_task_instances += response.total_entries or 0

    rich.print(
        f"[green]Cleared {cleared_task_instances} task instance(s) across {len(dag_runs)} Dag run(s).[/green]"
    )
    return {"dag_run_count": len(dag_runs), "cleared_task_instances": cleared_task_instances}
