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
import json
import sys
from typing import Literal

import rich
from rich.text import Text

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import DAGPatchBody, DAGRunResponse
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
    """Show next scheduled execution time for a DAG."""
    try:
        response = api_client.dags.get(dag_id=args.dag_id)
    except ServerResponseError as e:
        rich.print(f"[red]Error retrieving DAG {args.dag_id}: {e}[/red]")
        sys.exit(1)

    next_exec_data = {field: getattr(response, field) for field in _NEXT_EXECUTION_FIELDS}

    if all(value is None for value in next_exec_data.values()):
        rich.print(f"[yellow]No upcoming run scheduled for DAG {args.dag_id}.[/yellow]")
        return None

    result = next_exec_data
    AirflowConsole().print_as(
        data=[result],
        output=args.output,
    )
    return result


def _parse_logical_date(value: str) -> datetime.datetime | None:
    """Parse an ISO-formatted logical date."""
    try:
        logical_date = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if logical_date.tzinfo is None:
        raise SystemExit("Logical date must include a timezone offset")
    return logical_date


def _get_dag_run_by_run_id_or_logical_date(api_client, dag_id: str, value: str) -> DAGRunResponse | None:
    """Get a Dag run by run ID, falling back to an exact logical date match."""
    try:
        return api_client.dag_runs.get(dag_id=dag_id, dag_run_id=value, suppress_error_log=True)
    except ServerResponseError as e:
        if e.response.status_code != 404:
            raise

    if logical_date := _parse_logical_date(value):
        response = api_client.dag_runs.list(
            dag_id=dag_id,
            logical_date_gte=logical_date,
            logical_date_lte=logical_date,
            order_by="-id",
            limit=1,
        )
        if response.dag_runs:
            return response.dag_runs[0]
    else:
        api_client.dag_runs.list(dag_id=dag_id, limit=1)
    return None


@provide_api_client(kind=ClientKind.CLI)
def state(args, api_client=NEW_API_CLIENT) -> None:
    """Show the state and configuration of a Dag run."""
    dag_run = _get_dag_run_by_run_id_or_logical_date(
        api_client=api_client,
        dag_id=args.dag_id,
        value=args.logical_date_or_run_id,
    )
    if not dag_run:
        rich.print("[yellow]No matching Dag run found.[/yellow]")
    else:
        state_value = getattr(dag_run.state, "value", dag_run.state)
        if dag_run.conf:
            rich.print(Text(f"{state_value}, {json.dumps(dag_run.conf)}"))
        else:
            rich.print(Text(state_value))
