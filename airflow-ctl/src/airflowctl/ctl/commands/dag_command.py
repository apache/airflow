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
from typing import Literal

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import DAGPatchBody
from airflowctl.ctl.console_formatting import AirflowConsole


# --- Exception class for validation errors ---
class FieldValidationError(Exception):
    """Raised when a required argument is missing."""
    pass


# --- Helper function to update DAG state ---
def update_dag_state(
    dag_id: str,
    operation: Literal["pause", "unpause"],
    api_client,
    output: str,
):
    """Update DAG state (pause/unpause)."""
    try:
        response = api_client.dags.update(
            dag_id=dag_id,
            dag_body=DAGPatchBody(is_paused=operation == "pause")
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error while trying to {operation} DAG {dag_id}: {e}[/red]")
        sys.exit(1)

    # response may already be a dict from API client
    if hasattr(response, "model_dump"):
        response_dict = response.model_dump()
    else:
        response_dict = response  # use dict directly

    rich.print(f"[green]DAG {operation} successful: {dag_id}[/green]")
    rich.print("[green]Further DAG details:[/green]")
    AirflowConsole().print_as(
        data=[response_dict],
        output=output,
    )
    return response_dict


# --- Pause function with validation ---
@provide_api_client(kind=ClientKind.CLI)
def pause(args, api_client=NEW_API_CLIENT) -> None:
    """Pause a DAG with validation."""
    if not args.dag_id:
        raise FieldValidationError(
            "Missing required parameter(s): --dag-id\n"
            "Use 'airflowctl dags pause --help' for usage information."
        )
    return update_dag_state(
        dag_id=args.dag_id,
        operation="pause",
        api_client=api_client,
        output=args.output,
    )


# --- Unpause function with validation ---
@provide_api_client(kind=ClientKind.CLI)
def unpause(args, api_client=NEW_API_CLIENT) -> None:
    """Unpause a DAG with validation."""
    if not args.dag_id:
        raise FieldValidationError(
            "Missing required parameter(s): --dag-id\n"
            "Use 'airflowctl dags unpause --help' for usage information."
        )
    return update_dag_state(
        dag_id=args.dag_id,
        operation="unpause",
        api_client=api_client,
        output=args.output,
    )
