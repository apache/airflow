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
"""Task commands for airflowctl."""
from __future__ import annotations

import sys

import rich

from airflowctl.api.client import (
    NEW_API_CLIENT,
    ClientKind,
    ServerResponseError,
    provide_api_client,
)
from airflowctl.api.datamodels.generated import (
    ClearTaskInstancesBody,
    ClearTaskInstanceCollectionResponse,
)
from airflowctl.api.operations import TaskOperations
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> None:
    """Clear task instances for a DAG run."""
    try:
        body = ClearTaskInstancesBody(
            dry_run=getattr(args, "dry_run", False),
            only_failed=getattr(args, "only_failed", True),
            only_running=getattr(args, "only_running", False),
            reset_dag_runs=getattr(args, "reset_dag_runs", True),
            task_ids=getattr(args, "task_ids", None),
            dag_run_id=getattr(args, "dag_run_id", None),
            include_upstream=getattr(args, "upstream", False),
            include_downstream=getattr(args, "downstream", False),
            include_future=getattr(args, "include_future", False),
            include_past=getattr(args, "include_past", False),
        )
        response = api_client.client.post(
            f"dags/{args.dag_id}/clearTaskInstances",
            json=body.model_dump(mode="json", exclude_none=True),
        )
        if response.status_code >= 400:
            rich.print(f"[red]Error: {response.status_code} {response.text}[/red]")
            sys.exit(1)
        cleared = ClearTaskInstanceCollectionResponse.model_validate_json(response.content)
        rich.print(
            f"[green]Cleared {len(cleared.task_instances)} task instance(s) for DAG {args.dag_id}[/green]"
        )
        AirflowConsole().print_as(
            data=[t.model_dump() for t in cleared.task_instances],
            output=args.output,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error clearing task instances: {e}[/red]")
        sys.exit(1)


@provide_api_client(kind=ClientKind.CLI)
def states_for_dag_run(args, api_client=NEW_API_CLIENT) -> None:
    """Get task instance states for a DAG run."""
    try:
        ops = TaskOperations(client=api_client.client)
        tis = ops.states_for_dag_run(args.dag_id, args.dag_run_id, getattr(args, "limit", 100))
        rich.print(f"[green]Found {len(tis.task_instances)} task instance(s)[/green]")
        AirflowConsole().print_as(
            data=[t.model_dump() for t in tis.task_instances],
            output=args.output,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error getting task instances: {e}[/red]")
        sys.exit(1)
