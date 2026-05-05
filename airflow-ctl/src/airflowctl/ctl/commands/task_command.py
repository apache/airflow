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

from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> None:
    """Clear task instances for a Dag run."""
    try:
        task_ids = getattr(args, "task_ids", None)
        if task_ids:
            task_ids = task_ids.split(",")

        body = ClearTaskInstancesBody(
            dry_run=getattr(args, "dry_run", False) or False,
            only_failed=getattr(args, "only_failed", True) or True,
            only_running=getattr(args, "only_running", False) or False,
            reset_dag_runs=getattr(args, "reset_dag_runs", True) or True,
            task_ids=task_ids,
            dag_run_id=getattr(args, "dag_run_id", None),
            include_upstream=getattr(args, "upstream", False) or False,
            include_downstream=getattr(args, "downstream", False) or False,
            include_future=getattr(args, "include_future", False) or False,
            include_past=getattr(args, "include_past", False) or False,
        )
        cleared = api_client.client.tasks.clear(dag_id=args.dag_id, body=body)
        rich.print(
            f"[green]Cleared {len(cleared.task_instances)} task instance(s) for Dag {args.dag_id}[/green]"
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
    """Get task instance states for a Dag run."""
    try:
        tis = api_client.client.tasks.list(dag_id=args.dag_id, dag_run_id=args.dag_run_id)
        rich.print(f"[green]Found {len(tis.task_instances)} task instance(s)[/green]")
        AirflowConsole().print_as(
            data=[t.model_dump() for t in tis.task_instances],
            output=args.output,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error getting task instances: {e}[/red]")
        sys.exit(1)
