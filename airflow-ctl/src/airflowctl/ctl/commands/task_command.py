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

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.api.datamodels.generated import ClearTaskInstancesBody
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> None:
    """Clear task instances for a Dag."""
    start_date = _parse_date(args.start_date) if args.start_date else None
    end_date = _parse_date(args.end_date) if args.end_date else None

    body = ClearTaskInstancesBody(
        dry_run=False,
        task_ids=[args.task_id],
        start_date=start_date,
        end_date=end_date,
    )

    try:
        response = api_client.task_instances.clear(dag_id=args.dag_id, clear_task_instances_body=body)
    except ServerResponseError as e:
        rich.print(f"[red]Error clearing task instances: {e}[/red]")
        sys.exit(1)

    response_list = [ti.model_dump() for ti in (response.task_instances or [])]
    rich.print(
        f"[green]Cleared {len(response_list)} task instance(s) "
        f"for dag {args.dag_id}, task {args.task_id}[/green]"
    )
    AirflowConsole().print_as(data=response_list, output=args.output)
    return response_list


def _parse_date(value: str) -> datetime.datetime | None:
    """Parse a date string into a datetime object."""
    if not value:
        return None
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        rich.print(f"[red]Invalid date format: {value}. Use ISO format (e.g. 2026-01-01T00:00:00).[/red]")
        sys.exit(1)
