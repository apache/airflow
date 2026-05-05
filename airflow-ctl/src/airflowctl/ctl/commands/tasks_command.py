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
