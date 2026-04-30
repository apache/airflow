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

import rich

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, ServerResponseError, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def states_for_dag_run(args, api_client=NEW_API_CLIENT) -> list[dict] | None:
    """Get task states for a DAG run."""
    try:
        response = api_client.task_instances.list_for_dag_run(
            dag_id=args.dag_id,
            dag_run_id=args.dag_run_id,
        )
    except ServerResponseError as e:
        rich.print(f"[red]Error fetching task states for DAG run {args.dag_run_id}: {e}[/red]")
        sys.exit(1)

    data = [
        {"task_id": ti.task_id, "state": ti.state.value if ti.state is not None else None}
        for ti in response.task_instances
    ]
    AirflowConsole().print_as(data=data, output=args.output)
    return data
