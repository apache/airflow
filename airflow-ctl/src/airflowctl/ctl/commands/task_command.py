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
from airflowctl.api.datamodels.generated import TaskInstanceCollectionResponse
from airflowctl.api.operations import BaseOperations
from airflowctl.ctl.console_formatting import AirflowConsole


class _TaskInstancesForDagRunOperations(BaseOperations):
    """Paginated GET for task instances scoped to one DAG run (not registered in cli CommandFactory)."""

    def list_for_dag_run(self, dag_id: str, dag_run_id: str) -> TaskInstanceCollectionResponse:
        path = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances"
        return super().execute_list(path=path, data_model=TaskInstanceCollectionResponse, limit=100)


@provide_api_client(kind=ClientKind.CLI)
def states_for_dag_run(args, api_client=NEW_API_CLIENT) -> None:
    """Print task states for one DAG run (`GET .../dagRuns/{dag_run_id}/taskInstances`)."""
    try:
        collection = _TaskInstancesForDagRunOperations(api_client).list_for_dag_run(
            args.dag_id,
            args.dag_run_id,
        )
    except ServerResponseError as e:
        rich.print(
            f"[red]Error listing task instances for DAG {args.dag_id}, run {args.dag_run_id}: {e}[/red]"
        )
        sys.exit(1)

    rows: list[dict[str, object]] = []
    for ti in collection.task_instances:
        state_val: object | None = None
        if ti.state is not None:
            state_val = ti.state.value
        rows.append({"task_id": ti.task_id, "state": state_val})

    AirflowConsole().print_as(data=rows, output=args.output)
