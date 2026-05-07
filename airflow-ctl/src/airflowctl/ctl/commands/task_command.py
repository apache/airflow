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
from airflowctl.api.datamodels.generated import ClearTaskInstancesBody
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def clear(args, api_client=NEW_API_CLIENT) -> None:
    """Clear task instances for a DAG run (``POST .../clearTaskInstances``, body includes ``dag_run_id``)."""
    body = ClearTaskInstancesBody(
        dag_run_id=args.dag_run_id,
        dry_run=args.dry_run,
        only_failed=args.only_failed,
        only_running=args.only_running,
        include_upstream=args.upstream,
        include_downstream=args.downstream,
        task_ids=args.task_ids,
    )

    try:
        cleared = api_client.tasks.clear_task_instances(dag_id=args.dag_id, clear_body=body)
    except ServerResponseError as e:
        rich.print(f"[red]Error clearing tasks for DAG {args.dag_id}, run {args.dag_run_id}: {e}[/red]")
        sys.exit(1)

    AirflowConsole().print_as(
        data=[ti.model_dump(mode="json") for ti in cleared.task_instances],
        output=args.output,
    )
