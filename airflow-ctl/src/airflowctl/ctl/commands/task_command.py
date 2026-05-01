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

from airflowctl.api.client import NEW_API_CLIENT, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import TaskInstanceResponse
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def state(args, api_client=NEW_API_CLIENT) -> None:
    """Get the state of a task instance."""
    response = api_client.get(
        f"dags/{args.dag_id}/dagRuns/{args.dag_run_id}/taskInstances/{args.task_id}"
    )
    task_instance = TaskInstanceResponse.model_validate_json(response.content)
    AirflowConsole().print_as(
        data=[{"state": task_instance.state.value if task_instance.state is not None else None}],
        output=args.output,
    )
