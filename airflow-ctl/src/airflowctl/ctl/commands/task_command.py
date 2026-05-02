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

from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind, provide_api_client
from airflowctl.ctl.console_formatting import AirflowConsole


@provide_api_client(kind=ClientKind.CLI)
def task_clear(args, api_client: Client = NEW_API_CLIENT) -> dict:
    """Clear task instances."""
    if args.only_failed and args.only_running:
        raise SystemExit("--only-failed and --only-running cannot both be set")

    payload = {
        "dag_run_id": args.dag_run_id,
        "task_ids": args.task_ids,
        "only_failed": args.only_failed,
        "only_running": args.only_running,
        "include_upstream": args.include_upstream,
        "include_downstream": args.include_downstream,
        "dry_run": args.dry_run,
    }
    payload = {key: value for key, value in payload.items() if value is not None}

    response = api_client.post(f"dags/{args.dag_id}/clearTaskInstances", json=payload)
    data = response.json()
    AirflowConsole().print_as(data=[data], output=args.output)
    return data
