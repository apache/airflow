#
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
"""Pools sub-commands."""

from __future__ import annotations

import json
import os
from json import JSONDecodeError

from airflowctl.api.operations import ServerResponseError

from airflow.api_fastapi.core_api.datamodels.pools import PoolBody
from airflow.cli.api_client import NEW_API_CLIENT, Client, provide_api_client
from airflow.cli.simple_table import AirflowConsole
from airflow.cli.utils import deprecated_for_airflowctl
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


def _show_pools(pools, output):
    AirflowConsole().print_as(
        data=pools,
        output=output,
        mapper=lambda x: {
            "pool": x.name,
            "slots": x.slots,
            "description": x.description,
            "include_deferred": x.include_deferred,
            "team_name": x.team_name,
        },
    )


@deprecated_for_airflowctl("airflowctl pools list")
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_api_client
def pool_list(args, api_client: Client = NEW_API_CLIENT):
    """Display info of all the pools."""
    pools = api_client.pools.list().pools
    _show_pools(pools=pools, output=args.output)


@deprecated_for_airflowctl("airflowctl pools get")
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_api_client
def pool_get(args, api_client: Client = NEW_API_CLIENT):
    """Display pool info by a given name."""
    try:
        pools = [api_client.pools.get(pool_name=args.pool)]
        _show_pools(pools=pools, output=args.output)
    except ServerResponseError as e:
        if e.response.status_code == 404:
            raise SystemExit(f"Pool {args.pool} does not exist")
        raise


@cli_utils.action_cli
@deprecated_for_airflowctl("airflowctl pools create")
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_api_client
def pool_set(args, api_client: Client = NEW_API_CLIENT):
    """Create new pool with a given name and slots."""
    # core_api PoolBody is the source of truth and is wire-compatible with the airflowctl
    # client's generated model (the API server uses populate_by_name).
    pool_body = PoolBody(
        name=args.pool,
        slots=args.slots,
        description=args.description,
        include_deferred=args.include_deferred,
        team_name=args.team_name,
    )
    api_client.pools.create(pool=pool_body)  # type: ignore[arg-type]
    print(f"Pool {args.pool} created")


@cli_utils.action_cli
@deprecated_for_airflowctl("airflowctl pools delete")
@suppress_logs_and_warning
@providers_configuration_loaded
@provide_api_client
def pool_delete(args, api_client: Client = NEW_API_CLIENT):
    """Delete pool by a given name."""
    try:
        api_client.pools.delete(pool=args.pool)
        print(f"Pool {args.pool} deleted")
    except ServerResponseError as e:
        if e.response.status_code == 404:
            raise SystemExit(f"Pool {args.pool} does not exist")
        raise


@cli_utils.action_cli
@deprecated_for_airflowctl("airflowctl pools import")
@suppress_logs_and_warning
@providers_configuration_loaded
def pool_import(args):
    """Import pools from the file."""
    if not os.path.exists(args.file):
        raise SystemExit(f"Missing pools file {args.file}")
    pools, failed = pool_import_helper(args.file)
    if failed:
        raise SystemExit(f"Failed to update pool(s): {', '.join(failed)}")
    print(f"Uploaded {len(pools)} pool(s)")


@deprecated_for_airflowctl("airflowctl pools export")
@providers_configuration_loaded
def pool_export(args):
    """Export all the pools to the file."""
    pools = pool_export_helper(args.file)
    print(f"Exported {len(pools)} pools to {args.file}")


@provide_api_client
def pool_import_helper(filepath, api_client: Client = NEW_API_CLIENT):
    """Help import pools from the json file."""
    with open(filepath) as poolfile:
        data = poolfile.read()
    try:
        pools_json = json.loads(data)
    except JSONDecodeError as e:
        raise SystemExit(f"Invalid json file: {e}")
    pools = []
    failed = []
    for k, v in pools_json.items():
        if isinstance(v, dict) and "slots" in v and "description" in v:
            pool_body = PoolBody(
                name=k,
                slots=v["slots"],
                description=v["description"],
                include_deferred=v.get("include_deferred", False),
                team_name=v.get("team_name"),
            )
            pools.append(api_client.pools.create(pool=pool_body))  # type: ignore[arg-type]
        else:
            failed.append(k)
    return pools, failed


@provide_api_client
def pool_export_helper(filepath, api_client: Client = NEW_API_CLIENT):
    """Help export all the pools to the json file."""
    pool_dict = {}
    pools = api_client.pools.list().pools
    for pool in pools:
        entry = {
            "slots": pool.slots,
            "description": pool.description,
            "include_deferred": pool.include_deferred,
        }
        if pool.team_name is not None:
            entry["team_name"] = pool.team_name
        pool_dict[pool.name] = entry
    with open(filepath, "w") as poolfile:
        poolfile.write(json.dumps(pool_dict, sort_keys=True, indent=4))
    return pools
