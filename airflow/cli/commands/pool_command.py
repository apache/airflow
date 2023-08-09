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

from airflow.api.client import get_current_api_client
from airflow.cli.simple_table import AirflowConsole
from airflow.exceptions import PoolNotFound
from airflow.utils import cli as cli_utils
from airflow.utils.cli import suppress_logs_and_warning
from airflow.utils.providers_configuration_loader import providers_configuration_loaded


def _show_pools(pools, output):
    AirflowConsole().print_as(
        data=pools,
        output=output,
        mapper=lambda x: {
            "pool": x[0],
            "slots": x[1],
            "description": x[2],
            "include_deferred": x[3],
        },
    )


@suppress_logs_and_warning
@providers_configuration_loaded
def pool_list(args):
    """Displays info of all the pools."""
    api_client = get_current_api_client()
    pools = api_client.get_pools()
    _show_pools(pools=pools, output=args.output)


@suppress_logs_and_warning
@providers_configuration_loaded
def pool_get(args):
    """Displays pool info by a given name."""
    api_client = get_current_api_client()
    try:
        pools = [api_client.get_pool(name=args.pool)]
        _show_pools(pools=pools, output=args.output)
    except PoolNotFound:
        raise SystemExit(f"Pool {args.pool} does not exist")


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def pool_set(args):
    """Creates new pool with a given name and slots."""
    api_client = get_current_api_client()
    api_client.create_pool(
        name=args.pool, slots=args.slots, description=args.description, include_deferred=args.include_deferred
    )
    print(f"Pool {args.pool} created")


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def pool_delete(args):
    """Deletes pool by a given name."""
    api_client = get_current_api_client()
    try:
        api_client.delete_pool(name=args.pool)
        print(f"Pool {args.pool} deleted")
    except PoolNotFound:
        raise SystemExit(f"Pool {args.pool} does not exist")


@cli_utils.action_cli
@suppress_logs_and_warning
@providers_configuration_loaded
def pool_import(args):
    """Imports pools from the file."""
    if not os.path.exists(args.file):
        raise SystemExit(f"Missing pools file {args.file}")
    pools, failed = pool_import_helper(args.file)
    if len(failed) > 0:
        raise SystemExit(f"Failed to update pool(s): {', '.join(failed)}")
    print(f"Uploaded {len(pools)} pool(s)")


@providers_configuration_loaded
def pool_export(args):
    """Exports all the pools to the file."""
    pools = pool_export_helper(args.file)
    print(f"Exported {len(pools)} pools to {args.file}")


def pool_import_helper(filepath):
    """Helps import pools from the json file."""
    api_client = get_current_api_client()

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
            pools.append(
                api_client.create_pool(
                    name=k,
                    slots=v["slots"],
                    description=v["description"],
                    include_deferred=v.get("include_deferred", False),
                )
            )
        else:
            failed.append(k)
    return pools, failed


def pool_export_helper(filepath):
    """Helps export all the pools to the json file."""
    api_client = get_current_api_client()
    pool_dict = {}
    pools = api_client.get_pools()
    for pool in pools:
        pool_dict[pool[0]] = {"slots": pool[1], "description": pool[2], "include_deferred": pool[3]}
    with open(filepath, "w") as poolfile:
        poolfile.write(json.dumps(pool_dict, sort_keys=True, indent=4))
    return pools
