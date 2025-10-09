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
"""Command functions for managing Airflow pools."""

from __future__ import annotations

import json
from json import JSONDecodeError
from pathlib import Path

import rich

from airflowctl.api.client import NEW_API_CLIENT, Client, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkActionOnExistence,
    BulkBodyPoolBody,
    BulkCreateActionPoolBody,
    PoolBody,
)


@provide_api_client(kind=ClientKind.CLI)
def import_(args, api_client: Client = NEW_API_CLIENT) -> None:
    """Import pools from file."""
    filepath = Path(args.file)
    if not filepath.exists():
        raise SystemExit(f"Missing pools file {args.file}")

    success, errors = _import_helper(api_client, filepath)
    if errors:
        raise SystemExit(f"Failed to update pool(s): {errors}")
    rich.print(success)


@provide_api_client(kind=ClientKind.CLI)
def export(args, api_client: Client = NEW_API_CLIENT) -> None:
    """
    Export all pools.

    If output is json, write to file. Otherwise, print to console.
    """
    try:
        pools_response = api_client.pools.list()
        pools_list = [
            {
                "name": pool.name,
                "slots": pool.slots,
                "description": pool.description,
                "include_deferred": pool.include_deferred,
                "occupied_slots": pool.occupied_slots,
                "running_slots": pool.running_slots,
                "queued_slots": pool.queued_slots,
                "scheduled_slots": pool.scheduled_slots,
                "open_slots": pool.open_slots,
                "deferred_slots": pool.deferred_slots,
            }
            for pool in pools_response.pools
        ]

        if args.output == "json":
            file_path = Path(args.file)
            with open(file_path, "w") as f:
                json.dump(pools_list, f, indent=4, sort_keys=True)
            rich.print(f"Exported {pools_response.total_entries} pool(s) to {args.file}")
        else:
            # For non-json formats, print the pools directly to console
            rich.print(pools_list)
    except Exception as e:
        raise SystemExit(f"Failed to export pools: {e}")


def _import_helper(api_client: Client, filepath: Path):
    """Help import pools from the json file."""
    try:
        with open(filepath) as f:
            pools_json = json.load(f)
    except JSONDecodeError as e:
        raise SystemExit(f"Invalid json file: {e}")

    if not isinstance(pools_json, list):
        raise SystemExit("Invalid format: Expected a list of pool objects")

    pools_to_update = []
    for pool_config in pools_json:
        if not isinstance(pool_config, dict) or "name" not in pool_config or "slots" not in pool_config:
            raise SystemExit(f"Invalid pool configuration: {pool_config}")

        pools_to_update.append(
            PoolBody(
                name=pool_config["name"],
                slots=pool_config["slots"],
                description=pool_config.get("description", ""),
                include_deferred=pool_config.get("include_deferred", False),
            )
        )

    bulk_body = BulkBodyPoolBody(
        actions=[
            BulkCreateActionPoolBody(
                action="create",
                entities=pools_to_update,
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    result = api_client.pools.bulk(pools=bulk_body)
    # Return the successful and failed entities directly from the response
    return result.create.success, result.create.errors
