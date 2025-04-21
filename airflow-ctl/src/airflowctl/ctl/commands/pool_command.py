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

from airflowctl.api.client import Client, ClientKind, provide_api_client
from airflowctl.api.datamodels.generated import (
    BulkAction,
    BulkActionOnExistence,
    BulkBodyPoolBody,
    BulkCreateActionPoolBody,
    PoolBody,
)


@provide_api_client(kind=ClientKind.CLI)
def import_(api_client: Client, file: str | Path, **kwargs):
    """Import pools from file."""
    filepath = Path(file)
    if not filepath.exists():
        raise SystemExit(f"Missing pools file {file}")

    pools, failed = _import_helper(api_client, filepath)
    if failed:
        raise SystemExit(f"Failed to update pool(s): {', '.join(failed)}")
    rich.print(f"Uploaded {len(pools)} pool(s)")


@provide_api_client(kind=ClientKind.CLI)
def export(api_client: Client, file: str | Path, **kwargs):
    """Export all pools to file."""
    pools = api_client.pools.list()
    pools_dict = {
        pool["name"]: {
            "slots": pool["slots"],
            "description": pool["description"],
            "include_deferred": pool["include_deferred"],
        }
        for pool in pools
    }

    file_path = Path(file)
    with open(file_path, "w") as f:
        json.dump(pools_dict, f, indent=4, sort_keys=True)

    rich.print(f"Exported {len(pools)} pool(s) to {file}")


def _import_helper(api_client: Client, filepath: Path):
    """Help import pools from the json file."""
    try:
        with open(filepath) as f:
            pools_json = json.load(f)
    except JSONDecodeError as e:
        raise SystemExit(f"Invalid json file: {e}")

    pools_to_update = []
    for pool_name, pool_config in pools_json.items():
        if isinstance(pool_config, dict) and "slots" in pool_config and "description" in pool_config:
            pools_to_update.append(
                PoolBody(
                    name=pool_name,
                    slots=pool_config["slots"],
                    description=pool_config["description"],
                    include_deferred=pool_config.get("include_deferred", False),
                )
            )
        else:
            raise SystemExit(f"Invalid pool configuration for {pool_name}")

    bulk_body = BulkBodyPoolBody(
        actions=[
            BulkCreateActionPoolBody(
                action=BulkAction.CREATE,
                entities=pools_to_update,
                action_on_existence=BulkActionOnExistence.FAIL,
            )
        ]
    )
    result = api_client.pools.bulk(pools=bulk_body)
    return result.pools, result.failed
