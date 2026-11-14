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
from __future__ import annotations

from functools import cache
from typing import TYPE_CHECKING, Any

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path
from google.api_core import exceptions
from google.cloud.bigtable_admin_v2 import (
    BigtableInstanceAdminAsyncClient,
    BigtableTableAdminAsyncClient,
)

if TYPE_CHECKING:
    from collections.abc import Callable


@cache
def _get_table_admin_client() -> BigtableTableAdminAsyncClient:
    return BigtableTableAdminAsyncClient()


@cache
def _get_instance_admin_client() -> BigtableInstanceAdminAsyncClient:
    return BigtableInstanceAdminAsyncClient()


async def _delete_resource_via_client(
    client_method: Callable[..., Any],
    resource: dict,
    log_prefix: str,
    asset_desc: str,
    **extra_kwargs,
) -> bool:
    name = get_resource_path(resource)
    try:
        await client_method(name=name, **extra_kwargs)
        return True
    except exceptions.NotFound:
        print(f"{log_prefix}{asset_desc} '{name}' not found or already deleted.")
        return True
    except exceptions.FailedPrecondition as e:
        print(f"{log_prefix}Cannot delete {asset_desc.lower()} '{name}' (precondition failed): {e}")
        return False
    except Exception as e:
        print(f"{log_prefix}Error while deleting {asset_desc.lower()} '{name}': {e}")
        return False


async def _delete_table(resource: dict, log_prefix: str) -> bool:
    return await _delete_resource_via_client(
        _get_table_admin_client().delete_table,
        resource,
        log_prefix,
        "Table",
    )


async def _delete_cluster(resource: dict, log_prefix: str) -> bool:
    return await _delete_resource_via_client(
        _get_instance_admin_client().delete_cluster,
        resource,
        log_prefix,
        "Cluster",
    )


async def _delete_instance(resource: dict, log_prefix: str) -> bool:
    return await _delete_resource_via_client(
        _get_instance_admin_client().delete_instance,
        resource,
        log_prefix,
        "Instance",
    )


class BigtableDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "bigtableadmin.googleapis.com/Table": _delete_table,
        "bigtableadmin.googleapis.com/Cluster": _delete_cluster,
        "bigtableadmin.googleapis.com/Instance": _delete_instance,
    }

    DELETION_ORDER = [
        "bigtableadmin.googleapis.com/Table",
        "bigtableadmin.googleapis.com/Cluster",
        "bigtableadmin.googleapis.com/Instance",
    ]
