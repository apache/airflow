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
from google.cloud.devtools.cloudbuild_v1 import CloudBuildAsyncClient

if TYPE_CHECKING:
    from collections.abc import Callable


@cache
def _get_cloudbuild_client() -> CloudBuildAsyncClient:
    return CloudBuildAsyncClient()


async def _delete_resource_via_client(
    client_method: Callable[..., Any],
    resource: dict,
    log_prefix: str,
    asset_desc: str,
    **extra_kwargs,
) -> bool:
    name = get_resource_path(resource)
    try:
        response = await client_method(name=name, **extra_kwargs)
        if hasattr(response, "result"):
            await response.result()
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


async def _delete_build_trigger(resource: dict, log_prefix: str) -> bool:
    return await _delete_resource_via_client(
        _get_cloudbuild_client().delete_build_trigger,
        resource,
        log_prefix,
        "BuildTrigger",
    )


async def _delete_worker_pool(resource: dict, log_prefix: str) -> bool:
    return await _delete_resource_via_client(
        _get_cloudbuild_client().delete_worker_pool,
        resource,
        log_prefix,
        "WorkerPool",
    )


class CloudBuildDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "cloudbuild.googleapis.com/BuildTrigger": _delete_build_trigger,
        "cloudbuild.googleapis.com/WorkerPool": _delete_worker_pool,
    }

    DELETION_ORDER = [
        "cloudbuild.googleapis.com/BuildTrigger",
        "cloudbuild.googleapis.com/WorkerPool",
    ]
