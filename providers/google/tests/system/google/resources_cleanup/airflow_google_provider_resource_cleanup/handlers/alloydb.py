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

import asyncio

from airflow_google_provider_resource_cleanup.handlers._base import BaseDeleteHandler
from airflow_google_provider_resource_cleanup.helpers import get_resource_path
from google.api_core.exceptions import NotFound
from google.cloud import alloydb_v1

CLUSTER_ASSET_TYPE = "alloydb.googleapis.com/Cluster"
# Not a Cloud Asset type: secondary clusters are re-labelled with it so that they get their own
# deletion pass. AlloyDB refuses to delete a primary cluster that still has secondaries, and
# force=True only cascades to instances.
SECONDARY_CLUSTER_ASSET_TYPE = "alloydb.googleapis.com/Cluster#secondary"


async def _delete_resource(resource: dict, request_type, delete_method_name: str, **request_kwargs):
    client = alloydb_v1.AlloyDBAdminAsyncClient()
    request = request_type(name=get_resource_path(resource), **request_kwargs)
    operation = await getattr(client, delete_method_name)(request=request)
    await operation.result()


async def _delete_backup(resource: dict, log_prefix: str):
    await _delete_resource(resource, alloydb_v1.DeleteBackupRequest, "delete_backup")


async def _delete_instance(resource: dict, log_prefix: str):
    await _delete_resource(resource, alloydb_v1.DeleteInstanceRequest, "delete_instance")


async def _delete_cluster(resource: dict, log_prefix: str):
    await _delete_resource(resource, alloydb_v1.DeleteClusterRequest, "delete_cluster", force=True)


async def _is_secondary_cluster(resource: dict) -> bool:
    if resource.get("assetType") != CLUSTER_ASSET_TYPE:
        return False
    client = alloydb_v1.AlloyDBAdminAsyncClient()
    try:
        cluster = await client.get_cluster(name=get_resource_path(resource))
    except NotFound:
        return False
    return cluster.cluster_type == alloydb_v1.Cluster.ClusterType.SECONDARY


class AlloyDBDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "alloydb.googleapis.com/Backup": _delete_backup,
        SECONDARY_CLUSTER_ASSET_TYPE: _delete_cluster,
        CLUSTER_ASSET_TYPE: _delete_cluster,
        "alloydb.googleapis.com/Instance": _delete_instance,
    }

    DELETION_ORDER = [
        "alloydb.googleapis.com/Instance",
        SECONDARY_CLUSTER_ASSET_TYPE,
        CLUSTER_ASSET_TYPE,
        "alloydb.googleapis.com/Backup",
    ]

    async def handle(self, resources: list[dict]):
        is_secondary = await asyncio.gather(*(_is_secondary_cluster(resource) for resource in resources))
        resources = [
            {**resource, "assetType": SECONDARY_CLUSTER_ASSET_TYPE} if secondary else resource
            for resource, secondary in zip(resources, is_secondary)
        ]
        await super().handle(resources)
