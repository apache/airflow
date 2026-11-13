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
from google.api_core.exceptions import GoogleAPICallError, NotFound
from google.cloud import alloydb_v1

CLUSTER_ASSET_TYPE = "alloydb.googleapis.com/Cluster"
INSTANCE_ASSET_TYPE = "alloydb.googleapis.com/Instance"
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
    # force=True is needed because secondary instances can only be deleted together with their
    # cluster. AlloyDBDeleteHandler.handle only passes clusters whose instances are all selected
    # for deletion, so the cascade cannot remove a protected or too-young instance.
    await _delete_resource(resource, alloydb_v1.DeleteClusterRequest, "delete_cluster", force=True)


def _get_location_relative_name(name: str) -> str:
    """Return the part of a resource name after ``locations/``, which is the same in every API."""
    return name.split("/locations/", 1)[-1]


async def _is_secondary_cluster(resource: dict) -> bool:
    client = alloydb_v1.AlloyDBAdminAsyncClient()
    try:
        cluster = await client.get_cluster(name=get_resource_path(resource))
    except NotFound:
        return False
    except GoogleAPICallError as e:
        # Treating the cluster as a primary is safe: if it is a secondary, deleting its primary
        # fails with a printed error and the next run removes both.
        print(f"Could not get the type of AlloyDB cluster {resource.get('name')}: {e}")
        return False
    return cluster.cluster_type == alloydb_v1.Cluster.ClusterType.SECONDARY


async def _get_kept_instances(resource: dict, selected_instances: set[str]) -> list[str] | None:
    """Return the cluster's instances not selected for deletion, or None if they cannot be listed."""
    client = alloydb_v1.AlloyDBAdminAsyncClient()
    try:
        pager = await client.list_instances(parent=get_resource_path(resource))
        instance_names = [instance.name async for instance in pager]
    except NotFound:
        return []
    except GoogleAPICallError as e:
        print(f"Could not list the instances of AlloyDB cluster {resource.get('name')}: {e}")
        return None
    return [name for name in instance_names if _get_location_relative_name(name) not in selected_instances]


async def _prepare_cluster(resource: dict, selected_instances: set[str]) -> dict | None:
    """Return the cluster resource to delete (re-labelled if secondary), or None to keep it."""
    kept_instances = await _get_kept_instances(resource, selected_instances)
    if kept_instances is None:
        print(f"Skipping AlloyDB cluster {resource.get('name')}: its instances could not be checked.")
        return None
    if kept_instances:
        print(
            f"Skipping AlloyDB cluster {resource.get('name')}: deleting it would also delete instances "
            f"that are not selected for deletion (protected or too young): {', '.join(kept_instances)}"
        )
        return None
    if await _is_secondary_cluster(resource):
        return {**resource, "assetType": SECONDARY_CLUSTER_ASSET_TYPE}
    return resource


class AlloyDBDeleteHandler(BaseDeleteHandler):
    DELETERS = {
        "alloydb.googleapis.com/Backup": _delete_backup,
        SECONDARY_CLUSTER_ASSET_TYPE: _delete_cluster,
        CLUSTER_ASSET_TYPE: _delete_cluster,
        INSTANCE_ASSET_TYPE: _delete_instance,
    }

    DELETION_ORDER = [
        INSTANCE_ASSET_TYPE,
        SECONDARY_CLUSTER_ASSET_TYPE,
        CLUSTER_ASSET_TYPE,
        "alloydb.googleapis.com/Backup",
    ]

    async def handle(self, resources: list[dict]):
        selected_instances = {
            _get_location_relative_name(get_resource_path(resource))
            for resource in resources
            if resource.get("assetType") == INSTANCE_ASSET_TYPE
        }
        clusters = [resource for resource in resources if resource.get("assetType") == CLUSTER_ASSET_TYPE]
        prepared_clusters = await asyncio.gather(
            *(_prepare_cluster(cluster, selected_instances) for cluster in clusters)
        )
        resources = [resource for resource in resources if resource.get("assetType") != CLUSTER_ASSET_TYPE]
        resources.extend(cluster for cluster in prepared_clusters if cluster is not None)
        await super().handle(resources)
