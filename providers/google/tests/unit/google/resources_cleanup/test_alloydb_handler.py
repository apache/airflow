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

from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import alloydb


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("asset_type", "resource_name", "request_type", "delete_method_name", "force"),
    [
        (
            "alloydb.googleapis.com/Backup",
            "//alloydb.googleapis.com/projects/test-project/locations/us-central1/backups/test-backup",
            alloydb.alloydb_v1.DeleteBackupRequest,
            "delete_backup",
            False,
        ),
        (
            "alloydb.googleapis.com/Instance",
            "//alloydb.googleapis.com/projects/test-project/locations/us-central1/clusters/test-cluster/instances/test-instance",
            alloydb.alloydb_v1.DeleteInstanceRequest,
            "delete_instance",
            False,
        ),
        (
            "alloydb.googleapis.com/Cluster",
            "//alloydb.googleapis.com/projects/test-project/locations/us-central1/clusters/test-cluster",
            alloydb.alloydb_v1.DeleteClusterRequest,
            "delete_cluster",
            True,
        ),
    ],
)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_delete_resource(
    mock_client_class, asset_type, resource_name, request_type, delete_method_name, force
):
    client = mock_client_class.return_value
    operation = AsyncMock()
    delete_method = getattr(client, delete_method_name)
    delete_method.return_value = operation

    await alloydb.AlloyDBDeleteHandler.DELETERS[asset_type]({"name": resource_name}, "[1/1] ")

    mock_client_class.assert_called_once_with()
    delete_method.assert_awaited_once()
    request = delete_method.await_args.kwargs["request"]
    assert isinstance(request, request_type)
    assert request.name == resource_name.removeprefix("//alloydb.googleapis.com/")
    if isinstance(request, alloydb.alloydb_v1.DeleteClusterRequest):
        assert request.force is force
    operation.result.assert_awaited_once_with()


def test_deletion_order_removes_secondary_clusters_before_primaries_and_backups():
    assert alloydb.AlloyDBDeleteHandler.DELETION_ORDER == [
        "alloydb.googleapis.com/Instance",
        alloydb.SECONDARY_CLUSTER_ASSET_TYPE,
        "alloydb.googleapis.com/Cluster",
        "alloydb.googleapis.com/Backup",
    ]


PRIMARY_CLUSTER = "//alloydb.googleapis.com/projects/p/locations/us-central1/clusters/primary"
SECONDARY_CLUSTER = "//alloydb.googleapis.com/projects/p/locations/us-east1/clusters/secondary"
BACKUP = "//alloydb.googleapis.com/projects/p/locations/us-central1/backups/backup"
PRIMARY_INSTANCE = f"{PRIMARY_CLUSTER}/instances/primary-instance"
SECONDARY_INSTANCE = f"{SECONDARY_CLUSTER}/instances/secondary-instance"
CLUSTER_TYPES = {
    "projects/p/locations/us-central1/clusters/primary": alloydb.alloydb_v1.Cluster.ClusterType.PRIMARY,
    "projects/p/locations/us-east1/clusters/secondary": alloydb.alloydb_v1.Cluster.ClusterType.SECONDARY,
}
# The AlloyDB API may name the project differently from Cloud Asset Inventory, e.g. by number.
CLUSTER_INSTANCES = {
    "projects/p/locations/us-central1/clusters/primary": [
        "projects/123/locations/us-central1/clusters/primary/instances/primary-instance"
    ],
    "projects/p/locations/us-east1/clusters/secondary": [
        "projects/123/locations/us-east1/clusters/secondary/instances/secondary-instance"
    ],
}


class AsyncPager:
    def __init__(self, items):
        self._items = items

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        for item in self._items:
            yield item


def cluster(name):
    return {"name": name, "assetType": "alloydb.googleapis.com/Cluster"}


def instance(name):
    return {"name": name, "assetType": "alloydb.googleapis.com/Instance"}


def configure_client(client, *, get_cluster_error=None, list_instances_error=None):
    async def get_cluster(name):
        if get_cluster_error:
            raise get_cluster_error
        return alloydb.alloydb_v1.Cluster(name=name, cluster_type=CLUSTER_TYPES[name])

    async def list_instances(parent):
        if list_instances_error:
            raise list_instances_error
        return AsyncPager([alloydb.alloydb_v1.Instance(name=name) for name in CLUSTER_INSTANCES[parent]])

    client.get_cluster.side_effect = get_cluster
    client.list_instances.side_effect = list_instances


@pytest.mark.anyio
@patch.object(alloydb.BaseDeleteHandler, "handle", autospec=True)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_handle_labels_secondary_clusters(mock_client_class, mock_base_handle):
    configure_client(mock_client_class.return_value)
    resources = [
        instance(PRIMARY_INSTANCE),
        instance(SECONDARY_INSTANCE),
        cluster(PRIMARY_CLUSTER),
        cluster(SECONDARY_CLUSTER),
        {"name": BACKUP, "assetType": "alloydb.googleapis.com/Backup"},
    ]
    handler = alloydb.AlloyDBDeleteHandler()

    await handler.handle(resources)

    mock_base_handle.assert_awaited_once_with(
        handler,
        [
            instance(PRIMARY_INSTANCE),
            instance(SECONDARY_INSTANCE),
            {"name": BACKUP, "assetType": "alloydb.googleapis.com/Backup"},
            cluster(PRIMARY_CLUSTER),
            {"name": SECONDARY_CLUSTER, "assetType": alloydb.SECONDARY_CLUSTER_ASSET_TYPE},
        ],
    )


@pytest.mark.anyio
@patch.object(alloydb.BaseDeleteHandler, "handle", autospec=True)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_handle_keeps_cluster_with_instance_not_selected_for_deletion(
    mock_client_class, mock_base_handle, capsys
):
    # The primary instance was filtered out (protected or younger than --min-age-days), so its
    # cluster must not be deleted, as force=True would delete the instance too.
    configure_client(mock_client_class.return_value)
    handler = alloydb.AlloyDBDeleteHandler()

    await handler.handle([cluster(PRIMARY_CLUSTER)])

    mock_base_handle.assert_awaited_once_with(handler, [])
    assert "primary-instance" in capsys.readouterr().out


@pytest.mark.anyio
@patch.object(alloydb.BaseDeleteHandler, "handle", autospec=True)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_handle_keeps_cluster_when_instances_cannot_be_listed(
    mock_client_class, mock_base_handle, capsys
):
    configure_client(
        mock_client_class.return_value, list_instances_error=alloydb.GoogleAPICallError("permission denied")
    )
    handler = alloydb.AlloyDBDeleteHandler()

    await handler.handle([instance(PRIMARY_INSTANCE), cluster(PRIMARY_CLUSTER)])

    mock_base_handle.assert_awaited_once_with(handler, [instance(PRIMARY_INSTANCE)])
    assert "could not be checked" in capsys.readouterr().out


@pytest.mark.anyio
@patch.object(alloydb.BaseDeleteHandler, "handle", autospec=True)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_handle_treats_cluster_as_primary_when_type_lookup_fails(
    mock_client_class, mock_base_handle, capsys
):
    configure_client(
        mock_client_class.return_value, get_cluster_error=alloydb.GoogleAPICallError("quota exceeded")
    )
    handler = alloydb.AlloyDBDeleteHandler()

    await handler.handle([instance(SECONDARY_INSTANCE), cluster(SECONDARY_CLUSTER)])

    mock_base_handle.assert_awaited_once_with(
        handler, [instance(SECONDARY_INSTANCE), cluster(SECONDARY_CLUSTER)]
    )
    assert "quota exceeded" in capsys.readouterr().out


@pytest.mark.anyio
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_secondary_clusters_are_deleted_before_primaries(mock_client_class):
    client = mock_client_class.return_value
    configure_client(client)
    deleted = []

    async def delete(request):
        deleted.append(request.name.rsplit("/", 1)[-1])
        return AsyncMock()

    client.delete_instance.side_effect = delete
    client.delete_cluster.side_effect = delete

    await alloydb.AlloyDBDeleteHandler().handle(
        [
            cluster(PRIMARY_CLUSTER),
            cluster(SECONDARY_CLUSTER),
            instance(PRIMARY_INSTANCE),
            instance(SECONDARY_INSTANCE),
        ]
    )

    assert deleted[2:] == ["secondary", "primary"]
