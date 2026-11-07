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
GONE_CLUSTER = "//alloydb.googleapis.com/projects/p/locations/us-central1/clusters/gone"
BACKUP = "//alloydb.googleapis.com/projects/p/locations/us-central1/backups/backup"


@pytest.mark.anyio
@patch.object(alloydb.BaseDeleteHandler, "handle", autospec=True)
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_handle_labels_secondary_clusters(mock_client_class, mock_base_handle):
    cluster_types = {
        "projects/p/locations/us-central1/clusters/primary": alloydb.alloydb_v1.Cluster.ClusterType.PRIMARY,
        "projects/p/locations/us-east1/clusters/secondary": alloydb.alloydb_v1.Cluster.ClusterType.SECONDARY,
    }

    async def get_cluster(name):
        if name not in cluster_types:
            raise alloydb.NotFound("cluster deleted")
        return alloydb.alloydb_v1.Cluster(name=name, cluster_type=cluster_types[name])

    mock_client_class.return_value.get_cluster.side_effect = get_cluster
    resources = [
        {"name": PRIMARY_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
        {"name": SECONDARY_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
        {"name": GONE_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
        {"name": BACKUP, "assetType": "alloydb.googleapis.com/Backup"},
    ]
    handler = alloydb.AlloyDBDeleteHandler()

    await handler.handle(resources)

    mock_base_handle.assert_awaited_once_with(
        handler,
        [
            {"name": PRIMARY_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
            {"name": SECONDARY_CLUSTER, "assetType": alloydb.SECONDARY_CLUSTER_ASSET_TYPE},
            {"name": GONE_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
            {"name": BACKUP, "assetType": "alloydb.googleapis.com/Backup"},
        ],
    )
    assert mock_client_class.return_value.get_cluster.await_count == 3


@pytest.mark.anyio
@patch.object(alloydb.alloydb_v1, "AlloyDBAdminAsyncClient", autospec=True)
async def test_secondary_clusters_are_deleted_before_primaries(mock_client_class):
    client = mock_client_class.return_value
    client.get_cluster.side_effect = lambda name: alloydb.alloydb_v1.Cluster(
        name=name,
        cluster_type=alloydb.alloydb_v1.Cluster.ClusterType.SECONDARY
        if name.endswith("secondary")
        else alloydb.alloydb_v1.Cluster.ClusterType.PRIMARY,
    )
    deleted = []

    async def delete_cluster(request):
        deleted.append(request.name.rsplit("/", 1)[-1])
        return AsyncMock()

    client.delete_cluster.side_effect = delete_cluster

    await alloydb.AlloyDBDeleteHandler().handle(
        [
            {"name": PRIMARY_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
            {"name": SECONDARY_CLUSTER, "assetType": "alloydb.googleapis.com/Cluster"},
        ]
    )

    assert deleted == ["secondary", "primary"]
