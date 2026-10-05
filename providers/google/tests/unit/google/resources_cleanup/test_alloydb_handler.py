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


def test_deletion_order_removes_clusters_before_backups():
    assert alloydb.AlloyDBDeleteHandler.DELETION_ORDER == [
        "alloydb.googleapis.com/Instance",
        "alloydb.googleapis.com/Cluster",
        "alloydb.googleapis.com/Backup",
    ]
