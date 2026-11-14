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
from airflow_google_provider_resource_cleanup.handlers import bigtable
from google.api_core import exceptions

pytestmark = pytest.mark.anyio


def test_bigtable_handler_deleters_match_deletion_order():
    handler = bigtable.BigtableDeleteHandler()
    assert set(handler.DELETERS.keys()) == set(handler.DELETION_ORDER)
    assert handler.DELETION_ORDER == [
        "bigtableadmin.googleapis.com/Table",
        "bigtableadmin.googleapis.com/Cluster",
        "bigtableadmin.googleapis.com/Instance",
    ]


async def test_delete_table_success():
    mock_client = AsyncMock()
    with patch.object(bigtable, "_get_table_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1/tables/tbl1",
            "assetType": "bigtableadmin.googleapis.com/Table",
        }
        result = await bigtable._delete_table(res, "[1/1] ")

    assert result is True
    mock_client.delete_table.assert_awaited_once_with(name="projects/p/instances/inst1/tables/tbl1")


async def test_delete_cluster_success():
    mock_client = AsyncMock()
    with patch.object(bigtable, "_get_instance_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1/clusters/cl1",
            "assetType": "bigtableadmin.googleapis.com/Cluster",
        }
        result = await bigtable._delete_cluster(res, "[1/1] ")

    assert result is True
    mock_client.delete_cluster.assert_awaited_once_with(name="projects/p/instances/inst1/clusters/cl1")


async def test_delete_instance_success():
    mock_client = AsyncMock()
    with patch.object(bigtable, "_get_instance_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1",
            "assetType": "bigtableadmin.googleapis.com/Instance",
        }
        result = await bigtable._delete_instance(res, "[1/1] ")

    assert result is True
    mock_client.delete_instance.assert_awaited_once_with(name="projects/p/instances/inst1")


async def test_delete_resource_handles_not_found():
    mock_client = AsyncMock()
    mock_client.delete_table.side_effect = exceptions.NotFound("Table not found")
    with patch.object(bigtable, "_get_table_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1/tables/tbl1",
            "assetType": "bigtableadmin.googleapis.com/Table",
        }
        result = await bigtable._delete_table(res, "[1/1] ")

    assert result is True


async def test_delete_resource_handles_failed_precondition():
    mock_client = AsyncMock()
    mock_client.delete_cluster.side_effect = exceptions.FailedPrecondition("Must have at least one cluster")
    with patch.object(bigtable, "_get_instance_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1/clusters/cl1",
            "assetType": "bigtableadmin.googleapis.com/Cluster",
        }
        result = await bigtable._delete_cluster(res, "[1/1] ")

    assert result is False


async def test_delete_resource_handles_generic_exception():
    mock_client = AsyncMock()
    mock_client.delete_instance.side_effect = RuntimeError("Connection error")
    with patch.object(bigtable, "_get_instance_admin_client", return_value=mock_client):
        res = {
            "name": "//bigtableadmin.googleapis.com/projects/p/instances/inst1",
            "assetType": "bigtableadmin.googleapis.com/Instance",
        }
        result = await bigtable._delete_instance(res, "[1/1] ")

    assert result is False
