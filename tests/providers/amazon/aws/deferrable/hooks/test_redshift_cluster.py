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

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftAsyncHook
from tests.providers.amazon.aws.utils.compat import async_mock

pytest.importorskip("aiobotocore")


class TestRedshiftAsyncHook:
    @pytest.mark.asyncio
    @async_mock.patch("aiobotocore.client.AioBaseClient._make_api_call")
    async def test_cluster_status(self, mock_make_api_call):
        """Test that describe_clusters get called with correct param"""
        hook = RedshiftAsyncHook(aws_conn_id="aws_default", client_type="redshift", resource_type="redshift")
        await hook.cluster_status(cluster_identifier="redshift_cluster_1")
        mock_make_api_call.assert_called_once_with(
            "DescribeClusters", {"ClusterIdentifier": "redshift_cluster_1"}
        )

    @pytest.mark.asyncio
    @async_mock.patch("aiobotocore.client.AioBaseClient._make_api_call")
    async def test_pause_cluster(self, mock_make_api_call):
        """Test that pause_cluster get called with correct param"""
        hook = RedshiftAsyncHook(aws_conn_id="aws_default", client_type="redshift", resource_type="redshift")
        await hook.pause_cluster(cluster_identifier="redshift_cluster_1")
        mock_make_api_call.assert_called_once_with(
            "PauseCluster", {"ClusterIdentifier": "redshift_cluster_1"}
        )

    @pytest.mark.asyncio
    @async_mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.get_client_async"
    )
    @async_mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.cluster_status")
    async def test_get_cluster_status(self, cluster_status, mock_client):
        """Test get_cluster_status async function with success response"""
        flag = asyncio.Event()
        cluster_status.return_value = {"status": "success", "cluster_state": "available"}
        hook = RedshiftAsyncHook(aws_conn_id="aws_default")
        result = await hook.get_cluster_status("redshift_cluster_1", "available", flag)
        assert result == {"status": "success", "cluster_state": "available"}

    @pytest.mark.asyncio
    @async_mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftAsyncHook.cluster_status")
    async def test_get_cluster_status_exception(self, cluster_status):
        """Test get_cluster_status async function with exception response"""
        flag = asyncio.Event()
        cluster_status.side_effect = ClientError(
            {
                "Error": {
                    "Code": "SomeServiceException",
                    "Message": "Details/context around the exception or error",
                },
            },
            operation_name="redshift",
        )
        hook = RedshiftAsyncHook(aws_conn_id="aws_default")
        result = await hook.get_cluster_status("test-identifier", "available", flag)
        assert result == {
            "status": "error",
            "message": "An error occurred (SomeServiceException) when calling the "
            "redshift operation: Details/context around the exception or error",
        }
