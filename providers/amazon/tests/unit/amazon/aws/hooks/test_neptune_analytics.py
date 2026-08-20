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

from collections.abc import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook


@pytest.fixture
def neptune_hook() -> Generator[NeptuneAnalyticsHook, None, None]:
    """Returns a NeptuneAnalyticsHook mocked with moto"""
    with mock_aws():
        yield NeptuneAnalyticsHook(aws_conn_id="aws_default")


class TestNeptuneAnalyticsHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = NeptuneAnalyticsHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_get_graph_endpoint_id(self, mock_conn):
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": "vpce-12345",
        }

        hook = NeptuneAnalyticsHook(aws_conn_id="aws_default")
        result = hook._get_graph_endpoint_id(graph_id="g-abc123", vpc_id="vpc-99999")

        mock_conn.get_private_graph_endpoint.assert_called_once_with(
            graphIdentifier="g-abc123", vpcId="vpc-99999"
        )
        assert result == "vpce-12345"

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_get_graph_endpoint_id_missing_key(self, mock_conn):
        mock_conn.get_private_graph_endpoint.return_value = {}

        hook = NeptuneAnalyticsHook(aws_conn_id="aws_default")
        result = hook._get_graph_endpoint_id(graph_id="g-abc123", vpc_id="vpc-99999")

        assert result is None
