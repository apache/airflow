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
from airflow.providers.amazon.aws.operators.neptune_analytics import NeptuneCreateGraphOperator

GRAPH_NAME = "test_graph"


@pytest.fixture
def hook() -> Generator[NeptuneAnalyticsHook, None, None]:
    with mock_aws():
        yield NeptuneAnalyticsHook(aws_conn_id="aws_default")


class TestNeptuneCreateGraphOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
        )

        assert operator.public_connectivity is None
        assert operator.replica_count is None
        assert operator.deletion_protect is False
        assert operator.kms_key is None
        assert operator.tags is None

        operator.execute(None)

        mock_conn.create_graph.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"test": 123},
            provisionedMemory=16,
            deletionProtection=False,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
            public_connectivity=True,
            replica_count=3,
            kms_key_id="test-key",
            tags={"key1": "test"},
            deletion_protection=True,
        )

        assert operator.public_connectivity is True
        assert operator.replica_count == 3
        assert operator.deletion_protect is True
        assert operator.kms_key == "test-key"
        assert operator.tags == {"key1": "test"}

        operator.execute(None)

        mock_conn.create_graph.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"test": 123},
            replicaCount=3,
            publicConnectivity=True,
            provisionedMemory=16,
            deletionProtection=True,
            kmsKeyIdentifier="test-key",
            tags={"key1": "test"},
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            provisioned_memory=16,
            vector_search_config={"test": 123},
            wait_for_completion=False,
        )
        resp = operator.execute(None)

        mock_hook_get_waiter.assert_not_called()
        assert "graph_id" in resp
        assert resp["graph_id"] is not None

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            provisioned_memory=16,
            vector_search_config={"test": 123},
            wait_for_completion=True,
        )
        resp = operator.execute(None)

        mock_hook_get_waiter.assert_called_once_with("graph_available")
        assert "graph_id" in resp
        assert resp["graph_id"] is not None
