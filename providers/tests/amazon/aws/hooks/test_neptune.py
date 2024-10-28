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

from typing import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook


@pytest.fixture
def neptune_hook() -> Generator[NeptuneHook, None, None]:
    """Returns a NeptuneHook mocked with moto"""
    with mock_aws():
        yield NeptuneHook(aws_conn_id="aws_default")


@pytest.fixture
def neptune_cluster_id(neptune_hook: NeptuneHook) -> str:
    """Returns Neptune cluster ID"""
    resp = neptune_hook.conn.create_db_cluster(
        DBClusterIdentifier="test-cluster",
        Engine="neptune",
    )

    return resp["DBCluster"]["DBClusterIdentifier"]


class TestNeptuneHook:
    def test_get_conn_returns_a_boto3_connection(self):
        hook = NeptuneHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    def test_get_cluster_status(self, neptune_hook: NeptuneHook, neptune_cluster_id):
        assert neptune_hook.get_cluster_status(neptune_cluster_id) is not None

    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_wait_for_cluster_instance_availability(
        self, mock_get_waiter, neptune_hook: NeptuneHook, neptune_cluster_id
    ):
        neptune_hook.wait_for_cluster_instance_availability(neptune_cluster_id)
        mock_get_waiter.assert_called_once_with("db_instance_available")
