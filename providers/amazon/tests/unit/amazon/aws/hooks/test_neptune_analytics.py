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
    graph_id = "abc123"

    def test_get_conn_returns_a_boto3_connection(self):
        hook = NeptuneAnalyticsHook(aws_conn_id="aws_default")
        assert hook.get_conn() is not None

    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_wait_for_graph_availability(self, mock_get_waiter, neptune_hook: NeptuneAnalyticsHook):
        waiter = mock_get_waiter("graph_available")
        assert waiter is not None
