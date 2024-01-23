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
from moto import mock_neptune

from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.operators.neptune import (
    NeptuneStartDbClusterOperator,
    NeptuneStopDbClusterOperator,
)

CLUSTER_ID = "test_cluster"

EXPECTED_RESPONSE = {"db_cluster_id": CLUSTER_ID}


@pytest.fixture
def hook() -> Generator[NeptuneHook, None, None]:
    with mock_neptune():
        yield NeptuneHook(aws_conn_id="aws_default")


@pytest.fixture
def _create_cluster(hook: NeptuneHook):
    hook.conn.create_db_cluster(
        DBClusterIdentifier=CLUSTER_ID,
        Engine="neptune",
    )
    if not hook.conn.describe_db_clusters()["DBClusters"]:
        raise ValueError("AWS not properly mocked")


class TestNeptuneStartClusterOperator:
    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("cluster_available")
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_no_wait(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_not_called()
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_start_cluster_cluster_available(self, mock_waiter, mock_get_cluster_status, mock_conn):
        mock_get_cluster_status.return_value = "available"
        operator = NeptuneStartDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)

        mock_conn.start_db_cluster.assert_not_called()
        mock_waiter.assert_not_called()
        assert resp == {"db_cluster_id": CLUSTER_ID}


class TestNeptuneStopClusterOperator:
    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_called_once_with("cluster_stopped")
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_no_wait(self, mock_hook_get_waiter, mock_conn):
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=False,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)
        mock_hook_get_waiter.assert_not_called()
        assert resp == EXPECTED_RESPONSE

    @mock.patch.object(NeptuneHook, "conn")
    @mock.patch.object(NeptuneHook, "get_cluster_status")
    @mock.patch.object(NeptuneHook, "get_waiter")
    def test_stop_cluster_cluster_stopped(self, mock_waiter, mock_get_cluster_status, mock_conn):
        mock_get_cluster_status.return_value = "stopped"
        operator = NeptuneStopDbClusterOperator(
            task_id="task_test",
            db_cluster_id=CLUSTER_ID,
            deferrable=False,
            wait_for_completion=True,
            aws_conn_id="aws_default",
        )

        resp = operator.execute(None)

        mock_conn.stop_db_cluster.assert_not_called()
        mock_waiter.assert_not_called()
        assert resp == {"db_cluster_id": CLUSTER_ID}
