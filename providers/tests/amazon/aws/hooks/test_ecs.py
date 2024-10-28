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

from unittest import mock

import pytest

from airflow.providers.amazon.aws.exceptions import EcsOperatorError, EcsTaskFailToStart
from airflow.providers.amazon.aws.hooks.ecs import EcsHook, should_retry, should_retry_eni

DEFAULT_CONN_ID: str = "aws_default"
REGION: str = "us-east-1"


@pytest.fixture
def mock_conn():
    with mock.patch.object(EcsHook, "conn") as _conn:
        yield _conn


class TestEksHooks:
    def test_hook(self) -> None:
        hook = EcsHook(region_name=REGION)
        assert hook.conn is not None
        assert hook.aws_conn_id == DEFAULT_CONN_ID
        assert hook.region_name == REGION

    def test_get_cluster_state(self, mock_conn) -> None:
        mock_conn.describe_clusters.return_value = {"clusters": [{"status": "ACTIVE"}]}
        assert EcsHook().get_cluster_state(cluster_name="cluster_name") == "ACTIVE"

    def test_get_task_definition_state(self, mock_conn) -> None:
        mock_conn.describe_task_definition.return_value = {
            "taskDefinition": {"status": "ACTIVE"}
        }
        assert (
            EcsHook().get_task_definition_state(task_definition="task_name") == "ACTIVE"
        )

    def test_get_task_state(self, mock_conn) -> None:
        mock_conn.describe_tasks.return_value = {"tasks": [{"lastStatus": "ACTIVE"}]}
        assert (
            EcsHook().get_task_state(cluster="cluster_name", task="task_name") == "ACTIVE"
        )


class TestShouldRetry:
    def test_return_true_on_valid_reason(self):
        assert should_retry(EcsOperatorError([{"reason": "RESOURCE:MEMORY"}], "Foo"))

    def test_return_false_on_invalid_reason(self):
        assert not should_retry(
            EcsOperatorError([{"reason": "CLUSTER_NOT_FOUND"}], "Foo")
        )


class TestShouldRetryEni:
    def test_return_true_on_valid_reason(self):
        assert should_retry_eni(
            EcsTaskFailToStart(
                "The task failed to start due to: "
                "Timeout waiting for network interface provisioning to complete."
            )
        )

    def test_return_false_on_invalid_reason(self):
        assert not should_retry_eni(
            EcsTaskFailToStart(
                "The task failed to start due to: "
                "CannotPullContainerError: "
                "ref pull has been retried 5 time(s): failed to resolve reference"
            )
        )
