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
from unittest.mock import AsyncMock

import pytest

from airflow.providers.amazon.aws.triggers.redshift_cluster import RedshiftCreateClusterTrigger
from airflow.triggers.base import TriggerEvent

TEST_CLUSTER_IDENTIFIER = "test-cluster"
TEST_POLL_INTERVAL = 10
TEST_MAX_ATTEMPT = 10
TEST_AWS_CONN_ID = "test-aws-id"


class TestRedshiftCreateClusterTrigger:
    def test_redshift_create_cluster_trigger_serialize(self):
        redshift_create_cluster_trigger = RedshiftCreateClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempt=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )
        class_path, args = redshift_create_cluster_trigger.serialize()
        assert (
            class_path
            == "airflow.providers.amazon.aws.triggers.redshift_cluster.RedshiftCreateClusterTrigger"
        )
        assert args["cluster_identifier"] == TEST_CLUSTER_IDENTIFIER
        assert args["poll_interval"] == str(TEST_POLL_INTERVAL)
        assert args["max_attempt"] == str(TEST_MAX_ATTEMPT)
        assert args["aws_conn_id"] == TEST_AWS_CONN_ID

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.async_conn")
    async def test_redshift_create_cluster_trigger_run(self, mock_async_conn):
        the_mock = mock.MagicMock()
        mock_async_conn.__aenter__.return_value = the_mock
        the_mock.get_waiter().wait = AsyncMock()

        redshift_create_cluster_trigger = RedshiftCreateClusterTrigger(
            cluster_identifier=TEST_CLUSTER_IDENTIFIER,
            poll_interval=TEST_POLL_INTERVAL,
            max_attempt=TEST_MAX_ATTEMPT,
            aws_conn_id=TEST_AWS_CONN_ID,
        )

        generator = redshift_create_cluster_trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "message": "Cluster Created"})
