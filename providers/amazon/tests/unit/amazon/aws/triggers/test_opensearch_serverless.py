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

from airflow.providers.amazon.aws.hooks.opensearch_serverless import OpenSearchServerlessHook
from airflow.providers.amazon.aws.triggers.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveTrigger,
)
from airflow.triggers.base import TriggerEvent
from airflow.utils.helpers import prune_dict

from unit.amazon.aws.triggers.test_base import TestAwsBaseWaiterTrigger

BASE_TRIGGER_CLASSPATH = "airflow.providers.amazon.aws.triggers.opensearch_serverless."


class TestBaseBedrockTrigger(TestAwsBaseWaiterTrigger):
    EXPECTED_WAITER_NAME: str | None = None

    def test_setup(self):
        # Ensure that all subclasses have an expected waiter name set.
        if self.__class__.__name__ != "TestBaseBedrockTrigger":
            assert isinstance(self.EXPECTED_WAITER_NAME, str)


class TestOpenSearchServerlessCollectionActiveTrigger:
    EXPECTED_WAITER_NAME = "collection_available"
    COLLECTION_NAME = "test_collection_name"
    COLLECTION_ID = "test_collection_id"

    @pytest.mark.parametrize(
        ("collection_name", "collection_id"),
        [
            pytest.param(COLLECTION_NAME, None, id="collection_name"),
            pytest.param(None, COLLECTION_ID, id="collection_id"),
        ],
    )
    def test_serialization_round_trip(self, collection_name, collection_id):
        """Assert that all arguments survive serialization and reconstruction."""
        trigger = OpenSearchServerlessCollectionActiveTrigger(
            **prune_dict({"collection_id": collection_id, "collection_name": collection_name}),
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )

        classpath, kwargs = trigger.serialize()

        assert classpath == BASE_TRIGGER_CLASSPATH + "OpenSearchServerlessCollectionActiveTrigger"
        assert kwargs == {
            "collection_id": collection_id,
            "collection_name": collection_name,
            "waiter_delay": 60,
            "waiter_max_attempts": 20,
            "aws_conn_id": "test_conn",
            "region_name": "eu-west-1",
            "verify": False,
            "botocore_config": {"read_timeout": 42},
        }
        restored_trigger = OpenSearchServerlessCollectionActiveTrigger(**kwargs)
        assert restored_trigger.serialize() == (classpath, kwargs)

    def test_both_collection_name_and_id_raise(self):
        with pytest.raises(
            AttributeError, match="Either collection_ids or collection_names must be provided, not both."
        ):
            OpenSearchServerlessCollectionActiveTrigger(
                collection_name=self.COLLECTION_NAME, collection_id=self.COLLECTION_ID
            )

    @mock.patch(BASE_TRIGGER_CLASSPATH + "OpenSearchServerlessHook")
    def test_hook_forwards_aws_configuration(self, mock_hook_class):
        trigger = OpenSearchServerlessCollectionActiveTrigger(
            collection_id=self.COLLECTION_ID,
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            botocore_config={"read_timeout": 42},
        )

        trigger.hook()

        mock_hook_class.assert_called_once_with(
            aws_conn_id="test_conn",
            region_name="eu-west-1",
            verify="/path/to/ca-bundle.crt",
            config={"read_timeout": 42},
        )

    @pytest.mark.asyncio
    @mock.patch.object(OpenSearchServerlessHook, "get_waiter")
    @mock.patch.object(OpenSearchServerlessHook, "get_async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = OpenSearchServerlessCollectionActiveTrigger(collection_id=self.COLLECTION_ID)

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent({"status": "success", "collection_id": self.COLLECTION_ID})
        assert mock_get_waiter().wait.call_count == 1
