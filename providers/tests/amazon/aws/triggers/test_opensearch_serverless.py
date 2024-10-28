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

from airflow.providers.amazon.aws.hooks.opensearch_serverless import (
    OpenSearchServerlessHook,
)
from airflow.providers.amazon.aws.triggers.opensearch_serverless import (
    OpenSearchServerlessCollectionActiveTrigger,
)
from airflow.triggers.base import TriggerEvent
from airflow.utils.helpers import prune_dict

from providers.tests.amazon.aws.triggers.test_base import TestAwsBaseWaiterTrigger

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
        "collection_name, collection_id, expected_pass",
        [
            pytest.param(COLLECTION_NAME, COLLECTION_ID, False, id="both_provided_fails"),
            pytest.param(COLLECTION_NAME, None, True, id="only_name_provided_passes"),
            pytest.param(None, COLLECTION_ID, True, id="only_id_provided_passes"),
        ],
    )
    def test_serialization(self, collection_name, collection_id, expected_pass):
        """Assert that arguments and classpath are correctly serialized."""
        call_args = prune_dict(
            {"collection_id": collection_id, "collection_name": collection_name}
        )

        if expected_pass:
            trigger = OpenSearchServerlessCollectionActiveTrigger(**call_args)
            classpath, kwargs = trigger.serialize()
            assert (
                classpath
                == BASE_TRIGGER_CLASSPATH + "OpenSearchServerlessCollectionActiveTrigger"
            )
            if call_args.get("collection_name"):
                assert kwargs.get("collection_name") == self.COLLECTION_NAME
            if call_args.get("collection_id"):
                assert kwargs.get("collection_id") == self.COLLECTION_ID

        if not expected_pass:
            with pytest.raises(
                AttributeError,
                match="Either collection_ids or collection_names must be provided, not both.",
            ):
                OpenSearchServerlessCollectionActiveTrigger(**call_args)

    @pytest.mark.asyncio
    @mock.patch.object(OpenSearchServerlessHook, "get_waiter")
    @mock.patch.object(OpenSearchServerlessHook, "async_conn")
    async def test_run_success(self, mock_async_conn, mock_get_waiter):
        mock_async_conn.__aenter__.return_value = mock.MagicMock()
        mock_get_waiter().wait = AsyncMock()
        trigger = OpenSearchServerlessCollectionActiveTrigger(
            collection_id=self.COLLECTION_ID
        )

        generator = trigger.run()
        response = await generator.asend(None)

        assert response == TriggerEvent(
            {"status": "success", "collection_id": self.COLLECTION_ID}
        )
        assert mock_get_waiter().wait.call_count == 1
