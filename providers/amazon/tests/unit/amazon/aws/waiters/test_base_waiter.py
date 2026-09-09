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

from unittest import mock

import pytest
from botocore.waiter import Waiter, WaiterModel

from airflow.providers.amazon.aws.waiters.base_waiter import BaseBotoWaiter

SAMPLE_WAITER_MODEL_CONFIG = {
    "version": 2,
    "waiters": {
        "sample_waiter": {
            "operation": "DescribeInstances",
            "delay": 15,
            "maxAttempts": 40,
            "acceptors": [
                {
                    "matcher": "path",
                    "expected": "running",
                    "state": "success",
                    "argument": "status",
                }
            ],
        }
    },
}


class TestBaseBotoWaiter:
    @pytest.fixture
    def mock_client(self):
        client = mock.MagicMock()
        client.meta.service_model.metadata = {"serviceFullName": "EC2"}
        return client

    def test_init_defaults(self, mock_client):
        waiter = BaseBotoWaiter(client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG)
        assert waiter.client is mock_client
        assert isinstance(waiter.model, WaiterModel)
        assert waiter.deferrable is False

    def test_init_deferrable(self, mock_client):
        waiter = BaseBotoWaiter(client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG, deferrable=True)
        assert waiter.client is mock_client
        assert isinstance(waiter.model, WaiterModel)
        assert waiter.deferrable is True

    @mock.patch("airflow.providers.amazon.aws.waiters.base_waiter.create_waiter_with_client")
    def test_waiter_sync(self, mock_create_waiter, mock_client):
        waiter_instance = mock.MagicMock(spec=Waiter)
        mock_create_waiter.return_value = waiter_instance

        boto_waiter = BaseBotoWaiter(
            client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG, deferrable=False
        )
        result = boto_waiter.waiter("sample_waiter")

        mock_create_waiter.assert_called_once_with(
            waiter_name="sample_waiter", waiter_model=boto_waiter.model, client=mock_client
        )
        assert result is waiter_instance

    @mock.patch("aiobotocore.waiter.create_waiter_with_client")
    def test_waiter_deferrable(self, mock_async_create_waiter, mock_client):
        async_waiter_instance = mock.MagicMock()
        mock_async_create_waiter.return_value = async_waiter_instance

        boto_waiter = BaseBotoWaiter(
            client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG, deferrable=True
        )
        result = boto_waiter.waiter("sample_waiter")

        mock_async_create_waiter.assert_called_once_with(
            waiter_name="sample_waiter", waiter_model=boto_waiter.model, client=mock_client
        )
        assert result is async_waiter_instance

    def test_waiter_with_real_botocore_creation(self, mock_client):
        boto_waiter = BaseBotoWaiter(
            client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG, deferrable=False
        )
        real_waiter = boto_waiter.waiter("sample_waiter")
        assert isinstance(real_waiter, Waiter)
        assert real_waiter.name == "sample_waiter"

    def test_waiter_unknown_name_raises_value_error(self, mock_client):
        boto_waiter = BaseBotoWaiter(
            client=mock_client, model_config=SAMPLE_WAITER_MODEL_CONFIG, deferrable=False
        )
        with pytest.raises(ValueError, match="Waiter does not exist: unknown_waiter"):
            boto_waiter.waiter("unknown_waiter")
