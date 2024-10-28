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

from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
    from airflow.triggers.base import TriggerEvent


class TestImplem(AwsBaseWaiterTrigger):
    """An empty implementation that allows instantiation for tests."""

    __test__ = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def hook(self) -> AwsGenericHook:
        return MagicMock()


class TestAwsBaseWaiterTrigger:
    def setup_method(self):
        self.trigger = TestImplem(
            serialized_fields={},
            waiter_name="",
            waiter_args={},
            failure_message="",
            status_message="",
            status_queries=[],
            return_value=None,
            waiter_delay=0,
            waiter_max_attempts=0,
            aws_conn_id="",
        )

    def test_region_serialized(self):
        self.trigger.region_name = "my_region"
        _, args = self.trigger.serialize()

        assert "region_name" in args
        assert args["region_name"] == "my_region"

    @pytest.mark.parametrize(
        "verify", [True, False, pytest.param("/foo/bar.pem", id="path")]
    )
    def test_verify_serialized(self, verify):
        self.trigger.verify = verify
        _, args = self.trigger.serialize()

        assert "verify" in args
        assert args["verify"] == verify

    @pytest.mark.parametrize(
        "botocore_config",
        [
            pytest.param(
                {"read_timeout": 10, "connect_timeout": 42, "keepalive": True},
                id="non-empty-dict",
            ),
            pytest.param({}, id="empty-dict"),
        ],
    )
    def test_botocore_config_serialized(self, botocore_config):
        self.trigger.botocore_config = botocore_config
        _, args = self.trigger.serialize()

        assert "botocore_config" in args
        assert args["botocore_config"] == botocore_config

    @pytest.mark.parametrize("param_name", ["region_name", "verify", "botocore_config"])
    def test_hooks_args_not_serialized_if_omitted(self, param_name):
        _, args = self.trigger.serialize()

        assert param_name not in args

    def test_region_name_not_serialized_if_empty_string(self):
        """
        Compatibility with previous behaviour when empty string region name not serialised.

        It would evaluate as None, however empty string it is not valid region name in boto3.
        """
        self.trigger.region_name = ""
        _, args = self.trigger.serialize()

        assert "region_name" not in args

    def test_serialize_extra_fields(self):
        self.trigger.serialized_fields = {"foo": "bar", "foz": "baz"}

        _, args = self.trigger.serialize()

        assert "foo" in args
        assert args["foo"] == "bar"
        assert "foz" in args
        assert args["foz"] == "baz"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.amazon.aws.triggers.base.async_wait")
    async def test_run(self, wait_mock: MagicMock):
        self.trigger.return_key = "hello"
        self.trigger.return_value = "world"

        generator = self.trigger.run()
        res: TriggerEvent = await generator.asend(None)

        wait_mock.assert_called_once()
        assert res.payload["status"] == "success"
        assert res.payload["hello"] == "world"
