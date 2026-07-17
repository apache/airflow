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

import boto3
import pytest
from botocore.waiter import WaiterModel

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.waiters.base_waiter import BaseBotoWaiter


def assert_all_match(*args):
    assert len(set(args)) == 1


class TestBaseWaiter:
    def test_init(self):
        waiter_name = "test_waiter"
        client_name = "test_client"
        waiter_model_config = {
            "version": 2,
            "waiters": {
                waiter_name: {
                    "operation": "ListNodegroups",
                    "delay": 30,
                    "maxAttempts": 60,
                    "acceptors": [
                        {
                            "matcher": "path",
                            "argument": "length(nodegroups[]) == `0`",
                            "expected": True,
                            "state": "success",
                        },
                        {
                            "matcher": "path",
                            "expected": True,
                            "argument": "length(nodegroups[]) > `0`",
                            "state": "retry",
                        },
                    ],
                }
            },
        }
        expected_model = WaiterModel(waiter_model_config)

        waiter = BaseBotoWaiter(client_name, waiter_model_config)

        # WaiterModel objects don't implement an eq() so equivalence checking manually.
        for attr in expected_model.__dict__:
            assert waiter.model.__getattribute__(attr) == expected_model.__getattribute__(attr)
        assert waiter.client == client_name

    @pytest.mark.parametrize("boto_type", ["client", "resource"])
    def test_get_botocore_waiter(self, boto_type, monkeypatch):
        kw = {f"{boto_type}_type": "s3"}
        if boto_type == "client":
            fake_client = boto3.client("s3", region_name="eu-west-3")
        elif boto_type == "resource":
            fake_client = boto3.resource("s3", region_name="eu-west-3")
        else:
            raise ValueError(f"Unexpected value {boto_type!r} for `boto_type`.")
        monkeypatch.setattr(AwsBaseHook, "conn", fake_client)

        hook = AwsBaseHook(**kw)
        with mock.patch("botocore.client.BaseClient.get_waiter") as m:
            hook.get_waiter(waiter_name="FooBar")
            m.assert_called_once_with("FooBar")
