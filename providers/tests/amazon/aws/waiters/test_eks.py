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

import json

import boto3
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.eks import EksHook

from providers.tests.amazon.aws.waiters.test_custom_waiters import assert_all_match


class TestCustomEKSServiceWaiters:
    def test_service_waiters(self):
        hook = EksHook()
        with open(hook.waiter_path) as config_file:
            expected_waiters = json.load(config_file)["waiters"]

        for waiter in list(expected_waiters.keys()):
            assert waiter in hook.list_waiters()
            assert waiter in hook._list_custom_waiters()

    @mock_aws
    def test_existing_waiter_inherited(self):
        """
        AwsBaseHook::get_waiter will first check if there is a custom waiter with the
        provided name and pass that through is it exists, otherwise it will check the
        custom waiters for the given service.  This test checks to make sure that the
        waiter is the same whichever way you get it and no modifications are made.
        """
        hook_waiter = EksHook().get_waiter("cluster_active")
        client_waiter = EksHook().conn.get_waiter("cluster_active")
        boto_waiter = boto3.client("eks").get_waiter("cluster_active")

        assert_all_match(hook_waiter.name, client_waiter.name, boto_waiter.name)
        assert_all_match(
            len(hook_waiter.__dict__),
            len(client_waiter.__dict__),
            len(boto_waiter.__dict__),
        )
        for attr in hook_waiter.__dict__:
            # Not all attributes in a Waiter are directly comparable
            # so the best we can do it make sure the same attrs exist.
            assert hasattr(boto_waiter, attr)
            assert hasattr(client_waiter, attr)
