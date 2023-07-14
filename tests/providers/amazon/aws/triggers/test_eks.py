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

import pytest

from airflow.providers.amazon.aws.triggers.eks import (
    EksCreateFargateProfileTrigger,
    EksCreateNodegroupTrigger,
    EksDeleteFargateProfileTrigger,
    EksDeleteNodegroupTrigger,
)

TEST_CLUSTER_IDENTIFIER = "test-cluster"
TEST_FARGATE_PROFILE_NAME = "test-fargate-profile"
TEST_NODEGROUP_NAME = "test-nodegroup"
TEST_WAITER_DELAY = 10
TEST_WAITER_MAX_ATTEMPTS = 10
TEST_AWS_CONN_ID = "test-aws-id"
TEST_REGION = "test-region"


class TestEksTriggers:
    @pytest.mark.parametrize(
        "trigger",
        [
            EksCreateFargateProfileTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            ),
            EksDeleteFargateProfileTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                fargate_profile_name=TEST_FARGATE_PROFILE_NAME,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
            ),
            EksCreateNodegroupTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                nodegroup_name=TEST_NODEGROUP_NAME,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
                region_name=TEST_REGION,
            ),
            EksDeleteNodegroupTrigger(
                cluster_name=TEST_CLUSTER_IDENTIFIER,
                nodegroup_name=TEST_NODEGROUP_NAME,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
                region_name=TEST_REGION,
            ),
        ],
    )
    def test_serialize_recreate(self, trigger):
        class_path, args = trigger.serialize()

        class_name = class_path.split(".")[-1]
        clazz = globals()[class_name]
        instance = clazz(**args)

        class_path2, args2 = instance.serialize()

        assert class_path == class_path2
        assert args == args2
