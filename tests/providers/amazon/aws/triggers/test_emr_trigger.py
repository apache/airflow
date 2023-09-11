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

from airflow.providers.amazon.aws.triggers.emr import (
    EmrContainerTrigger,
    EmrCreateJobFlowTrigger,
    EmrStepSensorTrigger,
    EmrTerminateJobFlowTrigger,
)

TEST_JOB_FLOW_ID = "test_job_flow_id"
TEST_STEP_IDS = ["step1", "step2"]
TEST_AWS_CONN_ID = "test-aws-id"
TEST_MAX_ATTEMPTS = 10
TEST_POLL_INTERVAL = 10


class TestEmrTriggers:
    @pytest.mark.parametrize(
        "trigger",
        [
            EmrCreateJobFlowTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_POLL_INTERVAL,
                waiter_max_attempts=TEST_MAX_ATTEMPTS,
            ),
            EmrTerminateJobFlowTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_POLL_INTERVAL,
                waiter_max_attempts=TEST_MAX_ATTEMPTS,
            ),
            EmrContainerTrigger(
                virtual_cluster_id="my_cluster_id",
                job_id=TEST_JOB_FLOW_ID,
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_POLL_INTERVAL,
                waiter_max_attempts=TEST_MAX_ATTEMPTS,
            ),
            EmrStepSensorTrigger(
                job_flow_id=TEST_JOB_FLOW_ID,
                step_id="my_step",
                aws_conn_id=TEST_AWS_CONN_ID,
                waiter_delay=TEST_POLL_INTERVAL,
                waiter_max_attempts=TEST_MAX_ATTEMPTS,
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
