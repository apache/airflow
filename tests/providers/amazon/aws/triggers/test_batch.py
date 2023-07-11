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

from airflow.providers.amazon.aws.triggers.batch import (
    BatchCreateComputeEnvironmentTrigger,
    BatchJobTrigger,
)

BATCH_JOB_ID = "job_id"
POLL_INTERVAL = 5
MAX_ATTEMPT = 5
AWS_CONN_ID = "aws_batch_job_conn"
AWS_REGION = "us-east-2"
pytest.importorskip("aiobotocore")


class TestBatchTrigger:
    @pytest.mark.parametrize(
        "trigger",
        [
            BatchJobTrigger(
                job_id=BATCH_JOB_ID,
                waiter_delay=POLL_INTERVAL,
                waiter_max_attempts=MAX_ATTEMPT,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
            ),
            BatchCreateComputeEnvironmentTrigger(
                compute_env_arn="my_arn",
                waiter_delay=POLL_INTERVAL,
                waiter_max_attempts=MAX_ATTEMPT,
                aws_conn_id=AWS_CONN_ID,
                region_name=AWS_REGION,
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
