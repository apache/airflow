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

from airflow.providers.amazon.aws.triggers.rds import (
    RdsDbAvailableTrigger,
    RdsDbDeletedTrigger,
    RdsDbStoppedTrigger,
)
from airflow.providers.amazon.aws.utils.rds import RdsDbType

TEST_DB_INSTANCE_IDENTIFIER = "test-db-instance-identifier"
TEST_WAITER_DELAY = 10
TEST_WAITER_MAX_ATTEMPTS = 10
TEST_AWS_CONN_ID = "test-aws-id"
TEST_REGION = "sa-east-1"
TEST_RESPONSE = {
    "DBInstance": {
        "DBInstanceIdentifier": "test-db-instance-identifier",
        "DBInstanceStatus": "test-db-instance-status",
    }
}


class TestRdsTriggers:
    @pytest.mark.parametrize(
        "trigger",
        [
            RdsDbAvailableTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
                aws_conn_id=TEST_AWS_CONN_ID,
                region_name=TEST_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
            ),
            RdsDbDeletedTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
                aws_conn_id=TEST_AWS_CONN_ID,
                region_name=TEST_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
            ),
            RdsDbStoppedTrigger(
                db_identifier=TEST_DB_INSTANCE_IDENTIFIER,
                waiter_delay=TEST_WAITER_DELAY,
                waiter_max_attempts=TEST_WAITER_MAX_ATTEMPTS,
                aws_conn_id=TEST_AWS_CONN_ID,
                region_name=TEST_REGION,
                response=TEST_RESPONSE,
                db_type=RdsDbType.INSTANCE,
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
