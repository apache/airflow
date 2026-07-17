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

from airflow.providers.amazon.aws.triggers.rds import (
    RdsDbAvailableTrigger,
    RdsDbDeletedTrigger,
    RdsDbStoppedTrigger,
)


class TestRdsDbAvailableTrigger:
    def test_serialization(self):
        db_identifier = "test_db_identifier"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"
        response = {"key": "value"}
        db_type = "instance"  # or use an instance of RdsDbType if available
        region_name = "us-west-2"

        trigger = RdsDbAvailableTrigger(
            db_identifier=db_identifier,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            response=response,
            db_type=db_type,
            region_name=region_name,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.rds.RdsDbAvailableTrigger"
        assert kwargs == {
            "db_identifier": "test_db_identifier",
            "db_type": "instance",
            "response": {"key": "value"},
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
            "region_name": "us-west-2",
        }


class TestRdsDbDeletedTrigger:
    def test_serialization(self):
        db_identifier = "test_db_identifier"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"
        response = {"key": "value"}
        db_type = "instance"
        region_name = "us-west-2"

        trigger = RdsDbDeletedTrigger(
            db_identifier=db_identifier,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            response=response,
            db_type=db_type,
            region_name=region_name,
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.rds.RdsDbDeletedTrigger"
        assert kwargs == {
            "db_identifier": "test_db_identifier",
            "db_type": "instance",
            "response": {"key": "value"},
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
            "region_name": "us-west-2",
        }


class TestRdsDbStoppedTrigger:
    def test_serialization(self):
        db_identifier = "test_db_identifier"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"
        response = {"key": "value"}
        db_type = "instance"
        region_name = "us-west-2"

        trigger = RdsDbStoppedTrigger(
            db_identifier=db_identifier,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            response=response,
            db_type=db_type,
            region_name=region_name,
        )

        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.rds.RdsDbStoppedTrigger"
        assert kwargs == {
            "db_identifier": "test_db_identifier",
            "db_type": "instance",
            "response": {"key": "value"},
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
            "region_name": "us-west-2",
        }
