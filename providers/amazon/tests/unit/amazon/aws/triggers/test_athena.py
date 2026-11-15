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

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.triggers.athena import AthenaSparkTrigger, AthenaTrigger


class TestAthenaTrigger:
    def test_serialization(self):
        query_execution_id = "test_query_execution_id"
        waiter_delay = 30
        waiter_max_attempts = 60
        aws_conn_id = "aws_default"

        trigger = AthenaTrigger(
            query_execution_id=query_execution_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.amazon.aws.triggers.athena.AthenaTrigger"
        assert kwargs == {
            "query_execution_id": "test_query_execution_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
        }


class TestAthenaSparkTrigger:
    @pytest.mark.parametrize(
        "aws_kwargs",
        [
            pytest.param({}, id="defaults"),
            pytest.param(
                {
                    "region_name": "eu-west-1",
                    "verify": False,
                    "botocore_config": {"read_timeout": 42},
                },
                id="custom-aws-settings",
            ),
        ],
    )
    def test_serialization(self, aws_kwargs):
        expected_kwargs = {
            "calculation_execution_id": "test_calculation_execution_id",
            "waiter_delay": 30,
            "waiter_max_attempts": 60,
            "aws_conn_id": "aws_default",
            **aws_kwargs,
        }
        trigger = AthenaSparkTrigger(**expected_kwargs)

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.amazon.aws.triggers.athena.AthenaSparkTrigger"
        assert kwargs == expected_kwargs
        assert AthenaSparkTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_waiter_configuration(self):
        calculation_execution_id = "test_calculation_execution_id"

        trigger = AthenaSparkTrigger(
            calculation_execution_id=calculation_execution_id,
            waiter_delay=30,
            waiter_max_attempts=60,
            aws_conn_id="aws_default",
        )

        assert trigger.aws_hook_class is AthenaHook
        assert trigger.waiter_name == "spark_calculation_terminal"
        assert trigger.waiter_args == {
            "CalculationExecutionId": calculation_execution_id,
        }
        assert trigger.status_queries == ["Status.State", "Status.StateChangeReason"]
        assert trigger.return_value == calculation_execution_id
