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
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.sensors.athena_spark import AthenaSparkSensor


@pytest.fixture
def mock_check_spark_calculation_status():
    with mock.patch.object(
        AthenaHook,
        "check_spark_calculation_status",
        autospec=True,
    ) as m:
        yield m


@pytest.fixture
def mock_get_spark_calculation_state_change_reason():
    with mock.patch.object(
        AthenaHook,
        "get_spark_calculation_state_change_reason",
        autospec=True,
    ) as m:
        yield m


@mock_aws
class TestAthenaSparkSensor:
    def setup_method(self, _):
        self.default_op_kwargs = {
            "task_id": "test_athena_spark_sensor",
            "calculation_execution_id": "abc",
        }
        self.sensor = AthenaSparkSensor(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = AthenaSparkSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.hook.log_query is True

        op = AthenaSparkSensor(
            **self.default_op_kwargs,
            aws_conn_id="aws-test-custom-conn",
            region_name="eu-west-1",
            verify=False,
            botocore_config={"read_timeout": 42},
        )
        assert op.hook.aws_conn_id == "aws-test-custom-conn"
        assert op.hook._region_name == "eu-west-1"
        assert op.hook._verify is False
        assert op.hook._config is not None
        assert op.hook._config.read_timeout == 42

    def test_poke_success_state(self, mock_check_spark_calculation_status):
        mock_check_spark_calculation_status.return_value = "COMPLETED"

        assert self.sensor.poke({}) is True

        mock_check_spark_calculation_status.assert_called_once_with(
            mock.ANY,
            calculation_execution_id="abc",
        )

    @pytest.mark.parametrize(
        "state",
        ["CREATING", "CREATED", "QUEUED", "RUNNING", "CANCELING"],
    )
    def test_poke_intermediate_states(
        self,
        state,
        mock_check_spark_calculation_status,
    ):
        mock_check_spark_calculation_status.return_value = state

        assert self.sensor.poke({}) is False

        mock_check_spark_calculation_status.assert_called_once_with(
            mock.ANY,
            calculation_execution_id="abc",
        )

    @pytest.mark.parametrize(
        ("state", "reason", "expected_reason"),
        [
            pytest.param(
                "FAILED",
                "Calculation failed",
                "Calculation failed",
                id="failed-with-reason",
            ),
            pytest.param(
                "CANCELED",
                None,
                "Unknown",
                id="canceled-without-reason",
            ),
        ],
    )
    def test_poke_failure_states(
        self,
        state,
        reason,
        expected_reason,
        mock_check_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
    ):
        mock_check_spark_calculation_status.return_value = state
        mock_get_spark_calculation_state_change_reason.return_value = reason

        with pytest.raises(
            RuntimeError,
            match=rf"failed with state {state}\. Reason: {expected_reason}",
        ):
            self.sensor.poke({})

        mock_check_spark_calculation_status.assert_called_once_with(
            mock.ANY,
            calculation_execution_id="abc",
        )
        mock_get_spark_calculation_state_change_reason.assert_called_once_with(
            mock.ANY,
            calculation_execution_id="abc",
        )

    @pytest.mark.parametrize(
        "state",
        [
            pytest.param(None, id="missing-state"),
            pytest.param("UNKNOWN", id="unexpected-state"),
        ],
    )
    def test_poke_unexpected_states(
        self,
        state,
        mock_check_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
    ):
        mock_check_spark_calculation_status.return_value = state

        with pytest.raises(
            RuntimeError,
            match="Unexpected Athena Spark calculation state",
        ):
            self.sensor.poke({})

        mock_check_spark_calculation_status.assert_called_once_with(
            mock.ANY,
            calculation_execution_id="abc",
        )
        mock_get_spark_calculation_state_change_reason.assert_not_called()
