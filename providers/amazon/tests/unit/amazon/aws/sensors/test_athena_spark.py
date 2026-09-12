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
    with mock.patch.object(AthenaHook, "check_spark_calculation_status") as m:
        yield m


@pytest.fixture
def mock_get_spark_calculation_state_change_reason():
    with mock.patch.object(AthenaHook, "get_spark_calculation_state_change_reason") as m:
        yield m


@mock_aws
class TestAthenaSparkSensor:
    def setup_method(self, _):
        self.default_op_kwargs = dict(
            task_id="test_athena_spark_sensor",
            calculation_execution_id="abc",
        )
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

    def test_template_fields(self):
        assert AthenaSparkSensor.template_fields == (
            "calculation_execution_id",
            "aws_conn_id",
            "region_name",
        )

    @pytest.mark.parametrize("state", ["COMPLETED"])
    def test_poke_success_states(self, state, mock_check_spark_calculation_status):
        mock_check_spark_calculation_status.side_effect = [state]

        assert self.sensor.poke({}) is True
        mock_check_spark_calculation_status.assert_called_once_with(calculation_execution_id="abc")

    @pytest.mark.parametrize("state", ["CREATING", "CREATED", "QUEUED", "RUNNING"])
    def test_poke_intermediate_states(self, state, mock_check_spark_calculation_status):
        mock_check_spark_calculation_status.side_effect = [state]

        assert self.sensor.poke({}) is False
        mock_check_spark_calculation_status.assert_called_once_with(calculation_execution_id="abc")

    @pytest.mark.parametrize("state", ["FAILED", "CANCELED"])
    def test_poke_failure_states(
        self,
        state,
        mock_check_spark_calculation_status,
        mock_get_spark_calculation_state_change_reason,
    ):
        mock_check_spark_calculation_status.side_effect = [state]
        mock_get_spark_calculation_state_change_reason.return_value = "Calculation failed"

        with pytest.raises(RuntimeError, match=f"failed with state {state}"):
            self.sensor.poke({})

        mock_check_spark_calculation_status.assert_called_once_with(calculation_execution_id="abc")
        mock_get_spark_calculation_state_change_reason.assert_called_once_with(calculation_execution_id="abc")
