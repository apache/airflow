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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.sensors.kinesis_analytics import (
    KinesisAnalyticsV2StartApplicationCompletedSensor,
    KinesisAnalyticsV2StopApplicationCompletedSensor,
)


class TestKinesisAnalyticsV2StartApplicationCompletedSensor:
    SENSOR = KinesisAnalyticsV2StartApplicationCompletedSensor
    APPLICATION_ARN = "arn:aws:kinesisanalytics:us-east-1:123456789012:application/demo"

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="start_application_sensor",
            application_name="demo",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_poke_success_state(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }

        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_intermediate_state(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }

        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

        with pytest.raises(
            AirflowException,
            match="AWS Managed Service for Apache Flink application start failed",
        ):
            sensor.poke({})


class TestKinesisAnalyticsV2StopApplicationCompletedSensor:
    SENSOR = KinesisAnalyticsV2StopApplicationCompletedSensor
    APPLICATION_ARN = "arn:aws:kinesisanalytics:us-east-1:123456789012:application/demo"

    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="stop_application_sensor",
            application_name="demo",
            poke_interval=5,
            max_retries=1,
        )
        self.sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = self.SENSOR(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = self.SENSOR(
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

    @pytest.mark.parametrize("state", SENSOR.SUCCESS_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_poke_success_state(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }

        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", SENSOR.INTERMEDIATE_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_intermediate_state(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize("state", SENSOR.FAILURE_STATES)
    @mock.patch.object(KinesisAnalyticsV2Hook, "conn")
    def test_poke_failure_states(self, mock_conn, state):
        mock_conn.describe_application.return_value = {
            "ApplicationDetail": {
                "ApplicationARN": self.APPLICATION_ARN,
                "ApplicationStatus": state,
            }
        }

        sensor = self.SENSOR(**self.default_op_kwargs, aws_conn_id=None)

        with pytest.raises(
            AirflowException,
            match="AWS Managed Service for Apache Flink application stop failed",
        ):
            sensor.poke({})
