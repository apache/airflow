#
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

from airflow.providers.amazon.aws.sensors.glacier import GlacierJobOperationSensor, JobStatus
from airflow.providers.common.compat.sdk import AirflowException

SUCCEEDED = "Succeeded"
IN_PROGRESS = "InProgress"


@pytest.fixture
def mocked_describe_job():
    with mock.patch("airflow.providers.amazon.aws.sensors.glacier.GlacierHook.describe_job") as m:
        yield m


class TestAmazonGlacierSensor:
    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_athena_sensor",
            vault_name="airflow",
            job_id="1a2b3c4d",
            poke_interval=60 * 20,
        )
        self.op = GlacierJobOperationSensor(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = GlacierJobOperationSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None

        op = GlacierJobOperationSensor(
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

    def test_poke_succeeded(self, mocked_describe_job):
        mocked_describe_job.side_effect = [{"Action": "", "StatusCode": JobStatus.SUCCEEDED.value}]
        assert self.op.poke(None)

    def test_poke_in_progress(self, mocked_describe_job):
        mocked_describe_job.side_effect = [{"Action": "", "StatusCode": JobStatus.IN_PROGRESS.value}]
        assert not self.op.poke(None)

    def test_poke_fail(self, mocked_describe_job):
        mocked_describe_job.side_effect = [{"Action": "", "StatusCode": ""}]
        with pytest.raises(AirflowException, match="Sensor failed"):
            self.op.poke(None)

    def test_fail_poke(self, mocked_describe_job):
        response = {"Action": "some action", "StatusCode": "Failed"}
        message = f"Sensor failed. Job status: {response['Action']}, code status: {response['StatusCode']}"
        mocked_describe_job.return_value = response
        with pytest.raises(AirflowException, match=message):
            self.op.poke(context={})


class TestSensorJobDescription:
    def test_job_status_success(self):
        assert JobStatus.SUCCEEDED.value == SUCCEEDED

    def test_job_status_in_progress(self):
        assert JobStatus.IN_PROGRESS.value == IN_PROGRESS
