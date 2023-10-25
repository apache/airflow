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

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.amazon.aws.hooks.athena import AthenaHook
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor


@pytest.fixture
def mock_poll_query_status():
    with mock.patch.object(AthenaHook, "poll_query_status") as m:
        yield m


class TestAthenaSensor:
    def setup_method(self):
        self.default_op_kwargs = dict(
            task_id="test_athena_sensor",
            query_execution_id="abc",
            sleep_time=5,
            max_retries=1,
        )
        self.sensor = AthenaSensor(**self.default_op_kwargs, aws_conn_id=None)

    def test_base_aws_op_attributes(self):
        op = AthenaSensor(**self.default_op_kwargs)
        assert op.hook.aws_conn_id == "aws_default"
        assert op.hook._region_name is None
        assert op.hook._verify is None
        assert op.hook._config is None
        assert op.hook.log_query is True

        op = AthenaSensor(
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

    @pytest.mark.parametrize("state", ["SUCCEEDED"])
    def test_poke_success_states(self, state, mock_poll_query_status):
        mock_poll_query_status.side_effect = [state]
        assert self.sensor.poke({}) is True

    @pytest.mark.parametrize("state", ["RUNNING", "QUEUED"])
    def test_poke_intermediate_states(self, state, mock_poll_query_status):
        mock_poll_query_status.side_effect = [state]
        assert self.sensor.poke({}) is False

    @pytest.mark.parametrize(
        "soft_fail, expected_exception",
        [
            pytest.param(False, AirflowException, id="not-soft-fail"),
            pytest.param(True, AirflowSkipException, id="soft-fail"),
        ],
    )
    @pytest.mark.parametrize("state", ["FAILED", "CANCELLED"])
    def test_poke_failure_states(self, state, soft_fail, expected_exception, mock_poll_query_status):
        mock_poll_query_status.side_effect = [state]
        sensor = AthenaSensor(**self.default_op_kwargs, aws_conn_id=None, soft_fail=soft_fail)
        message = "Athena sensor failed"
        with pytest.raises(expected_exception, match=message):
            sensor.poke({})
