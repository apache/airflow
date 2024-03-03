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
"""Test for Exceptions used by Docker provider."""

from __future__ import annotations

from unittest import mock

import pytest
from docker import APIClient

from airflow.providers.docker.exceptions import (
    DockerContainerFailedException,
    DockerContainerFailedSkipException,
)
from airflow.providers.docker.operators.docker import DockerOperator

FAILED_MESSAGE = {"StatusCode": 1}
FAILED_LOGS = ["unicode container log 😁   ", b"byte string container log"]
EXPECTED_MESSAGE = f"Docker container failed: {FAILED_MESSAGE}"
FAILED_SKIP_MESSAGE = {"StatusCode": 2}
SKIP_ON_EXIT_CODE = 2
EXPECTED_SKIP_MESSAGE = f"Docker container returned exit code {[SKIP_ON_EXIT_CODE]}. Skipping."


@pytest.mark.parametrize(
    "failed_msg, log_line, expected_message, skip_on_exit_code",
    [
        (FAILED_MESSAGE, FAILED_LOGS, EXPECTED_MESSAGE, None),
        (FAILED_SKIP_MESSAGE, FAILED_LOGS, EXPECTED_SKIP_MESSAGE, SKIP_ON_EXIT_CODE),
    ],
)
class TestDockerContainerExceptions:
    @pytest.fixture(autouse=True)
    def setup_patchers(self, docker_api_client_patcher):
        self.client_mock = mock.MagicMock(spec=APIClient)
        self.client_mock.wait.return_value = {"StatusCode": 0}
        self.log_messages = ["container log  😁   ", b"byte string container log"]
        self.client_mock.attach.return_value = self.log_messages

        self.client_mock.logs.side_effect = (
            lambda **kwargs: iter(self.log_messages[-kwargs["tail"] :])
            if "tail" in kwargs
            else iter(self.log_messages)
        )

        docker_api_client_patcher.return_value = self.client_mock

    def test_docker_failed_exception(self, failed_msg, log_line, expected_message, skip_on_exit_code):
        self.client_mock.attach.return_value = log_line
        self.client_mock.wait.return_value = failed_msg

        operator = DockerOperator(
            image="ubuntu", owner="unittest", task_id="unittest", skip_on_exit_code=skip_on_exit_code
        )

        if skip_on_exit_code:
            with pytest.raises(DockerContainerFailedSkipException) as raised_exception:
                operator.execute(None)
        else:
            with pytest.raises(DockerContainerFailedException) as raised_exception:
                operator.execute(None)

        assert str(raised_exception.value) == expected_message
        assert raised_exception.value.logs == [log_line[0].strip(), log_line[1].decode("utf-8")]
