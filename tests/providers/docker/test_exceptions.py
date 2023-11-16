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

import pytest

from airflow.providers.docker.exceptions import (
    DockerContainerFailedException,
    DockerContainerFailedSkipException,
)

FAILED_MESSAGE = "Error 1"
FAILED_LOGS = ["Log 1", "Log2"]
FAILED_SKIP_MESSAGE = "Error 2"
FAILED_SKIP_LOGS = ["Log 3", "Log4"]


class TestDockerContainerExceptions:
    @pytest.mark.parametrize(
        "exception_class, message, logs",
        [
            (DockerContainerFailedException, FAILED_MESSAGE, FAILED_LOGS),
            (DockerContainerFailedSkipException, FAILED_SKIP_MESSAGE, FAILED_SKIP_LOGS),
        ],
    )
    def test_docker_failed_exception(self, exception_class, message, logs):
        with pytest.raises(exception_class) as exc_info:
            raise exception_class(message, logs)
        assert exc_info.value.args[0] == message
        assert exc_info.value.logs == logs
