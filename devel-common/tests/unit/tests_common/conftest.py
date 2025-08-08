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

import pytest

from tests_common.test_utils.stream_capture_manager import (
    CombinedCaptureManager,
    StderrCaptureManager,
    StdoutCaptureManager,
    StreamCaptureManager,
)


@pytest.fixture
def stdout_capture():
    """Fixture that captures stdout only."""
    return StdoutCaptureManager()


@pytest.fixture
def stderr_capture():
    """Fixture that captures stderr only."""
    return StderrCaptureManager()


@pytest.fixture
def stream_capture():
    """Fixture that returns a configurable stream capture manager."""

    def _capture(stdout=True, stderr=False):
        return StreamCaptureManager(capture_stdout=stdout, capture_stderr=stderr)

    return _capture


@pytest.fixture
def combined_capture():
    """Fixture that captures both stdout and stderr."""
    return CombinedCaptureManager()
