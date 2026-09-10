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

from unittest.mock import AsyncMock, patch

import pytest
from airflow_google_provider_resource_cleanup.handlers import logging


@pytest.mark.anyio
async def test_delete_log_sink_returns_false_for_protected_sink():
    result = await logging._delete_log_sink(
        {
            "name": "//logging.googleapis.com/projects/test-project/sinks/_Default",
            "displayName": "_Default",
        },
        "[1/1] ",
    )

    assert result is False


@pytest.mark.anyio
async def test_delete_log_sink_handles_missing_display_name():
    with patch.object(logging, "run_command_async", AsyncMock()) as mock_run_command:
        result = await logging._delete_log_sink(
            {"name": "//logging.googleapis.com/projects/test-project/sinks/test-sink"},
            "[1/1] ",
        )

    assert result is True
    mock_run_command.assert_awaited_once_with(
        "gcloud logging sinks delete projects/test-project/sinks/test-sink --quiet", "[1/1] "
    )
