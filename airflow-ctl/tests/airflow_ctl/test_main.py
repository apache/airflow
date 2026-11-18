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
import structlog

from airflowctl import __main__
from airflowctl.api import client


@pytest.fixture(autouse=True)
def restore_structlog_config():
    config = structlog.get_config()
    yield
    structlog.configure(**config)


@mock.patch("sys.argv", ["airflowctl", "version"])
@mock.patch.object(__main__, "safe_call_command", autospec=True)
def test_main_writes_client_logs_to_stderr(mock_safe_call_command, capsys):
    mock_safe_call_command.side_effect = lambda function, args: client.log.warning("Server error ")

    __main__.main()

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "Server error" in captured.err
