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

import os
import subprocess
import sys
from unittest import mock

from airflow import __main__ as airflow_main


@mock.patch("airflow.__main__.redirect_stdout_log_handlers_to_stderr", autospec=True)
@mock.patch("airflow.settings.ensure_initialized", autospec=True)
@mock.patch("airflow.__main__.argcomplete.autocomplete", autospec=True)
@mock.patch("airflow.__main__.cli_parser.get_parser", autospec=True)
def test_main_initializes_between_parsing_and_running_the_command(
    mock_get_parser, mock_autocomplete, mock_ensure_initialized, mock_redirect
):
    calls = mock.Mock()
    calls.attach_mock(mock_get_parser, "get_parser")
    calls.attach_mock(mock_ensure_initialized, "ensure_initialized")
    calls.attach_mock(mock_redirect, "redirect")
    args = mock_get_parser.return_value.parse_args.return_value
    args.subcommand = "version"
    calls.attach_mock(args.func, "func")

    airflow_main.main()

    assert calls.mock_calls == [
        mock.call.get_parser(),
        mock.call.get_parser().parse_args(),
        mock.call.ensure_initialized(),
        mock.call.redirect(),
        mock.call.func(args),
    ]


def test_command_runs_when_importing_airflow_does_not_initialize():
    result = subprocess.run(
        [sys.executable, "-m", "airflow", "plugins"],
        env={**os.environ, "_AIRFLOW__AS_LIBRARY": "1"},
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )

    assert result.returncode == 0, result.stderr
