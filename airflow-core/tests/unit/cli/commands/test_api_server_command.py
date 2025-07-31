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
from rich.console import Console

from airflow.cli.commands import api_server_command
from airflow.exceptions import AirflowConfigException

from unit.cli.commands._common_cli_classes import _CommonCLIGunicornTestClass

console = Console(width=400, color_system="standard")


@pytest.mark.db_test
class TestCliApiServer(_CommonCLIGunicornTestClass):
    main_process_regexp = r"airflow api-server"

    @pytest.mark.parametrize(
        "args, expected_command",
        [
            (
                ["api-server", "--port", "9092", "--host", "somehost", "--dev"],
                [
                    "fastapi",
                    "dev",
                    "airflow-core/src/airflow/api_fastapi/main.py",
                    "--port",
                    "9092",
                    "--host",
                    "somehost",
                ],
            ),
            (
                ["api-server", "--port", "9092", "--host", "somehost", "--dev", "--proxy-headers"],
                [
                    "fastapi",
                    "dev",
                    "airflow-core/src/airflow/api_fastapi/main.py",
                    "--port",
                    "9092",
                    "--host",
                    "somehost",
                    "--proxy-headers",
                ],
            ),
        ],
    )
    def test_dev_arg(self, args, expected_command):
        with (
            mock.patch("subprocess.Popen") as Popen,
        ):
            args = self.parser.parse_args(args)
            api_server_command.api_server(args)

            Popen.assert_called_with(
                expected_command,
                close_fds=True,
            )

    @pytest.mark.parametrize(
        "args",
        [
            (["api-server"]),
            (["api-server", "--apps", "all"]),
            (["api-server", "--apps", "core,execution"]),
            (["api-server", "--apps", "core"]),
            (["api-server", "--apps", "execution"]),
        ],
        ids=[
            "default_apps",
            "all_apps_explicit",
            "multiple_apps_explicit",
            "single_app_core",
            "single_app_execution",
        ],
    )
    @pytest.mark.parametrize("dev_mode", [True, False])
    @pytest.mark.parametrize(
        "original_env",
        [None, "some_value"],
    )
    def test_api_apps_env(self, args, dev_mode, original_env):
        """
        Test that AIRFLOW_API_APPS is set and unset in the environment when
        calling the airflow api-server command
        """
        expected_setitem_calls = []

        if dev_mode:
            args.append("--dev")

        with (
            mock.patch("os.environ", autospec=True) as mock_environ,
            mock.patch("uvicorn.run"),
            mock.patch("subprocess.Popen"),
        ):
            # Mock the environment variable with initial value or None
            mock_environ.get.return_value = original_env

            # Parse the command line arguments and call the api_server command
            parsed_args = self.parser.parse_args(args)
            api_server_command.api_server(parsed_args)

            # Verify the AIRFLOW_API_APPS was set correctly
            if "--apps" in args:
                expected_setitem_calls.append(
                    mock.call(api_server_command.AIRFLOW_API_APPS, parsed_args.apps)
                )

            # Verify AIRFLOW_API_APPS was cleaned up
            if original_env is not None:
                expected_setitem_calls.append(mock.call(api_server_command.AIRFLOW_API_APPS, original_env))
            else:
                mock_environ.pop.assert_called_with(api_server_command.AIRFLOW_API_APPS, None)

            # Verify that the environment variable was set and cleaned up correctly
            mock_environ.__setitem__.assert_has_calls(expected_setitem_calls)

    def test_args_to_uvicorn(self, ssl_cert_and_key):
        cert_path, key_path = ssl_cert_and_key

        with (
            mock.patch("uvicorn.run") as mock_run,
        ):
            args = self.parser.parse_args(
                [
                    "api-server",
                    "--pid",
                    "/tmp/x.pid",
                    "--ssl-cert",
                    str(cert_path),
                    "--ssl-key",
                    str(key_path),
                    "--apps",
                    "core",
                ]
            )
            api_server_command.api_server(args)

            mock_run.assert_called_with(
                "airflow.api_fastapi.main:app",
                host="0.0.0.0",
                port=8080,
                workers=4,
                timeout_keep_alive=120,
                timeout_graceful_shutdown=120,
                ssl_keyfile=str(key_path),
                ssl_certfile=str(cert_path),
                access_log="-",
                proxy_headers=False,
            )

    @pytest.mark.parametrize(
        "ssl_arguments, error_pattern",
        [
            (["--ssl-cert", "_.crt", "--ssl-key", "_.key"], "does not exist _.crt"),
            (["--ssl-cert", "_.crt"], "Need both.*certificate.*key"),
            (["--ssl-key", "_.key"], "Need both.*key.*certificate"),
        ],
    )
    def test_get_ssl_cert_and_key_filepaths_with_incorrect_usage(self, ssl_arguments, error_pattern):
        args = self.parser.parse_args(["api-server"] + ssl_arguments)
        with pytest.raises(AirflowConfigException, match=error_pattern):
            api_server_command._get_ssl_cert_and_key_filepaths(args)

    def test_get_ssl_cert_and_key_filepaths_with_correct_usage(self, ssl_cert_and_key):
        cert_path, key_path = ssl_cert_and_key

        args = self.parser.parse_args(
            ["api-server"] + ["--ssl-cert", str(cert_path), "--ssl-key", str(key_path)]
        )
        assert api_server_command._get_ssl_cert_and_key_filepaths(args) == (str(cert_path), str(key_path))

    @pytest.fixture
    def ssl_cert_and_key(self, tmp_path):
        cert_path, key_path = tmp_path / "_.crt", tmp_path / "_.key"
        cert_path.touch()
        key_path.touch()
        return cert_path, key_path
