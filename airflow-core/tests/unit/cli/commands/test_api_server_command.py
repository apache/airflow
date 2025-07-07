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

from unit.cli.commands._common_cli_classes import _CommonCLIUvicornTestClass

console = Console(width=400, color_system="standard")


@pytest.mark.db_test
class TestCliApiServer(_CommonCLIUvicornTestClass):
    main_process_regexp = r"airflow api-server"

    @pytest.mark.parametrize(
        "args, expected_command",
        [
            pytest.param(
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
                id="dev mode with port and host",
            ),
            pytest.param(
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
                id="dev mode with port, host and proxy headers",
            ),
            pytest.param(
                [
                    "api-server",
                    "--port",
                    "9092",
                    "--host",
                    "somehost",
                    "--dev",
                    "--log-config",
                    "my_log_config.yaml",
                ],
                [
                    "fastapi",
                    "dev",
                    "airflow-core/src/airflow/api_fastapi/main.py",
                    "--port",
                    "9092",
                    "--host",
                    "somehost",
                    "--log-config",
                    "my_log_config.yaml",
                ],
                id="dev mode with port, host and log config",
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

    def test_apps_env_var_set_unset(self):
        """
        Test that AIRFLOW_API_APPS is set and unset in the environment when
        calling the airflow api-server command
        """
        with (
            mock.patch("subprocess.Popen") as Popen,
            mock.patch("os.environ", autospec=True) as mock_environ,
        ):
            apps_value = "core,execution"
            port = "9092"
            host = "somehost"

            # Parse the command line arguments
            args = self.parser.parse_args(
                ["api-server", "--port", port, "--host", host, "--apps", apps_value, "--dev"]
            )

            # Ensure AIRFLOW_API_APPS is not set initially
            mock_environ.get.return_value = None

            # Call the fastapi_api command
            api_server_command.api_server(args)

            # Assert that AIRFLOW_API_APPS was set in the environment before subprocess
            mock_environ.__setitem__.assert_called_with("AIRFLOW_API_APPS", apps_value)

            # Simulate subprocess execution
            Popen.assert_called_with(
                [
                    "fastapi",
                    "dev",
                    "airflow-core/src/airflow/api_fastapi/main.py",
                    "--port",
                    port,
                    "--host",
                    host,
                ],
                close_fds=True,
            )

            # Assert that AIRFLOW_API_APPS was unset after subprocess
            mock_environ.pop.assert_called_with("AIRFLOW_API_APPS")

    @pytest.mark.parametrize(
        "cli_args, expected_additional_kwargs",
        [
            pytest.param(
                [
                    "api-server",
                    "--pid",
                    "/tmp/x.pid",
                    "--ssl-cert",
                    "ssl_cert_path_placeholder",
                    "--ssl-key",
                    "ssl_key_path_placeholder",
                    "--apps",
                    "core",
                ],
                {
                    "ssl_keyfile": "ssl_key_path_placeholder",
                    "ssl_certfile": "ssl_cert_path_placeholder",
                },
                id="api-server with SSL cert and key",
            ),
            pytest.param(
                [
                    "api-server",
                    "--log-config",
                    "my_log_config.yaml",
                ],
                {
                    "ssl_keyfile": None,
                    "ssl_certfile": None,
                    "log_config": "my_log_config.yaml",
                },
                id="api-server with log config",
            ),
        ],
    )
    def test_args_to_uvicorn(self, ssl_cert_and_key, cli_args, expected_additional_kwargs):
        cert_path, key_path = ssl_cert_and_key
        if "ssl_cert_path_placeholder" in cli_args:
            cli_args[cli_args.index("ssl_cert_path_placeholder")] = str(cert_path)
            expected_additional_kwargs["ssl_certfile"] = str(cert_path)
        if "ssl_key_path_placeholder" in cli_args:
            cli_args[cli_args.index("ssl_key_path_placeholder")] = str(key_path)
            expected_additional_kwargs["ssl_keyfile"] = str(key_path)

        with (
            mock.patch("uvicorn.run") as mock_run,
        ):
            args = self.parser.parse_args(cli_args)
            api_server_command.api_server(args)

            mock_run.assert_called_with(
                "airflow.api_fastapi.main:app",
                **{
                    "host": args.host,
                    "port": args.port,
                    "workers": args.workers,
                    "timeout_keep_alive": args.worker_timeout,
                    "timeout_graceful_shutdown": args.worker_timeout,
                    "access_log": True,
                    "proxy_headers": args.proxy_headers,
                    **expected_additional_kwargs,
                },
            )

    @pytest.mark.parametrize(
        "demonize",
        [True, False],
    )
    @mock.patch("airflow.cli.commands.daemon_utils.TimeoutPIDLockFile")
    @mock.patch("airflow.cli.commands.daemon_utils.setup_locations")
    @mock.patch("airflow.cli.commands.daemon_utils.daemon")
    @mock.patch("airflow.cli.commands.daemon_utils.check_if_pidfile_process_is_running")
    @mock.patch("airflow.cli.commands.api_server_command.uvicorn")
    def test_run_command_daemon(
        self, mock_uvicorn, _, mock_daemon, mock_setup_locations, mock_pid_file, demonize
    ):
        mock_setup_locations.return_value = (
            mock.MagicMock(name="pidfile"),
            mock.MagicMock(name="stdout"),
            mock.MagicMock(name="stderr"),
            mock.MagicMock(name="INVALID"),
        )
        args = self.parser.parse_args(
            [
                "api-server",
                "--host",
                "my-hostname",
                "--port",
                "9090",
                "--workers",
                "2",
                "--worker-timeout",
                "60",
            ]
            + (["--daemon"] if demonize else [])
        )
        mock_open = mock.mock_open()
        with mock.patch("airflow.cli.commands.daemon_utils.open", mock_open):
            api_server_command.api_server(args)

        mock_uvicorn.run.assert_called_once_with(
            "airflow.api_fastapi.main:app",
            host="my-hostname",
            port=9090,
            workers=2,
            timeout_keep_alive=60,
            timeout_graceful_shutdown=60,
            ssl_keyfile=None,
            ssl_certfile=None,
            access_log=True,
            proxy_headers=False,
        )

        if demonize:
            assert mock_daemon.mock_calls[:3] == [
                mock.call.DaemonContext(
                    pidfile=mock_pid_file.return_value,
                    files_preserve=None,
                    stdout=mock_open.return_value,
                    stderr=mock_open.return_value,
                    umask=0o077,
                ),
                mock.call.DaemonContext().__enter__(),
                mock.call.DaemonContext().__exit__(None, None, None),
            ]
            assert mock_setup_locations.mock_calls == [
                mock.call(
                    process="api_server",
                    pid=None,
                    stdout=None,
                    stderr=None,
                    log=None,
                )
            ]
            mock_pid_file.assert_has_calls([mock.call(mock_setup_locations.return_value[0], -1)])
            assert mock_open.mock_calls == [
                mock.call(mock_setup_locations.return_value[1], "a"),
                mock.call().__enter__(),
                mock.call(mock_setup_locations.return_value[2], "a"),
                mock.call().__enter__(),
                mock.call().truncate(0),
                mock.call().truncate(0),
                mock.call().__exit__(None, None, None),
                mock.call().__exit__(None, None, None),
            ]
        else:
            assert mock_daemon.mock_calls == []
            mock_setup_locations.mock_calls == []
            mock_pid_file.assert_not_called()
            mock_open.assert_not_called()

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
