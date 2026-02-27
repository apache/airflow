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
"""Unit tests for CLI api-server command argument parsing.

These tests verify that the argparse configuration for the ``api-server``
subcommand correctly parses --port, --workers, --apps, --host,
--worker-timeout, --proxy-headers, and --dev flags, and that invalid
values are rejected by the parser.
"""
from __future__ import annotations

import pytest

from airflow.cli import cli_parser


@pytest.fixture
def parser():
    return cli_parser.get_parser()


class TestApiServerArgParsing:
    """Tests that the api-server CLI flags are parsed into the expected namespace values."""

    def test_port_flag_sets_integer(self, parser):
        args = parser.parse_args(["api-server", "--port", "8080"])
        assert args.port == 8080

    def test_port_short_flag(self, parser):
        args = parser.parse_args(["api-server", "-p", "9000"])
        assert args.port == 9000

    def test_workers_flag_sets_integer(self, parser):
        args = parser.parse_args(["api-server", "--workers", "4"])
        assert args.workers == 4

    def test_workers_short_flag(self, parser):
        args = parser.parse_args(["api-server", "-w", "2"])
        assert args.workers == 2

    def test_apps_flag_all(self, parser):
        args = parser.parse_args(["api-server", "--apps", "all"])
        assert args.apps == "all"

    def test_apps_flag_core(self, parser):
        args = parser.parse_args(["api-server", "--apps", "core"])
        assert args.apps == "core"

    def test_apps_flag_execution(self, parser):
        args = parser.parse_args(["api-server", "--apps", "execution"])
        assert args.apps == "execution"

    def test_apps_flag_comma_separated(self, parser):
        args = parser.parse_args(["api-server", "--apps", "core,execution"])
        assert args.apps == "core,execution"

    def test_apps_default_is_all(self, parser):
        args = parser.parse_args(["api-server"])
        assert args.apps == "all"

    def test_host_flag(self, parser):
        args = parser.parse_args(["api-server", "--host", "0.0.0.0"])
        assert args.host == "0.0.0.0"

    def test_host_short_flag(self, parser):
        args = parser.parse_args(["api-server", "-H", "127.0.0.1"])
        assert args.host == "127.0.0.1"

    def test_worker_timeout_flag(self, parser):
        args = parser.parse_args(["api-server", "--worker-timeout", "300"])
        assert args.worker_timeout == 300

    def test_worker_timeout_default(self, parser):
        args = parser.parse_args(["api-server"])
        assert args.worker_timeout == 120

    def test_proxy_headers_flag(self, parser):
        args = parser.parse_args(["api-server", "--proxy-headers"])
        assert args.proxy_headers is True

    def test_proxy_headers_default_false(self, parser):
        args = parser.parse_args(["api-server"])
        assert args.proxy_headers is False

    def test_dev_flag(self, parser):
        args = parser.parse_args(["api-server", "--dev"])
        assert args.dev is True

    def test_dev_short_flag(self, parser):
        args = parser.parse_args(["api-server", "-d"])
        assert args.dev is True

    def test_dev_default_false(self, parser):
        args = parser.parse_args(["api-server"])
        assert args.dev is False

    def test_combined_flags(self, parser):
        args = parser.parse_args([
            "api-server",
            "--port", "9090",
            "--workers", "8",
            "--apps", "core",
            "--host", "10.0.0.1",
            "--worker-timeout", "60",
            "--proxy-headers",
            "--dev",
        ])
        assert args.port == 9090
        assert args.workers == 8
        assert args.apps == "core"
        assert args.host == "10.0.0.1"
        assert args.worker_timeout == 60
        assert args.proxy_headers is True
        assert args.dev is True

    def test_invalid_port_non_integer(self, parser):
        with pytest.raises(SystemExit):
            parser.parse_args(["api-server", "--port", "not_a_number"])

    def test_invalid_workers_non_integer(self, parser):
        with pytest.raises(SystemExit):
            parser.parse_args(["api-server", "--workers", "abc"])

    def test_invalid_worker_timeout_non_integer(self, parser):
        with pytest.raises(SystemExit):
            parser.parse_args(["api-server", "--worker-timeout", "xyz"])

    def test_ssl_cert_and_key_flags(self, parser):
        args = parser.parse_args([
            "api-server",
            "--ssl-cert", "/path/to/cert.pem",
            "--ssl-key", "/path/to/key.pem",
        ])
        assert args.ssl_cert == "/path/to/cert.pem"
        assert args.ssl_key == "/path/to/key.pem"

    def test_log_config_flag(self, parser):
        args = parser.parse_args(["api-server", "--log-config", "my_log.yaml"])
        assert args.log_config == "my_log.yaml"

    @pytest.mark.parametrize(
        "flags, expected_port, expected_workers, expected_apps",
        [
            (["--port", "8080", "--workers", "2", "--apps", "core"], 8080, 2, "core"),
            (["--port", "443", "--workers", "1", "--apps", "execution"], 443, 1, "execution"),
            (["--port", "5000", "--workers", "16", "--apps", "all"], 5000, 16, "all"),
        ],
        ids=["core-app-small", "execution-single-worker", "all-apps-many-workers"],
    )
    def test_parametrized_flag_combinations(self, parser, flags, expected_port, expected_workers, expected_apps):
        args = parser.parse_args(["api-server"] + flags)
        assert args.port == expected_port
        assert args.workers == expected_workers
        assert args.apps == expected_apps
