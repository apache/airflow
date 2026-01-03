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

import importlib
from datetime import datetime

import pytest

from airflow.cli import cli_parser
from airflow.providers.cncf.kubernetes.cli.definition import (
    KUBERNETES_COMMANDS,
    get_kubernetes_cli_commands,
)

from tests_common.test_utils.cli import skip_cli_test_marker


@skip_cli_test_marker("airflow.providers.cncf.kubernetes.cli.definition", "Kubernetes")
class TestKubernetesCliDefinition:
    @pytest.fixture(autouse=True)
    def setup_parser(self):
        importlib.reload(cli_parser)
        self.arg_parser = cli_parser.get_parser()

    def test_kubernetes_cli_commands_count(self):
        """Test that get_kubernetes_cli_commands returns exactly 1 GroupCommand."""
        commands = get_kubernetes_cli_commands()
        assert len(commands) == 1

    def test_kubernetes_commands_count(self):
        """Test that KUBERNETES_COMMANDS contains all 2 subcommands."""
        assert len(KUBERNETES_COMMANDS) == 2

    @pytest.mark.parametrize(
        "command",
        [
            "cleanup-pods",
            "generate-dag-yaml",
        ],
    )
    def test_kubernetes_subcommands_defined(self, command):
        """Test that all kubernetes subcommands are properly defined."""
        params = ["kubernetes", command, "--help"]
        with pytest.raises(SystemExit) as exc_info:
            self.arg_parser.parse_args(params)
        # --help exits with code 0
        assert exc_info.value.code == 0

    def test_cleanup_pods_command_args(self):
        """Test cleanup-pods command with various arguments."""
        params = [
            "kubernetes",
            "cleanup-pods",
            "--namespace",
            "my-namespace",
            "--min-pending-minutes",
            "60",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.namespace == "my-namespace"
        assert args.min_pending_minutes == 60

    def test_cleanup_pods_command_default_args(self):
        """Test cleanup-pods command with default arguments."""
        params = ["kubernetes", "cleanup-pods"]
        args = self.arg_parser.parse_args(params)
        # Should use default values from configuration
        assert hasattr(args, "namespace")
        assert args.min_pending_minutes == 30

    def test_generate_dag_yaml_command_args(self):
        """Test generate-dag-yaml command with various arguments."""
        params = [
            "kubernetes",
            "generate-dag-yaml",
            "--output-path",
            "/tmp/output",
            "my_dag",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.dag_id == "my_dag"
        assert args.output_path == "/tmp/output"

    def test_generate_dag_yaml_command_with_logical_date(self):
        """Test generate-dag-yaml command with logical-date argument."""
        params = [
            "kubernetes",
            "generate-dag-yaml",
            "--logical-date",
            "2024-01-01T00:00:00+00:00",
            "--output-path",
            "/tmp/output",
            "my_dag",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.dag_id == "my_dag"
        assert args.logical_date == datetime.fromisoformat("2024-01-01T00:00:00+00:00")
        assert args.output_path == "/tmp/output"
