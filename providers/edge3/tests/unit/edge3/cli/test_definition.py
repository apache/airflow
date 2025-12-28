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

import pytest

from airflow.cli import cli_parser
from airflow.providers.edge3.cli.definition import EDGE_COMMANDS, get_edge_cli_commands

from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS


@pytest.mark.skipif(
    not AIRFLOW_V_3_2_PLUS, reason="The ProviderManager-based CLI is available in Airflow 3.2+"
)
class TestEdgeCliDefinition:
    @pytest.fixture(autouse=True)
    def setup_parser(self):
        importlib.reload(cli_parser)
        self.arg_parser = cli_parser.get_parser()

    def test_edge_cli_commands_count(self):
        """Test that get_edge_cli_commands returns exactly 1 GroupCommand."""
        commands = get_edge_cli_commands()
        assert len(commands) == 1

    def test_edge_commands_count(self):
        """Test that EDGE_COMMANDS contains all 13 subcommands."""
        assert len(EDGE_COMMANDS) == 13

    @pytest.mark.parametrize(
        "command",
        [
            "worker",
            "status",
            "maintenance",
            "stop",
            "list-workers",
            "remote-edge-worker-request-maintenance",
            "remote-edge-worker-exit-maintenance",
            "remote-edge-worker-update-maintenance-comment",
            "remove-remote-edge-worker",
            "shutdown-remote-edge-worker",
            "add-worker-queues",
            "remove-worker-queues",
            "shutdown-all-workers",
        ],
    )
    def test_edge_subcommands_defined(self, command):
        """Test that all edge subcommands are properly defined."""
        params = ["edge", command, "--help"]
        with pytest.raises(SystemExit) as exc_info:
            self.arg_parser.parse_args(params)
        # --help exits with code 0
        assert exc_info.value.code == 0

    def test_worker_command_args(self):
        """Test worker command with various arguments."""
        params = [
            "edge",
            "worker",
            "--queues",
            "queue1,queue2",
            "--concurrency",
            "4",
            "--edge-hostname",
            "edge-worker-1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.queues == "queue1,queue2"
        assert args.concurrency == 4
        assert args.edge_hostname == "edge-worker-1"

    def test_status_command_args(self):
        """Test status command with pid argument."""
        params = ["edge", "status", "--pid", "/path/to/pid"]
        args = self.arg_parser.parse_args(params)
        assert args.pid == "/path/to/pid"

    def test_maintenance_command_args_on(self):
        """Test maintenance command to enable maintenance mode."""
        params = [
            "edge",
            "maintenance",
            "on",
            "--comments",
            "Scheduled maintenance",
            "--wait",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.maintenance == "on"
        assert args.comments == "Scheduled maintenance"
        assert args.wait is True

    def test_maintenance_command_args_off(self):
        """Test maintenance command to disable maintenance mode."""
        params = ["edge", "maintenance", "off"]
        args = self.arg_parser.parse_args(params)
        assert args.maintenance == "off"

    def test_stop_command_args(self):
        """Test stop command with wait argument."""
        params = ["edge", "stop", "--wait", "--pid", "/path/to/pid"]
        args = self.arg_parser.parse_args(params)
        assert args.wait is True
        assert args.pid == "/path/to/pid"

    def test_list_workers_command_args(self):
        """Test list-workers command with output format and state filter."""
        params = ["edge", "list-workers", "--output", "json", "--state", "running", "maintenance"]
        args = self.arg_parser.parse_args(params)
        assert args.output == "json"
        assert args.state == ["running", "maintenance"]

    def test_remote_edge_worker_request_maintenance_args(self):
        """Test remote-edge-worker-request-maintenance command with required arguments."""
        params = [
            "edge",
            "remote-edge-worker-request-maintenance",
            "--edge-hostname",
            "remote-worker-1",
            "--comments",
            "Emergency maintenance",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"
        assert args.comments == "Emergency maintenance"

    def test_remote_edge_worker_exit_maintenance_args(self):
        """Test remote-edge-worker-exit-maintenance command with required hostname."""
        params = [
            "edge",
            "remote-edge-worker-exit-maintenance",
            "--edge-hostname",
            "remote-worker-1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"

    def test_remote_edge_worker_update_maintenance_comment_args(self):
        """Test remote-edge-worker-update-maintenance-comment command with required arguments."""
        params = [
            "edge",
            "remote-edge-worker-update-maintenance-comment",
            "--edge-hostname",
            "remote-worker-1",
            "--comments",
            "Updated maintenance reason",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"
        assert args.comments == "Updated maintenance reason"

    def test_remove_remote_edge_worker_args(self):
        """Test remove-remote-edge-worker command with required hostname."""
        params = [
            "edge",
            "remove-remote-edge-worker",
            "--edge-hostname",
            "remote-worker-1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"

    def test_shutdown_remote_edge_worker_args(self):
        """Test shutdown-remote-edge-worker command with required hostname."""
        params = [
            "edge",
            "shutdown-remote-edge-worker",
            "--edge-hostname",
            "remote-worker-1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"

    def test_add_worker_queues_args(self):
        """Test add-worker-queues command with required arguments."""
        params = [
            "edge",
            "add-worker-queues",
            "--edge-hostname",
            "remote-worker-1",
            "--queues",
            "queue3,queue4",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"
        assert args.queues == "queue3,queue4"

    def test_remove_worker_queues_args(self):
        """Test remove-worker-queues command with required arguments."""
        params = [
            "edge",
            "remove-worker-queues",
            "--edge-hostname",
            "remote-worker-1",
            "--queues",
            "queue1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.edge_hostname == "remote-worker-1"
        assert args.queues == "queue1"

    def test_shutdown_all_workers_args(self):
        """Test shutdown-all-workers command with yes flag."""
        params = ["edge", "shutdown-all-workers", "--yes"]
        args = self.arg_parser.parse_args(params)
        assert args.yes is True
