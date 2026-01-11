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
from airflow.providers.celery.cli.definition import CELERY_CLI_COMMANDS, CELERY_COMMANDS

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS


class TestCeleryCliDefinition:
    @pytest.fixture(autouse=True)
    def setup_parser(self):
        if AIRFLOW_V_3_2_PLUS:
            importlib.reload(cli_parser)
            cli_parser.get_parser.cache_clear()
            self.arg_parser = cli_parser.get_parser()
        else:
            with conf_vars(
                {
                    (
                        "core",
                        "executor",
                    ): "CeleryExecutor",
                }
            ):
                importlib.reload(cli_parser)
                cli_parser.get_parser.cache_clear()
                self.arg_parser = cli_parser.get_parser()

    def test_celery_cli_commands_count(self):
        """Test that CELERY_CLI_COMMANDS contains exactly 1 GroupCommand."""
        assert len(CELERY_CLI_COMMANDS) == 1

    def test_celery_commands_count(self):
        """Test that CELERY_COMMANDS contains all 9 subcommands."""
        assert len(CELERY_COMMANDS) == 9

    @pytest.mark.parametrize(
        "command",
        [
            "worker",
            "flower",
            "stop",
            "list-workers",
            "shutdown-worker",
            "shutdown-all-workers",
            "add-queue",
            "remove-queue",
            "remove-all-queues",
        ],
    )
    def test_celery_subcommands_defined(self, command):
        """Test that all celery subcommands are properly defined."""
        params = ["celery", command, "--help"]
        with pytest.raises(SystemExit) as exc_info:
            self.arg_parser.parse_args(params)
        # --help exits with code 0
        assert exc_info.value.code == 0

    def test_worker_command_args(self):
        """Test worker command with various arguments."""
        params = [
            "celery",
            "worker",
            "--queues",
            "queue1,queue2",
            "--concurrency",
            "4",
            "--celery-hostname",
            "worker1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.queues == "queue1,queue2"
        assert args.concurrency == 4
        assert args.celery_hostname == "worker1"

    def test_flower_command_args(self):
        """Test flower command with various arguments."""
        params = [
            "celery",
            "flower",
            "--hostname",
            "localhost",
            "--port",
            "5555",
            "--url-prefix",
            "/flower",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.hostname == "localhost"
        assert args.port == 5555
        assert args.url_prefix == "/flower"

    def test_list_workers_command_args(self):
        """Test list-workers command with output format."""
        params = ["celery", "list-workers", "--output", "json"]
        args = self.arg_parser.parse_args(params)
        assert args.output == "json"

    def test_shutdown_worker_command_args(self):
        """Test shutdown-worker command with celery hostname."""
        params = ["celery", "shutdown-worker", "--celery-hostname", "celery@worker1"]
        args = self.arg_parser.parse_args(params)
        assert args.celery_hostname == "celery@worker1"

    def test_shutdown_all_workers_command_args(self):
        """Test shutdown-all-workers command with yes flag."""
        params = ["celery", "shutdown-all-workers", "--yes"]
        args = self.arg_parser.parse_args(params)
        assert args.yes is True

    def test_add_queue_command_args(self):
        """Test add-queue command with required arguments."""
        params = [
            "celery",
            "add-queue",
            "--queues",
            "new_queue",
            "--celery-hostname",
            "celery@worker1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.queues == "new_queue"
        assert args.celery_hostname == "celery@worker1"

    def test_remove_queue_command_args(self):
        """Test remove-queue command with required arguments."""
        params = [
            "celery",
            "remove-queue",
            "--queues",
            "old_queue",
            "--celery-hostname",
            "celery@worker1",
        ]
        args = self.arg_parser.parse_args(params)
        assert args.queues == "old_queue"
        assert args.celery_hostname == "celery@worker1"

    def test_remove_all_queues_command_args(self):
        """Test remove-all-queues command with celery hostname."""
        params = ["celery", "remove-all-queues", "--celery-hostname", "celery@worker1"]
        args = self.arg_parser.parse_args(params)
        assert args.celery_hostname == "celery@worker1"

    def test_stop_command_args(self):
        """Test stop command with pid argument."""
        params = ["celery", "stop", "--pid", "/path/to/pid"]
        args = self.arg_parser.parse_args(params)
        assert args.pid == "/path/to/pid"
