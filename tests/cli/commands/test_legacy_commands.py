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

import unittest
from argparse import ArgumentError
from unittest import mock
from unittest.mock import MagicMock

from airflow.cli.commands.legacy_commands import COMMAND_MAP, check_value

LEGACY_COMMANDS = ["worker", "flower", "trigger_dag", "delete_dag", "show_dag", "list_dag",
                   "dag_status", "backfill", "list_dag_runs", "pause", "unpause", "test",
                   "clear", "list_tasks", "task_failed_deps", "task_state", "run",
                   "render", "initdb", "resetdb", "upgradedb", "checkdb", "shell", "pool",
                   "list_users", "create_user", "delete_user"]


class TestLegacyCommandCheck(unittest.TestCase):

    def test_command_map(self):
        for item in LEGACY_COMMANDS:
            self.assertIsNotNone(COMMAND_MAP[item])

    @mock.patch("airflow.cli.commands.legacy_commands.COMMAND_MAP")
    def test_check_value(self, command_map):
        action = MagicMock()
        command_map.__getitem__.return_value = "users list"
        with self.assertRaises(ArgumentError) as e:
            check_value(action, 'list_user')
        self.assertEqual(
            str(e.exception),
            "argument : list_user command, has been removed, please use `airflow users list`")
