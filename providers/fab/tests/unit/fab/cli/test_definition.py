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

import shlex

import pytest

from airflow.providers.fab.cli.definition import (
    PERMISSIONS_CLEANUP_COMMAND,
    ROLES_COMMANDS,
    SYNC_PERM_COMMAND,
    USERS_COMMANDS,
    get_parser,
)


def _extract_epilog_examples(command):
    examples = []
    for line in (command.epilog or "").splitlines():
        stripped = line.strip()
        if stripped.startswith("$ airflow "):
            examples.append(stripped.removeprefix("$ "))
    if not examples:
        # An empty parametrize set is silently skipped, which would hide the loss of this guard.
        raise ValueError(f"No '$ airflow ...' example found in the {command.name} epilog")
    return examples


class TestCliDefinition:
    def test_users_commands(self):
        assert len(USERS_COMMANDS) == 8

    def test_roles_commands(self):
        assert len(ROLES_COMMANDS) == 7

    def test_sync_perm_command(self):
        assert SYNC_PERM_COMMAND.name == "sync-perm"

    @pytest.mark.parametrize(
        "example", _extract_epilog_examples(PERMISSIONS_CLEANUP_COMMAND), ids=lambda e: e
    )
    def test_permissions_cleanup_epilog_examples_are_runnable(self, example):
        args = get_parser().parse_args(shlex.split(example)[1:])
        assert args.subcommand == PERMISSIONS_CLEANUP_COMMAND.name
