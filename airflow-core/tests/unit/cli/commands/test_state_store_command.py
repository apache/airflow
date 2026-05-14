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

from argparse import Namespace
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.cli.commands.state_store_command import cleanup_task_states
from airflow.state.metastore import MetastoreStateBackend

pytestmark = pytest.mark.db_test


class TestStateStoreCleanupCommand:
    def test_cleanup_calls_backend(self):
        args = Namespace(dry_run=False, verbose=False)
        backend = MetastoreStateBackend()
        with (
            mock.patch("airflow.cli.commands.state_store_command.get_state_backend", return_value=backend),
            patch.object(backend, "cleanup"),
        ):
            cleanup_task_states(args)

            backend.cleanup.assert_called_once_with()

    def test_dry_run_does_not_call_backend(self, capsys):
        args = Namespace(dry_run=True, verbose=False)
        backend = MetastoreStateBackend()
        with (
            mock.patch("airflow.cli.commands.state_store_command.get_state_backend", return_value=backend),
            patch.object(backend, "_summary_dry_run", return_value={"expired": []}),
        ):
            cleanup_task_states(args)

            captured = capsys.readouterr()
            assert "Nothing to delete" in captured.out

    def test_custom_backend_is_skipped(self, capsys):
        args = Namespace(dry_run=False, verbose=False)
        custom_backend = MagicMock(spec=[])
        with mock.patch(
            "airflow.cli.commands.state_store_command.get_state_backend", return_value=custom_backend
        ):
            cleanup_task_states(args)

            captured = capsys.readouterr()
            assert "Custom backend configured" in captured.out
            assert not hasattr(custom_backend, "cleanup") or not custom_backend.cleanup.called
