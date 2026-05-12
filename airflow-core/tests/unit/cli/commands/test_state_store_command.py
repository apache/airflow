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

from airflow.cli.commands.state_store_command import cleanup
from airflow.state.metastore import MetastoreStateBackend

pytestmark = pytest.mark.db_test


class TestStateStoreCleanupCommand:
    def test_cleanup_calls_backend(self):
        args = Namespace(dry_run=False, verbose=False)
        with mock.patch("airflow.state.get_state_backend") as mock_get_backend:
            mock_backend = MagicMock()
            mock_get_backend.return_value = mock_backend

            cleanup(args)

            mock_backend.cleanup.assert_called_once_with()

    def test_dry_run_does_not_call_backend(self, capsys):
        args = Namespace(dry_run=True, verbose=False)
        backend = MetastoreStateBackend()
        with (
            mock.patch("airflow.state.get_state_backend", return_value=backend),
            patch.object(backend, "_summary_dry_run_", return_value={"expired": []}),
        ):
            cleanup(args)

            captured = capsys.readouterr()
            assert "Nothing to delete" in captured.out
