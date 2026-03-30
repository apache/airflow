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

from unittest.mock import MagicMock

from airflow.providers.keycloak.auth_manager.cli.utils import dry_run_message_wrap, dry_run_preview


class TestDryRunMessageWrap:
    def test_prints_messages_when_dry_run_true(self, capsys):
        @dry_run_message_wrap
        def test_func(args):
            return "executed"

        args = MagicMock()
        args.dry_run = True
        result = test_func(args)

        captured = capsys.readouterr()
        assert "Performing dry run" in captured.out
        assert "Dry run completed" in captured.out
        assert result == "executed"


class TestDryRunPreview:
    def test_calls_preview_when_dry_run_true(self):
        preview_called = []

        def preview_func(*args, **kwargs):
            preview_called.append(True)

        @dry_run_preview(preview_func)
        def actual_func(*args, **kwargs):
            return "actual"

        result = actual_func(_dry_run=True)
        assert result is None
        assert len(preview_called) == 1

    def test_calls_actual_when_dry_run_false(self):
        @dry_run_preview(lambda *a, **k: None)
        def actual_func(*args, **kwargs):
            return "actual"

        result = actual_func(_dry_run=False)
        assert result == "actual"
