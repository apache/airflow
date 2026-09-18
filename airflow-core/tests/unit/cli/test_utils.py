#
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

import logging
import sys
import warnings

import pytest

from airflow.cli.utils import deprecated_for_airflowctl, redirect_stdout_log_handlers_to_stderr


class TestDeprecatedForAirflowctl:
    def test_records_replacement_without_emitting_a_user_warning(self):
        @deprecated_for_airflowctl("airflowctl dags trigger")
        def command(args):
            return "result"

        # Calling the command emits nothing to users (any warning would become an error here).
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            result = command(args=None)

        assert result == "result"
        # The replacement is recorded for maintainers, not shown to users.
        assert command._migrated_to_airflowctl == "airflowctl dags trigger"

    def test_passes_through_args_and_leaves_function_untouched(self):
        @deprecated_for_airflowctl("airflowctl pools create")
        def command(a, b, *, c):
            """Original docstring."""
            return (a, b, c)

        assert command(1, 2, c=3) == (1, 2, 3)
        # The decorator returns the original function untouched apart from the metadata it records.
        assert command.__name__ == "command"
        assert command.__doc__ == "Original docstring."
        assert command._migrated_to_airflowctl == "airflowctl pools create"


class TestRedirectStdoutLogHandlersToStderr:
    """Tests for the CLI helper that keeps logs off stdout for ``-o``-style commands."""

    @pytest.fixture
    def isolated_root_logger(self):
        """Snapshot and restore root logger handlers so tests don't leak state."""
        root = logging.getLogger()
        original_handlers = root.handlers[:]
        root.handlers = []
        try:
            yield root
        finally:
            root.handlers = original_handlers

    @pytest.mark.parametrize(
        ("make_handler", "expected_stream"),
        [
            pytest.param(
                lambda _tmp_path: logging.StreamHandler(stream=sys.stdout),
                lambda _original: sys.stderr,
                id="stdout-stream-handler-redirected",
            ),
            pytest.param(
                lambda _tmp_path: logging.StreamHandler(stream=sys.stderr),
                lambda _original: sys.stderr,
                id="stderr-stream-handler-untouched",
            ),
            pytest.param(
                lambda tmp_path: logging.FileHandler(tmp_path / "airflow.log"),
                lambda original: original,
                id="file-handler-untouched",
            ),
        ],
    )
    def test_redirect(self, isolated_root_logger, tmp_path, make_handler, expected_stream):
        handler = make_handler(tmp_path)
        isolated_root_logger.addHandler(handler)
        original_stream = handler.stream
        try:
            redirect_stdout_log_handlers_to_stderr()
            assert handler.stream is expected_stream(original_stream)
        finally:
            handler.close()
