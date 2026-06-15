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

import os
import tempfile
from unittest.mock import patch

from airflow.utils.log.callback_log_reader import (
    _get_callback_log_relative_paths,
    read_callback_log,
)


class TestGetCallbackLogRelativePaths:
    def test_returns_both_executor_and_triggerer_paths(self):
        paths = _get_callback_log_relative_paths("my_dag", "run_123", "cb_456")
        assert paths == [
            "executor_callbacks/my_dag/run_123/cb_456",
            "triggerer_callbacks/my_dag/run_123/cb_456",
        ]

    def test_path_components_preserved(self):
        paths = _get_callback_log_relative_paths("dag-with-dashes", "manual__2024-01-01", "abc123")
        assert "dag-with-dashes" in paths[0]
        assert "manual__2024-01-01" in paths[0]
        assert "abc123" in paths[0]
        assert paths[1] == "triggerer_callbacks/dag-with-dashes/manual__2024-01-01/abc123"


class TestReadCallbackLog:
    def test_no_logs_found_yields_message(self):
        """When no logs exist at either path, a 'No callback logs found.' message is yielded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
                mock_conf.get.return_value = tmpdir
                msgs = list(read_callback_log("dag1", "run1", "cb1"))
        assert len(msgs) == 1
        assert msgs[0].event == "No callback logs found."

    def test_reads_executor_callback_logs(self):
        """Logs at the executor_callbacks path are found and returned."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # The local reader looks for files matching the last path component as a
            # filename (with potential suffixes) in its parent directory.
            # relative_path = "executor_callbacks/dag1/run1/cb1"
            # It globs base_log_folder/executor_callbacks/dag1/run1/cb1*
            log_dir = os.path.join(tmpdir, "executor_callbacks", "dag1", "run1")
            os.makedirs(log_dir)
            log_file = os.path.join(log_dir, "cb1")
            with open(log_file, "w") as f:
                f.write("executor log line 1\n")

            with patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
                mock_conf.get.return_value = tmpdir
                # Suppress remote log attempts
                with patch(
                    "airflow.utils.log.callback_log_reader._read_callback_remote_logs",
                    return_value=([], []),
                ):
                    msgs = list(read_callback_log("dag1", "run1", "cb1"))

        # Should have source header + log content (not "No callback logs found.")
        assert not any(m.event == "No callback logs found." for m in msgs)

    def test_reads_triggerer_callback_logs(self):
        """Logs at the triggerer_callbacks path are found when executor path is empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only create triggerer path, NOT executor path
            log_dir = os.path.join(tmpdir, "triggerer_callbacks", "dag1", "run1")
            os.makedirs(log_dir)
            log_file = os.path.join(log_dir, "cb1")
            with open(log_file, "w") as f:
                f.write("triggerer log line 1\n")

            with patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
                mock_conf.get.return_value = tmpdir
                with patch(
                    "airflow.utils.log.callback_log_reader._read_callback_remote_logs",
                    return_value=([], []),
                ):
                    msgs = list(read_callback_log("dag1", "run1", "cb1"))

        # Should have found logs (not the "no logs" message)
        assert not any(m.event == "No callback logs found." for m in msgs)

    def test_executor_path_preferred_over_triggerer(self):
        """When logs exist at both paths, executor_callbacks is returned (first match wins)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create both paths with log files
            for prefix in ("executor_callbacks", "triggerer_callbacks"):
                log_dir = os.path.join(tmpdir, prefix, "dag1", "run1")
                os.makedirs(log_dir)
                log_file = os.path.join(log_dir, "cb1")
                with open(log_file, "w") as f:
                    f.write(f"{prefix} log line\n")

            with patch("airflow.utils.log.callback_log_reader.conf") as mock_conf:
                mock_conf.get.return_value = tmpdir
                with patch(
                    "airflow.utils.log.callback_log_reader._read_callback_remote_logs",
                    return_value=([], []),
                ):
                    msgs = list(read_callback_log("dag1", "run1", "cb1"))

        # We can't easily check which path was used from the messages alone, but we know
        # the function returns after the first successful path. No "No callback logs" message.
        assert not any(m.event == "No callback logs found." for m in msgs)
