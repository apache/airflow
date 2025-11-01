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

import os
import sys
from unittest import mock

import pytest

from airflow.utils import hot_reload


class TestHotReload:
    """Tests for hot reload utilities."""

    def test_run_with_reloader_missing_watchfiles(self):
        """Test that run_with_reloader exits gracefully when watchfiles is not installed."""
        with mock.patch.dict(sys.modules, {"watchfiles": None}):
            with pytest.raises(SystemExit):
                hot_reload.run_with_reloader(lambda: None)

    @mock.patch("airflow.utils.hot_reload._run_reloader")
    def test_run_with_reloader_main_process(self, mock_run_reloader):
        """Test run_with_reloader as the main process."""
        # Clear the reloader PID env var to simulate being the main process
        with mock.patch.dict(os.environ, {}, clear=True):
            callback = mock.Mock()
            watch_paths = ["/tmp/test"]
            exclude_patterns = ["*.pyc"]

            hot_reload.run_with_reloader(callback, watch_paths, exclude_patterns)

            # Should set the env var and call _run_reloader
            assert "AIRFLOW_DEV_RELOADER_PID" in os.environ
            mock_run_reloader.assert_called_once()

    def test_run_with_reloader_child_process(self):
        """Test run_with_reloader as a child process."""
        # Set the reloader PID env var to simulate being a child process
        with mock.patch.dict(os.environ, {"AIRFLOW_DEV_RELOADER_PID": "12345"}):
            callback = mock.Mock()
            hot_reload.run_with_reloader(callback)

            # Should just call the callback directly
            callback.assert_called_once()

    @mock.patch("subprocess.Popen")
    @mock.patch("airflow.utils.hot_reload.watch")
    def test_run_reloader_starts_process(self, mock_watch, mock_popen):
        """Test that _run_reloader starts a subprocess."""
        mock_process = mock.Mock()
        mock_popen.return_value = mock_process
        mock_watch.return_value = []  # Empty iterator, will exit immediately

        def test_callback():
            """Test callback function."""
            pass

        watch_paths = ["/tmp/test"]
        exclude_patterns = ["*.pyc"]

        hot_reload._run_reloader(test_callback, watch_paths, exclude_patterns)

        # Should have started a process
        mock_popen.assert_called_once()
        assert mock_popen.call_args[0][0] == [sys.executable] + sys.argv

    @mock.patch("subprocess.Popen")
    @mock.patch("airflow.utils.hot_reload.watch")
    def test_run_reloader_restarts_on_changes(self, mock_watch, mock_popen):
        """Test that _run_reloader restarts the process on file changes."""
        mock_process = mock.Mock()
        mock_popen.return_value = mock_process

        # Simulate one file change and then exit
        mock_watch.return_value = iter([[("change", "/tmp/test/file.py")]])

        def test_callback():
            """Test callback function."""
            pass

        watch_paths = ["/tmp/test"]
        exclude_patterns = ["*.pyc"]

        hot_reload._run_reloader(test_callback, watch_paths, exclude_patterns)

        # Should have started process twice (initial + restart)
        assert mock_popen.call_count == 2
        # Should have terminated the first process
        mock_process.terminate.assert_called()
