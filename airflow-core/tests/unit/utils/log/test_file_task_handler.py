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

from pathlib import Path
from unittest.mock import MagicMock, patch

from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.file_task_handler import convert_list_to_stream, extract_events


class TestFileTaskHandlerLogServer:
    """Tests for _read_from_logs_server 404 handling."""

    def setup_method(self):
        self.handler = FileTaskHandler(base_log_folder="/tmp/test_logs")

        self.ti = MagicMock()
        self.ti.hostname = "worker-1"
        self.ti.triggerer_job = None
        self.ti.task = None

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    @patch.object(FileTaskHandler, "_read_from_local")
    def test_404_falls_back_to_local_when_available(self, mock_read_local, mock_get_url, mock_fetch):
        """When log server returns 404 and local logs exist, use local logs."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_fetch.return_value = mock_response

        mock_stream = MagicMock()
        mock_read_local.return_value = (["/tmp/test_logs/dag/run/task/1.log"], [mock_stream])

        sources, streams = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert sources == ["/tmp/test_logs/dag/run/task/1.log"]
        assert streams == [mock_stream]
        mock_read_local.assert_called_once_with(Path("/tmp/test_logs", "dag/run/task/1.log"))

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    @patch.object(FileTaskHandler, "_read_from_local")
    def test_404_shows_clear_message_when_no_local_fallback(self, mock_read_local, mock_get_url, mock_fetch):
        """When log server returns 404 and no local logs exist, show helpful message."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_fetch.return_value = mock_response

        mock_read_local.return_value = ([], [])

        sources, streams = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert len(sources) == 1
        assert "worker-1" in sources[0]
        assert "no longer accessible" in sources[0]
        assert "remote logging" in sources[0]
        assert streams == []

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    def test_403_shows_secret_key_message(self, mock_get_url, mock_fetch):
        """When log server returns 403, show secret key configuration message."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_fetch.return_value = mock_response

        sources, streams = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert len(sources) == 1
        assert "secret_key" in sources[0]
        assert streams == []

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    def test_read_from_logs_server_no_hostname(self, mock_get_url, mock_fetch):
        """When hostname is missing, show a clear message instead of attempting log server fetch."""
        mock_get_url.return_value = (None, None)

        sources, streams = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert len(sources) == 1
        assert "Hostname not available" in sources[0]
        assert "worker" in sources[0]
        assert streams == []
        mock_fetch.assert_not_called()

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    def test_read_from_logs_server_no_hostname_triggerer(self, mock_get_url, mock_fetch):
        """When hostname is missing for triggerer, show a clear message instead of attempting log server fetch."""
        mock_get_url.return_value = (None, None)
        self.ti.triggerer_job = MagicMock()
        self.ti.triggerer_job.hostname = None
        self.ti.triggerer_job.id = 123

        sources, streams = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert len(sources) == 1
        assert "Hostname not available" in sources[0]
        assert "trigger" in sources[0]
        assert streams == []
        mock_fetch.assert_not_called()


class TestFileTaskHandlerReadFromLocal:
    """Tests for ``FileTaskHandler._read_from_local`` path containment."""

    @staticmethod
    def _drain(stream) -> str:
        return "".join(list(stream))

    def test_reads_regular_log_file_inside_base(self, tmp_path):
        """A regular file under ``base_log_folder`` is streamed as before."""
        log_dir = tmp_path / "dag" / "run" / "task"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "1.log"
        log_file.write_text("legitimate log line\n")

        handler = FileTaskHandler(base_log_folder=str(tmp_path))
        sources, streams = handler._read_from_local(log_file)

        assert sources == [str(log_file)]
        assert len(streams) == 1
        assert "legitimate log line" in self._drain(streams[0])

    def test_skips_symlink_resolving_outside_base_log_folder(self, tmp_path):
        """A glob hit that resolves outside ``base_log_folder`` is not streamed.

        This documents the intended containment behaviour: a file under the
        task's log directory that is actually a symlink whose real path is
        outside the configured base log folder must be skipped, even though
        it matches the glob pattern used to discover the task's log files.
        """
        base_log_folder = tmp_path / "logs"
        log_dir = base_log_folder / "dag" / "run" / "task"
        log_dir.mkdir(parents=True)

        # A regular log file inside the base log folder.
        legit = log_dir / "1.log"
        legit.write_text("legitimate log line\n")

        # A file that lives outside the base log folder.
        external_dir = tmp_path / "external"
        external_dir.mkdir()
        external_file = external_dir / "other.txt"
        external_file.write_text("external content\n")

        # A glob hit that matches ``1.log*`` but resolves outside the base.
        escape_link = log_dir / "1.log.external"
        escape_link.symlink_to(external_file)

        handler = FileTaskHandler(base_log_folder=str(base_log_folder))
        sources, streams = handler._read_from_local(legit)

        assert str(legit) in sources
        assert str(escape_link) not in sources
        content = "".join(self._drain(s) for s in streams)
        assert "legitimate log line" in content
        assert "external content" not in content

    def test_follows_symlink_within_base_log_folder(self, tmp_path):
        """A symlink that resolves back into the base log folder is allowed.

        The containment check compares the realpath of the glob hit to the
        realpath of the base log folder, so a symlink that stays entirely
        inside the log tree (for example from log rotation) still works.
        """
        base_log_folder = tmp_path / "logs"
        log_dir = base_log_folder / "dag" / "run" / "task"
        log_dir.mkdir(parents=True)

        real_file = log_dir / "real.log"
        real_file.write_text("inner content\n")

        link = log_dir / "1.log.link"
        link.symlink_to(real_file)

        handler = FileTaskHandler(base_log_folder=str(base_log_folder))
        sources, streams = handler._read_from_local(log_dir / "1.log")

        assert str(link) in sources
        assert "inner content" in "".join(self._drain(s) for s in streams)

    def test_handles_base_log_folder_that_is_itself_a_symlink(self, tmp_path):
        """``base_log_folder`` itself is realpath'd so a base that is a
        symlink to the actual log directory is treated as contained."""
        real_base = tmp_path / "real_logs"
        real_base.mkdir()
        base_link = tmp_path / "logs"
        base_link.symlink_to(real_base)

        log_dir = base_link / "dag" / "run" / "task"
        log_dir.mkdir(parents=True)
        log_file = log_dir / "1.log"
        log_file.write_text("through-symlink content\n")

        handler = FileTaskHandler(base_log_folder=str(base_link))
        sources, streams = handler._read_from_local(log_file)

        assert len(sources) == 1
        assert "through-symlink content" in self._drain(streams[0])


class TestFileTaskHandlerExecutorLogs:
    """Tests for executor log retrieval selection."""

    @staticmethod
    def _running_ti(executor_name: str) -> MagicMock:
        ti = MagicMock()
        ti.executor = executor_name
        ti.state = TaskInstanceState.RUNNING
        ti.try_number = 1
        return ti

    def test_running_task_prefers_streaming_executor_logs(self):
        """Use executor streaming logs when the executor implements streaming."""
        handler = FileTaskHandler(base_log_folder="")
        executor = MagicMock()
        executor.get_streaming_task_log.return_value = (
            ["streaming source"],
            [convert_list_to_stream(["streaming log"])],
        )
        executor.get_task_log.return_value = (["legacy source"], ["legacy log"])
        handler.executor_instances = {"StreamingExecutor": executor}
        ti = self._running_ti("StreamingExecutor")

        with (
            patch.object(handler, "_render_filename", return_value="dag/run/task/1.log"),
            patch.object(handler, "_read_remote_logs", side_effect=NotImplementedError),
            patch.object(handler, "_read_from_local", return_value=([], [])),
            patch.object(handler, "_read_from_logs_server", return_value=([], [])) as read_from_logs_server,
        ):
            logs, metadata = handler._read(ti=ti, try_number=1)

        executor.get_streaming_task_log.assert_called_once_with(ti, 1)
        executor.get_task_log.assert_not_called()
        read_from_logs_server.assert_not_called()
        assert extract_events(logs, skip_source_info=False) == [
            "::group::Log message source details",
            "streaming source",
            "::endgroup::",
            "streaming log",
        ]
        assert metadata == {"end_of_log": False, "log_pos": 1}

    def test_running_task_falls_back_to_legacy_executor_logs(self):
        """Use legacy executor logs when the executor doesn't implement streaming."""
        handler = FileTaskHandler(base_log_folder="")
        executor = MagicMock()
        executor.get_streaming_task_log.side_effect = NotImplementedError
        executor.get_task_log.return_value = (["legacy source"], ["legacy log"])
        handler.executor_instances = {"LegacyExecutor": executor}
        ti = self._running_ti("LegacyExecutor")

        with (
            patch.object(handler, "_render_filename", return_value="dag/run/task/1.log"),
            patch.object(handler, "_read_remote_logs", side_effect=NotImplementedError),
            patch.object(handler, "_read_from_local", return_value=([], [])),
            patch.object(handler, "_read_from_logs_server", return_value=([], [])) as read_from_logs_server,
        ):
            logs, metadata = handler._read(ti=ti, try_number=1)

        executor.get_streaming_task_log.assert_called_once_with(ti, 1)
        executor.get_task_log.assert_called_once_with(ti, 1)
        read_from_logs_server.assert_not_called()
        assert extract_events(logs, skip_source_info=False) == [
            "::group::Log message source details",
            "legacy source",
            "::endgroup::",
            "legacy log",
        ]
        assert metadata == {"end_of_log": False, "log_pos": 1}
