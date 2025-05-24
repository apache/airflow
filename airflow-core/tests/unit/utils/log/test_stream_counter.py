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
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest

from airflow.utils.log.file_task_handler import StructuredLogMessage
from airflow.utils.log.log_stream_counter import LogStreamCounter

if TYPE_CHECKING:
    from airflow.utils.log.file_task_handler import LogHandlerOutputStream

LOG_START_DATETIME = pendulum.datetime(2023, 10, 1, 0, 0, 0)


class TestLogStreamCounter:
    """Test cases for the LogStreamCounter class."""

    @pytest.fixture
    def structured_logs(self):
        """Create a list of mock structured log messages."""
        return [
            StructuredLogMessage(
                timestamp=LOG_START_DATETIME.add(seconds=i),
                event=f"test_event_{i + 1}",
            )
            for i in range(20)
        ]

    def validate_log_stream(self, log_stream: LogHandlerOutputStream):
        """Validate the log stream by checking the number of lines."""

        count = 0
        for i, log in enumerate(log_stream):
            assert log.event == f"test_event_{i + 1}"
            assert log.timestamp == LOG_START_DATETIME.add(seconds=i)
            count += 1
        assert count == 20

    def test__capture(self, structured_logs):
        """Test that temporary file is properly cleaned up during get_stream, not when exiting context."""

        counter = LogStreamCounter(structured_logs, 5)
        with (
            mock.patch.object(counter, "_capture") as mock_setup,
        ):
            with counter:
                mock_setup.assert_called_once()

    def test__flush_buffer_to_disk(self, structured_logs):
        """Test flush-to-disk behavior with a small threshold."""
        threshold = 6

        # Mock the temporary file to verify it's being written to
        with (
            mock.patch("tempfile.NamedTemporaryFile") as mock_tmpfile,
        ):
            mock_file = mock.MagicMock()
            mock_tmpfile.return_value = mock_file

            with LogStreamCounter(structured_logs, threshold) as counter:
                mock_tmpfile.assert_called_once_with(
                    delete=False,
                    mode="w+",
                    encoding="utf-8",
                )
                # Verify _flush_buffer_to_disk was called multiple times
                # (20 logs / 6 threshold = 3 flushes + 2 remaining logs in buffer)
                assert counter._disk_lines == 18
                assert mock_file.writelines.call_count == 3
                assert len(counter._buffer) == 2

    @pytest.mark.parametrize(
        "threshold",
        [
            pytest.param(30, id="buffer_only"),
            pytest.param(5, id="flush_to_disk"),
        ],
    )
    def test_get_stream(self, structured_logs, threshold):
        """Test that get_stream returns all logs regardless of whether they were flushed to disk."""

        tmpfile_name = None
        with LogStreamCounter(structured_logs, threshold) as counter:
            out_stream = counter.get_stream()

            # Check if the temporary file was created
            if threshold < len(structured_logs):
                tmpfile_name = counter._tmpfile.name
                assert os.path.exists(tmpfile_name)
            else:
                assert counter._tmpfile is None

            # Validate the log stream
            self.validate_log_stream(out_stream)

            # Verify temp file was created and cleaned up
            if threshold < len(structured_logs):
                assert counter._tmpfile is None
                assert not os.path.exists(tmpfile_name) if tmpfile_name else True

    @pytest.mark.parametrize(
        "threshold, expected_buffer_size, expected_disk_lines",
        [
            pytest.param(30, 20, 0, id="no_flush_needed"),
            pytest.param(10, 0, 20, id="single_flush_needed"),
            pytest.param(3, 2, 18, id="multiple_flushes_needed"),
        ],
    )
    def test_get_total_lines(self, structured_logs, threshold, expected_buffer_size, expected_disk_lines):
        """Test that LogStreamCounter correctly counts lines across buffer and disk."""

        with LogStreamCounter(structured_logs, threshold) as counter:
            # Check buffer and disk line counts
            assert len(counter._buffer) == expected_buffer_size
            assert counter._disk_lines == expected_disk_lines
            # Validate the log stream and line counts
            self.validate_log_stream(counter.get_stream())

    def test__cleanup(self, structured_logs):
        """Test that cleanup happens when get_stream() is fully consumed, not on context exit."""

        counter = LogStreamCounter(structured_logs, 5)
        with mock.patch.object(counter, "_cleanup") as mock_cleanup:
            with counter:
                # _cleanup should not be called yet
                mock_cleanup.assert_not_called()

                # Get the stream but don't iterate through it yet
                stream = counter.get_stream()
                mock_cleanup.assert_not_called()

                # Now iterate through the stream
                for _ in stream:
                    pass

                # After fully consuming the stream, cleanup should be called
                mock_cleanup.assert_called_once()
