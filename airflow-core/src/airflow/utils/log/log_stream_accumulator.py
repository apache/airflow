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
import tempfile
from itertools import islice
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.typing_compat import Self
    from airflow.utils.log.file_task_handler import (
        LogHandlerOutputStream,
        StructuredLogMessage,
        StructuredLogStream,
    )


class LogStreamAccumulator:
    """
    Memory-efficient log stream accumulator that tracks the total number of lines while preserving the original stream.

    This class captures logs from a stream and stores them in a buffer, flushing them to disk when the buffer
    exceeds a specified threshold. This approach optimizes memory usage while handling large log streams.

    Usage:

    .. code-block:: python

        with LogStreamAccumulator(stream, threshold) as log_accumulator:
            # Get total number of lines captured
            total_lines = log_accumulator.get_total_lines()

            # Retrieve the original stream of logs
            for log in log_accumulator.get_stream():
                print(log)
    """

    def __init__(
        self,
        stream: LogHandlerOutputStream,
        threshold: int,
    ) -> None:
        """
        Initialize the LogStreamAccumulator.

        Args:
            stream: The input log stream to capture and count.
            threshold: Maximum number of lines to keep in memory before flushing to disk.
        """
        self._stream = stream
        self._threshold = threshold
        self._buffer: list[StructuredLogMessage] = []
        self._disk_lines: int = 0
        self._tmpfile: IO[str] | None = None

    def _flush_buffer_to_disk(self) -> None:
        """Flush the buffer contents to a temporary file on disk."""
        if self._tmpfile is None:
            self._tmpfile = tempfile.NamedTemporaryFile(delete=False, mode="w+", encoding="utf-8")

        self._disk_lines += len(self._buffer)
        self._tmpfile.writelines(f"{log.model_dump_json()}\n" for log in self._buffer)
        self._tmpfile.flush()
        self._buffer.clear()

    def _capture(self) -> None:
        """Capture logs from the stream into the buffer, flushing to disk when threshold is reached."""
        while True:
            # `islice` will try to get up to `self._threshold` lines from the stream.
            self._buffer.extend(islice(self._stream, self._threshold))
            # If there are no more lines to capture, exit the loop.
            if len(self._buffer) < self._threshold:
                break
            self._flush_buffer_to_disk()

    def _cleanup(self) -> None:
        """Clean up the temporary file if it exists."""
        self._buffer.clear()
        if self._tmpfile:
            self._tmpfile.close()
            os.remove(self._tmpfile.name)
            self._tmpfile = None

    @property
    def total_lines(self) -> int:
        """
        Return the total number of lines captured from the stream.

        Returns:
            The sum of lines stored in the buffer and lines written to disk.
        """
        return self._disk_lines + len(self._buffer)

    @property
    def stream(self) -> StructuredLogStream:
        """
        Return the original stream of logs and clean up resources.

        Important: This method automatically cleans up resources after all logs have been yielded.
        Make sure to fully consume the returned generator to ensure proper cleanup.

        Returns:
            A stream of the captured log messages.
        """
        try:
            if not self._tmpfile:
                # if no temporary file was created, return from the buffer
                yield from self._buffer
            else:
                # avoid circular import
                from airflow.utils.log.file_task_handler import StructuredLogMessage

                with open(self._tmpfile.name, encoding="utf-8") as f:
                    yield from (StructuredLogMessage.model_validate_json(line.strip()) for line in f)
                # yield the remaining buffer
                yield from self._buffer
        finally:
            # Ensure cleanup after yielding
            self._cleanup()

    def __enter__(self) -> Self:
        """
        Context manager entry point that initiates log capture.

        Returns:
            Self instance for use in context manager.
        """
        self._capture()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Context manager exit that doesn't perform resource cleanup.

        Note: Resources are not cleaned up here. Cleanup is deferred until
        get_stream() is called and fully consumed, ensuring all logs are properly
        yielded before cleanup occurs.
        """
