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

from contextlib import ExitStack, redirect_stderr, redirect_stdout


class StreamCaptureManager(ExitStack):
    """Context manager class for capturing stdout and/or stderr while isolating from Logger output."""

    def __init__(self, capture_stdout=True, capture_stderr=False):
        super().__init__()
        from io import StringIO

        self.capture_stdout = capture_stdout
        self.capture_stderr = capture_stderr

        self._stdout_buffer = StringIO() if capture_stdout else None
        self._stderr_buffer = StringIO() if capture_stderr else None

        self._stdout_final = ""
        self._stderr_final = ""
        self._in_context = False

        # Store original streams
        self._original_stdout = None
        self._original_stderr = None

    @property
    def stdout(self) -> str:
        """Get captured stdout content."""
        if self._in_context and self._stdout_buffer and not self._stdout_buffer.closed:
            return self._stdout_buffer.getvalue()
        return self._stdout_final

    @property
    def stderr(self) -> str:
        """Get captured stderr content."""
        if self._in_context and self._stderr_buffer and not self._stderr_buffer.closed:
            return self._stderr_buffer.getvalue()
        return self._stderr_final

    def getvalue(self) -> str:
        """Get captured content. For backward compatibility, returns stdout by default."""
        return self.stdout if self.capture_stdout else self.stderr

    def get_combined(self) -> str:
        """Get combined stdout and stderr content."""
        parts = []
        if self.capture_stdout:
            parts.append(self.stdout)
        if self.capture_stderr:
            parts.append(self.stderr)
        return "".join(parts)

    def splitlines(self) -> list[str]:
        """Split captured content into lines."""
        content = self.getvalue()
        if not content:
            return [""]  # Return list with empty string to avoid IndexError
        return content.splitlines()

    def __enter__(self):
        import logging
        import sys

        # Set up context managers for redirection
        self._context_manager = ExitStack()

        self._in_context = True

        # Setup logging isolation
        root_logger = logging.getLogger()
        original_handlers = list(root_logger.handlers)

        def reset_handlers():
            from logging import _lock

            root_logger = logging.getLogger()

            with _lock:
                root_logger.handlers = original_handlers

        self.callback(reset_handlers)

        # Remove stream handlers that would interfere with capture
        handlers_to_remove = []
        for handler in original_handlers:
            if isinstance(handler, logging.StreamHandler):
                if self.capture_stdout and handler.stream == sys.stdout:
                    handlers_to_remove.append(handler)
                elif self.capture_stderr and handler.stream == sys.stderr:
                    handlers_to_remove.append(handler)

        for handler in handlers_to_remove:
            root_logger.removeHandler(handler)

        def final_value(buffer, attrname):
            if buffer:
                try:
                    val = buffer.getvalue()
                except (ValueError, AttributeError):
                    val = ""
                setattr(self, attrname, val)

        if self.capture_stdout:
            self._stdout_redirect = redirect_stdout(self._stdout_buffer)
            self.enter_context(self._stdout_redirect)
            self.callback(final_value, buffer=self._stdout_buffer, attrname="_stdout_final")

        if self.capture_stderr:
            self._stderr_redirect = redirect_stderr(self._stderr_buffer)
            self.enter_context(self._stderr_redirect)
            self.callback(final_value, buffer=self._stderr_buffer, attrname="_stderr_final")

        self.callback(setattr, self, "_in_context", False)

        return super().__enter__()


# Convenience classes
class StdoutCaptureManager(StreamCaptureManager):
    """Convenience class for stdout-only capture."""

    def __init__(self):
        super().__init__(capture_stdout=True, capture_stderr=False)


class StderrCaptureManager(StreamCaptureManager):
    """Convenience class for stderr-only capture."""

    def __init__(self):
        super().__init__(capture_stdout=False, capture_stderr=True)


class CombinedCaptureManager(StreamCaptureManager):
    """Convenience class for capturing both stdout and stderr."""

    def __init__(self):
        super().__init__(capture_stdout=True, capture_stderr=True)
