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


class StreamCaptureManager:
    """Context manager class for capturing stdout and/or stderr while isolating from Logger output."""

    def __init__(self, capture_stdout=True, capture_stderr=False):
        from io import StringIO

        self.capture_stdout = capture_stdout
        self.capture_stderr = capture_stderr

        self._stdout_buffer = StringIO() if capture_stdout else None
        self._stderr_buffer = StringIO() if capture_stderr else None

        self.original_handlers = []
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
        from contextlib import redirect_stderr, redirect_stdout

        self._in_context = True

        # Setup logging isolation
        root_logger = logging.getLogger()
        self.original_handlers = list(root_logger.handlers)

        # Remove stream handlers that would interfere with capture
        handlers_to_remove = []
        for handler in self.original_handlers:
            if isinstance(handler, logging.StreamHandler):
                if self.capture_stdout and handler.stream == sys.stdout:
                    handlers_to_remove.append(handler)
                elif self.capture_stderr and handler.stream == sys.stderr:
                    handlers_to_remove.append(handler)

        for handler in handlers_to_remove:
            root_logger.removeHandler(handler)

        # Set up context managers for redirection
        self._context_managers = []

        if self.capture_stdout:
            self._stdout_redirect = redirect_stdout(self._stdout_buffer)
            self._context_managers.append(self._stdout_redirect)

        if self.capture_stderr:
            self._stderr_redirect = redirect_stderr(self._stderr_buffer)
            self._context_managers.append(self._stderr_redirect)

        # Enter all context managers
        for cm in self._context_managers:
            cm.__enter__()

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        import logging
        import sys

        self._in_context = False

        # Capture content BEFORE cleaning up
        if self._stdout_buffer:
            try:
                self._stdout_final = self._stdout_buffer.getvalue()
            except (ValueError, AttributeError):
                self._stdout_final = ""

        if self._stderr_buffer:
            try:
                self._stderr_final = self._stderr_buffer.getvalue()
            except (ValueError, AttributeError):
                self._stderr_final = ""

        # Exit all context managers in reverse order
        for cm in reversed(self._context_managers):
            try:
                cm.__exit__(exc_type, exc_val, exc_tb)
            except Exception:
                pass  # Don't let cleanup failures mask the real error

        # Restore logging handlers
        root_logger = logging.getLogger()
        for handler in self.original_handlers:
            if isinstance(handler, logging.StreamHandler):
                if (self.capture_stdout and handler.stream == sys.stdout) or (
                    self.capture_stderr and handler.stream == sys.stderr
                ):
                    if handler not in root_logger.handlers:
                        root_logger.addHandler(handler)


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
