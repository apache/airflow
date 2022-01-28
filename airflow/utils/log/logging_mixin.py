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
import abc
import logging
import re
import sys
from io import IOBase
from logging import Handler, Logger, StreamHandler
from typing import IO, Optional

# 7-bit C1 ANSI escape sequences
ANSI_ESCAPE = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')


def remove_escape_codes(text: str) -> str:
    """
    Remove ANSI escapes codes from string. It's used to remove
    "colors" from log messages.
    """
    return ANSI_ESCAPE.sub("", text)


class LoggingMixin:
    """Convenience super-class to have a logger configured with the class name"""

    _log: Optional[logging.Logger] = None

    def __init__(self, context=None):
        self._set_context(context)

    @property
    def log(self) -> Logger:
        """Returns a logger."""
        if self._log is None:
            self._log = logging.getLogger(self.__class__.__module__ + '.' + self.__class__.__name__)
        return self._log

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)


class ExternalLoggingMixin:
    """Define a log handler based on an external service (e.g. ELK, StackDriver)."""

    @property
    @abc.abstractmethod
    def log_name(self) -> str:
        """Return log name"""

    @abc.abstractmethod
    def get_external_log_url(self, task_instance, try_number) -> str:
        """Return the URL for log visualization in the external service."""

    @property
    @abc.abstractmethod
    def supports_external_link(self) -> bool:
        """Return whether handler is able to support external links."""


# We have to ignore typing errors here because Python I/O classes are a mess, and they do not
# have the same type hierarchy defined as the `typing.IO` - they violate Liskov Substitution Principle
# While it is ok to make your class derive from IOBase (and its good thing to do as they provide
# base implementation for IO-implementing classes, it's impossible to make them work with
# IO generics (and apparently it has not even been intended)
# See more: https://giters.com/python/typeshed/issues/6077
class StreamLogWriter(IOBase, IO[str]):  # type: ignore[misc]
    """Allows to redirect stdout and stderr to logger"""

    encoding: None = None

    def __init__(self, logger, level):
        """
        :param log: The log level method to write to, ie. log.debug, log.warning
        :return:
        """
        self.logger = logger
        self.level = level
        self._buffer = ''

    def close(self):
        """
        Provide close method, for compatibility with the io.IOBase interface.

        This is a no-op method.
        """

    @property
    def closed(self):
        """
        Returns False to indicate that the stream is not closed, as it will be
        open for the duration of Airflow's lifecycle.

        For compatibility with the io.IOBase interface.
        """
        return False

    def _propagate_log(self, message):
        """Propagate message removing escape codes."""
        self.logger.log(self.level, remove_escape_codes(message))

    def write(self, message):
        """
        Do whatever it takes to actually log the specified logging record

        :param message: message to log
        """
        if not message.endswith("\n"):
            self._buffer += message
        else:
            self._buffer += message.rstrip()
            self.flush()

    def flush(self):
        """Ensure all logging output has been flushed"""
        buf = self._buffer
        if len(buf) > 0:
            self._buffer = ''
            self._propagate_log(buf)

    def isatty(self):
        """
        Returns False to indicate the fd is not connected to a tty(-like) device.
        For compatibility reasons.
        """
        return False


class RedirectStdHandler(StreamHandler):
    """
    This class is like a StreamHandler using sys.stderr/stdout, but always uses
    whatever sys.stderr/stderr is currently set to rather than the value of
    sys.stderr/stdout at handler construction time.
    """

    def __init__(self, stream):
        if not isinstance(stream, str):
            raise Exception(
                "Cannot use file like objects. Use 'stdout' or 'stderr' as a str and without 'ext://'."
            )

        self._use_stderr = True
        if 'stdout' in stream:
            self._use_stderr = False

        # StreamHandler tries to set self.stream
        Handler.__init__(self)

    @property
    def stream(self):
        """Returns current stream."""
        if self._use_stderr:
            return sys.stderr

        return sys.stdout


def set_context(logger, value):
    """
    Walks the tree of loggers and tries to set the context for each handler

    :param logger: logger
    :param value: value to set
    """
    _logger = logger
    while _logger:
        for handler in _logger.handlers:
            try:
                handler.set_context(value)
            except AttributeError:
                # Not all handlers need to have context passed in so we ignore
                # the error when handlers do not have set_context defined.
                pass
        if _logger.propagate is True:
            _logger = _logger.parent
        else:
            _logger = None
