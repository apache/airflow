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

"""Mixing as a base of Logging classes."""
import logging
import re
import sys
from logging import Handler, Logger, StreamHandler
# 7-bit C1 ANSI escape sequences
from types import TracebackType
from typing import IO, AnyStr, Iterable, Iterator, List, Optional, Type

ANSI_ESCAPE = re.compile(r'\x1B[@-_][0-?]*[ -/]*[@-~]')


def remove_escape_codes(text: str) -> str:
    """
    Remove ANSI escapes codes from string. It's used to remove
    "colors" from log messages.
    """
    return ANSI_ESCAPE.sub("", text)


class LoggingMixin:
    """
    Convenience super-class to have a logger configured with the class name
    """
    def __init__(self, context=None):
        self._set_context(context)
        self._log: Optional[Logger] = None

    @property
    def log(self) -> Logger:
        """
        Returns a logger.
        """
        try:
            # FIXME: LoggingMixin should have a default _log field.
            return self._log  # type: ignore
        except AttributeError:
            self._log = logging.root.getChild(
                self.__class__.__module__ + '.' + self.__class__.__name__
            )
            return self._log

    def _set_context(self, context):
        if context is not None:
            set_context(self.log, context)


class StreamLogWriter(IO[str]):
    """
    Allows to redirect stdout and stderr to logger
    """

    def close(self) -> None:
        pass

    def fileno(self) -> int:
        return 0

    def read(self, n: int = 0) -> str:
        return ""

    def readable(self) -> bool:
        return False

    def readline(self, limit: int = 0) -> str:
        return ""

    def readlines(self, hint: int = 0) -> List[AnyStr]:
        return []

    def seek(self, offset: int, whence: int = 0) -> int:
        return 0

    def seekable(self) -> bool:
        return False

    def tell(self) -> int:
        return 0

    def truncate(self, size: Optional[int] = 0) -> int:
        return 0

    def writable(self) -> bool:
        return True

    def writelines(self, lines: Iterable[AnyStr]) -> None:
        for line in lines:
            self.write(line)

    def __next__(self) -> AnyStr:
        pass

    def __iter__(self) -> Iterator[AnyStr]:
        pass

    def __enter__(self) -> IO[AnyStr]:
        pass

    def __exit__(self, t: Optional[Type[BaseException]], value: Optional[BaseException],
                 traceback: Optional[TracebackType]) -> Optional[bool]:
        pass

    encoding = False

    def __init__(self, logger: Logger, level: int):
        """
        :param logger: Logger to write the logs to
        :param level: The log level method to write to, ie. log.debug, log.warning
        """
        self.logger = logger
        self.level = level
        self._buffer = str()

    @property
    def closed(self) -> bool:
        """
        Returns False to indicate that the stream is not closed (as it will be
        open for the duration of Airflow's lifecycle).

        For compatibility with the io.IOBase interface.
        """
        return False

    def _propagate_log(self, message: AnyStr) -> None:
        """
        Propagate message removing escape codes.
        """
        if isinstance(message, bytes):
            message_str = message.decode("utf-8")
        else:
            message_str = message
        self.logger.log(self.level, remove_escape_codes(message_str))

    def write(self, message: AnyStr) -> int:
        """
        Do whatever it takes to actually log the specified logging record

        :param message: message to log
        """
        if isinstance(message, bytes):
            message_str = message.decode("utf-8")
        else:
            message_str = message
        if not message_str.endswith("\n"):
            self._buffer += message_str
        else:
            self._buffer += message_str
            self._propagate_log(self._buffer.rstrip())
            self._buffer = str()
        return len(message_str)

    def flush(self) -> None:
        """
        Ensure all logging output has been flushed
        """
        if self._buffer:
            self._propagate_log(self._buffer)
            self._buffer = str()

    def isatty(self) -> bool:
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
            raise Exception("Cannot use file like objects. Use 'stdout' or 'stderr'"
                            " as a str and without 'ext://'.")

        self._use_stderr = True
        if 'stdout' in stream:
            self._use_stderr = False

        # StreamHandler tries to set self.stream
        Handler.__init__(self)  # pylint: disable=non-parent-init-called

    @property
    def stream(self):
        """
        Returns current stream.
        """
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
