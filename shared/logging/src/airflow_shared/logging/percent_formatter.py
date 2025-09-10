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

import collections.abc
import datetime
import os
import sys
from io import StringIO
from typing import TYPE_CHECKING

import structlog.dev
from structlog.dev import ConsoleRenderer, Styles

if TYPE_CHECKING:
    from structlog.typing import (
        EventDict,
        WrappedLogger,
    )


class _LazyLogRecordDict(collections.abc.Mapping):
    __slots__ = ("event", "styles", "level_styles", "method_name", "no_colors")

    def __init__(self, event: EventDict, method_name: str, level_styles: dict[str, str], styles: Styles):
        self.event = event
        self.method_name = method_name
        self.level_styles = level_styles
        self.styles = styles
        self.no_colors = self.styles.reset == ""

    def __getitem__(self, key):
        # Roughly compatible with names from https://github.com/python/cpython/blob/v3.13.7/Lib/logging/__init__.py#L571
        # Plus with ColoredLog added in
        if key == "name":
            return self.event.get("logger") or self.event.get("logger_name")
        if key == "levelname":
            return self.event.get("level", self.method_name).upper()
        if key == "pathname":
            return self.event.get("pathname", "?")
        if key == "filename":
            path = self.event.get("pathname", "?")
            return os.path.basename(path)
        if key == "module":
            path = self.event.get("pathname", "?")
            return os.path.splitext(os.path.basename(path))[0]
        if key == "lineno":
            return self.event.get("lineno", 0)
        if key == "asctime" or key == "created":
            return (
                self.event.get("timestamp", None)
                or datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
            )
        if key == "message":
            return self.event["event"]
        if key == "process":
            return os.getpid()

        if key in ("red", "green", "yellow", "blue", "purple", "cyan"):
            if self.no_colors:
                return ""
            return getattr(structlog.dev, key.upper(), "")
        if key == "reset":
            return self.styles.reset
        if key == "log_color":
            if self.no_colors:
                return ""
            return self.level_styles.get(self.event.get("level", self.method_name), "")

        return self.event[key]

    def __iter__(self):
        return self.event.__iter__()

    def __len__(self):
        return len(self.event)


class PercentFormatRender(ConsoleRenderer):
    """A Structlog processor that uses a stdlib-like percent based format string."""

    _fmt: str

    special_keys = {
        "pathname",
        "lineno",
        "func_name",
        "event",
        "name",
        "logger",
        "logger_name",
        "timestamp",
        "level",
    }

    def __init__(self, fmt: str, **kwargs):
        super().__init__(**kwargs)
        self._fmt = fmt

    def __call__(self, logger: WrappedLogger, method_name: str, event_dict: EventDict):
        exc = event_dict.pop("exception", None)
        exc_info = event_dict.pop("exc_info", None)
        stack = event_dict.pop("stack", None)
        params = _LazyLogRecordDict(
            event_dict,
            method_name,
            ConsoleRenderer.get_default_level_styles(),
            self._styles,
        )

        sio = StringIO()
        sio.write(self._fmt % params)

        sio.write(
            "".join(
                " " + self._default_column_formatter(key, val)
                for key, val in event_dict.items()
                if key not in self.special_keys
            ).rstrip(" ")
        )

        if stack is not None:
            sio.write("\n" + stack)
            if exc_info or exc is not None:
                sio.write("\n\n" + "=" * 79 + "\n")

        if exc_info:
            if isinstance(exc_info, BaseException):
                exc_info = (exc_info.__class__, exc_info, exc_info.__traceback__)
            if not isinstance(exc_info, tuple):
                if (exc_info := sys.exc_info()) == (None, None, None):
                    exc_info = None
            if exc_info:
                self._exception_formatter(sio, exc_info)
        elif exc is not None:
            sio.write("\n" + exc)

        return sio.getvalue()
