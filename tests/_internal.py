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

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pytest


class LogCaptureFixtureWrapper:
    """
    Provides access and control of log capturing (extending).

    This class is used to provide additional capabilities over the ``caplog`` fixture:
        https://docs.pytest.org/en/stable/how-to/logging.html#caplog-fixture

    .. code-block:: python
        def test_something(self, ext_caplog):
            # Use in resulting asserts only for this severities
            ext_caplog.log_level = ["DEBUG", "INFO", "WARNING"]
            # And only for `airflow.task`
            ext_caplog.logger_name = "airflow.task"
            do_something()
            assert ext_caplog.messages == ["Foo", "Bar"]
    """

    def __init__(self, caplog: pytest.LogCaptureFixture):
        self.caplog = caplog
        self._logger_name: str | None = None
        self._log_level: tuple[int, ...] | None = None

    @property
    def logger_name(self) -> str | None:
        """Return records only for the specified logger name.

        For reset filter by logger name set `None`.
        """
        return self._logger_name

    @logger_name.setter
    def logger_name(self, value: str | logging.Logger | None) -> None:
        if isinstance(value, logging.Logger):
            value = value.name
        self._logger_name = value

    @property
    def log_level(self) -> tuple[int, ...] | None:
        """Return records only for the specified logging severities.

        For reset filter by logger severity set it to `None`.
        """
        return self._log_level

    @log_level.setter
    def log_level(self, value: list[str | int] | str | int | None) -> None:
        if value is None:
            self._log_level = None
            return

        if isinstance(value, (str, int)):
            value = [value]
        self._log_level = tuple(map(self._resolve_levelno, value))

    @staticmethod
    def _resolve_levelno(value: str | int) -> int:
        if isinstance(value, int):
            return value

        levelno = logging.getLevelName(value)
        if not isinstance(levelno, int):
            msg = f"Invalid log level: {value!r}."
            raise RuntimeError(msg)
        return levelno

    @property
    def text(self) -> str:
        """
        The formatted log text (wrapper around caplog fixture).

        Note: No filters are applied on this property and return result as it is captured in ``caplog``
        """
        return self.caplog.text

    def _filter_record(self, record: logging.LogRecord):
        if self.logger_name and self.logger_name != record.name:
            return False
        if self.log_level is not None and record.levelno not in self.log_level:
            return False
        return True

    @property
    def records(self) -> list[logging.LogRecord]:
        """The list of log records with applying filters to it."""
        return list(filter(self._filter_record, self.caplog.records))

    @property
    def record_tuples(self) -> list[tuple[str, int, str]]:
        """A list of a stripped down version of log records with applying filters to it."""
        return [(r.name, r.levelno, r.getMessage()) for r in self.records]

    @property
    def messages(self) -> list[str]:
        """A list of format-interpolated log messages with applying filters to it."""
        return [r.getMessage() for r in self.records]

    def clear(self) -> None:
        """Reset the list of log records and the captured log text (wrapper around caplog fixture)."""
        self.caplog.clear()

    def set_level(self, level: int | str, logger: str | None = None) -> None:
        """Set threshold severity level for ``caplog``fixture."""
        self.caplog.set_level(level, logger=logger)
