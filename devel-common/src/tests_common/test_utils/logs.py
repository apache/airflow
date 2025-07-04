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

import itertools
import json
import logging
import sys
import traceback
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, NoReturn

from airflow.models import Log
from airflow.sdk.execution_time.secrets_masker import DEFAULT_SENSITIVE_FIELDS

if TYPE_CHECKING:
    from structlog.typing import EventDict, WrappedLogger


def _masked_value_check(data, sensitive_fields):
    """
    Recursively check if sensitive fields are properly masked.

    :param data: JSON object (dict, list, or value)
    :param sensitive_fields: Set of sensitive field names
    """
    if isinstance(data, dict):
        for key, value in data.items():
            if key in sensitive_fields:
                assert value == "***", f"Expected masked value for {key}, but got {value}"
            else:
                _masked_value_check(value, sensitive_fields)
    elif isinstance(data, list):
        for item in data:
            _masked_value_check(item, sensitive_fields)


def check_last_log(session, dag_id, event, logical_date, expected_extra=None, check_masked=False):
    logs = (
        session.query(
            Log.dag_id,
            Log.task_id,
            Log.event,
            Log.logical_date,
            Log.owner,
            Log.extra,
        )
        .filter(
            Log.dag_id == dag_id,
            Log.event == event,
            Log.logical_date == logical_date,
        )
        .order_by(Log.dttm.desc())
        .limit(5)
        .all()
    )
    assert len(logs) >= 1
    assert logs[0].extra
    if expected_extra:
        assert json.loads(logs[0].extra) == expected_extra
    if check_masked:
        extra_json = json.loads(logs[0].extra)
        _masked_value_check(extra_json, DEFAULT_SENSITIVE_FIELDS)

    session.query(Log).delete()


class StructlogCapture:
    """
    Test that structlog messages are logged.

    This extends the feature built in to structlog to make it easier to find if a message is logged.

    >>> def test_something(cap_structlog):
    ...     log.info("some event", field1=False, field2=[1, 2])
    ...     log.info("some event", field1=True)
    ...     assert "some_event" in cap_structlog  # a string searches on `event` field
    ...     assert {"event": "some_event", "field1": True} in cap_structlog  # Searches only on passed fields
    ...     assert {"field2": [1, 2]} in cap_structlog
    ...
    ...     assert "not logged" not in cap_structlog  # not in works too

    This fixture class will also manage the log level of stdlib loggers via ``at_level`` and ``set_level``.
    """

    # This class is a manual mixing of pytest's LogCaptureFixture and structlog's LogCapture class, but
    # tailored to Airflow's "send all logs via structlog" approach

    _logger: str | None = None
    """The logger we specifically want to capture log messages from"""

    def __init__(self):
        self.entries = []
        self._initial_logger_levels: dict[str | None, int] = {}

    def _finalize(self) -> None:
        """
        Finalize the fixture.

        This restores the log levels and the disabled logging levels changed by :meth:`set_level`.
        """
        from airflow._logging.structlog import PER_LOGGER_LEVELS

        for logger_name, level in self._initial_logger_levels.items():
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)
            if level is logging.NOTSET:
                del PER_LOGGER_LEVELS[logger_name]
            else:
                PER_LOGGER_LEVELS[logger_name] = level

    def __contains__(self, target):
        import operator

        if isinstance(target, str):

            def predicate(e):
                return e["event"] == target
        elif isinstance(target, dict):
            # Partial comparison -- only check keys passed in
            get = operator.itemgetter(*target.keys())
            want = tuple(target.values())

            def predicate(e):
                try:
                    return get(e) == want
                except Exception:
                    return False
        else:
            raise TypeError(f"Can't search logs using {type(target)}")

        return any(predicate(e) for e in self.entries)

    def __getitem__(self, i):
        return self.entries[i]

    def __iter__(self):
        return iter(self.entries)

    def __repr__(self):
        return repr(self.entries)

    def __call__(self, logger: WrappedLogger, method_name: str, event_dict: EventDict) -> NoReturn:
        from structlog import DropEvent
        from structlog._log_levels import map_method_name

        from airflow._logging.structlog import (
            NamedBytesLogger,
            NamedWriteLogger,
        )

        logger_name = (
            event_dict.get("logger_name")
            or event_dict.get("logger")
            or (isinstance(logger, (NamedBytesLogger, NamedWriteLogger)) and logger.name)
            or ""
        )
        if not self._logger or logger_name.startswith(self._logger):
            event_dict["log_level"] = map_method_name(method_name)

            # Capture the current exception. This mirrors the "ExceptionRenderer", but much more minimal for
            # testing
            if event_dict.get("exc_info") is True:
                event_dict["exc_info"] = sys.exc_info()
            self.entries.append(event_dict)

        raise DropEvent

    @property
    def text(self):
        """All the event text as a single multi-line string."""

        def format(e):
            yield e["event"] + "\n"
            if exc_info := e.get("exc_info"):
                yield from traceback.format_exception(*exc_info)

        return "".join(itertools.chain.from_iterable(map(format, self.entries)))

    # These next fns make it duck-type the same as Pytests "caplog" fixture
    @property
    def messages(self):
        """All the event messages as a list."""
        return [e["event"] for e in self.entries]

    def _force_enable_logging(self, level: int, logger_obj: logging.Logger) -> int:
        """
        Enable the desired logging level if the global level was disabled via ``logging.disabled``.

        Only enables logging levels greater than or equal to the requested ``level``.

        Does nothing if the desired ``level`` wasn't disabled.

        :param level:
            The logger level caplog should capture.
            All logging is enabled if a non-standard logging level string is supplied.
            Valid level strings are in :data:`logging._nameToLevel`.
        :param logger_obj: The logger object to check.

        :return: The original disabled logging level.
        """
        original_disable_level: int = logger_obj.manager.disable

        if not logger_obj.isEnabledFor(level):
            # Each level is `10` away from other levels.
            # https://docs.python.org/3/library/logging.html#logging-levels
            disable_level = max(level - 10, logging.NOTSET)
            logging.disable(disable_level)

        return original_disable_level

    @contextmanager
    def at_level(self, level: str | int, logger=None):
        from airflow._logging.structlog import NAME_TO_LEVEL, PER_LOGGER_LEVELS

        if isinstance(level, str):
            level = NAME_TO_LEVEL[level.lower()]

        # Since we explicitly set the level of the "airflow" logger in our config, we want to set that by
        # default if the test auithor didn't ask for this at a specific logger to be set (otherwise we only
        # set the root logging level, which doesn't have any affect if sub loggers have explicit levels set)
        keys: tuple[str, ...] = (logger or "",)
        if not logger:
            keys += ("airflow",)

        def _reset(logger, key, level, orig_hdlr_level):
            logger.setLevel(level)
            if level is logging.NOTSET:
                del PER_LOGGER_LEVELS[key]
            else:
                PER_LOGGER_LEVELS[key] = level
            if logger.handlers:
                hdlr = logger.handlers[0]
                hdlr.setLevel(orig_hdlr_level)

        cm = ExitStack()
        for key in keys:
            old = PER_LOGGER_LEVELS.get(key, logging.NOTSET)
            PER_LOGGER_LEVELS[key] = level
            stdlogger = logging.getLogger(key)
            stdlogger.setLevel(level)
            hdlr = orig_hdlr_level = None
            if stdlogger.handlers:
                hdlr = stdlogger.handlers[0]
                orig_hdlr_level = hdlr.level
                hdlr.setLevel(level)
            cm.callback(_reset, stdlogger, key, old, orig_hdlr_level)

        with cm:
            yield self

    def set_level(self, level: str | int, logger=None):
        from airflow._logging.structlog import NAME_TO_LEVEL, PER_LOGGER_LEVELS

        # Set the global level
        if isinstance(level, str):
            level = NAME_TO_LEVEL[level.lower()]

        key = logger or ""

        stdlogger = logging.getLogger(key)
        self._initial_logger_levels[key] = PER_LOGGER_LEVELS.get(key, logging.NOTSET)

        PER_LOGGER_LEVELS[key] = level
        stdlogger.setLevel(level)
        self._logger = logger

    def clear(self):
        self.entries = []

    @property
    def records(self):
        return self.entries
