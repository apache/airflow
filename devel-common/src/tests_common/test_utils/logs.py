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
import re
import sys
import traceback
from collections.abc import MutableMapping
from contextlib import ExitStack, contextmanager
from typing import TYPE_CHECKING, NoReturn

from airflow.models import Log

try:
    from airflow.sdk._shared.secrets_masker import DEFAULT_SENSITIVE_FIELDS
except ImportError:
    from airflow.sdk.execution_time.secrets_masker import DEFAULT_SENSITIVE_FIELDS  # type:ignore[no-redef]

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
    NAME_TO_LEVEL: dict[str, int]
    PER_LOGGER_LEVELS: MutableMapping[str, int]
    """The logger we specifically want to capture log messages from"""

    def __init__(self):
        self.entries = []
        self._initial_logger_levels: dict[str, int] = {}

        try:
            from airflow.sdk._shared.logging.structlog import NAME_TO_LEVEL, PER_LOGGER_LEVELS

            self.NAME_TO_LEVEL = NAME_TO_LEVEL
            self.PER_LOGGER_LEVELS = PER_LOGGER_LEVELS

            try:
                import airflow._shared.logging.structlog
            except ModuleNotFoundError:
                pass
            else:
                airflow._shared.logging.structlog.PER_LOGGER_LEVELS = PER_LOGGER_LEVELS
        except ModuleNotFoundError:
            from airflow._shared.logging.structlog import NAME_TO_LEVEL, PER_LOGGER_LEVELS

            self.NAME_TO_LEVEL = NAME_TO_LEVEL
            self.PER_LOGGER_LEVELS = PER_LOGGER_LEVELS

    def _finalize(self) -> None:
        """
        Finalize the fixture.

        This restores the log levels and the disabled logging levels changed by :meth:`set_level`.
        """
        for logger_name, level in self._initial_logger_levels.items():
            logger = logging.getLogger(logger_name)
            logger.setLevel(level)
            if level is logging.NOTSET:
                del self.PER_LOGGER_LEVELS[logger_name]
            else:
                self.PER_LOGGER_LEVELS[logger_name] = level

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
                    got = get(e)
                    return all(
                        expected.match(val) if isinstance(expected, re.Pattern) else val == expected
                        for (val, expected) in zip(got, want)
                    )
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
        return f"<StructlogCapture #entries={len(self.entries)}>"

    def __call__(self, logger: WrappedLogger, method_name: str, event_dict: EventDict) -> NoReturn:
        from structlog import DropEvent
        from structlog._log_levels import map_method_name

        event_dict["event"] = str(event_dict["event"])
        event_dict["log_level"] = map_method_name(method_name)
        if name := getattr(logger, "name", None):
            event_dict["logger_name"] = name

        # Capture the current exception. This mirrors the "ExceptionRenderer", but much more minimal for
        # testing
        if event_dict.get("exc_info") is True:
            event_dict["exc_info"] = sys.exc_info()
        self.entries.append(event_dict)

        raise DropEvent

    @property
    def text(self):
        """All the event text as a single multi-line string."""

        def exc_dict_to_string(exc):
            if isinstance(exc, tuple):
                yield from traceback.format_exception(*exc)
                return
            for i, e in enumerate(exc):
                if i != 0:
                    yield "\n"
                    yield "During handling of the above exception, another exception occurred:\n"
                    yield "\n"

                # This doesn't include the stacktrace, but this should be enough for testing
                yield f"{e['exc_type']}: {e['exc_value']}\n"

        def format(e):
            yield e["event"] + "\n"
            if exc_info := e.get("exc_info"):
                yield from exc_dict_to_string(exc_info)
            elif exc := e.get("exception"):
                yield from exc_dict_to_string(exc)

        return "".join(itertools.chain.from_iterable(map(format, self.entries)))

    # These next fns make it duck-type the same as Pytests "caplog" fixture
    @property
    def messages(self):
        """All the event messages as a list."""
        return [e["event"] for e in self.entries]

    @contextmanager
    def at_level(self, level: str | int, logger: str | None = None):
        if isinstance(level, str):
            level = self.NAME_TO_LEVEL[level.lower()]

        # Since we explicitly set the level of the "airflow" logger in our config, we want to set that by
        # default if the test auithor didn't ask for this at a specific logger to be set (otherwise we only
        # set the root logging level, which doesn't have any affect if sub loggers have explicit levels set)
        keys: tuple[str, ...] = (logger or "",)
        if not logger:
            keys += ("airflow",)

        def _reset(logger, key, level, orig_hdlr_level):
            logger.setLevel(level)
            if level is logging.NOTSET:
                del self.PER_LOGGER_LEVELS[key]
            else:
                self.PER_LOGGER_LEVELS[key] = level
            if logger.handlers:
                hdlr = logger.handlers[0]
                hdlr.setLevel(orig_hdlr_level)

        cm = ExitStack()
        for key in keys:
            old = self.PER_LOGGER_LEVELS.get(key, logging.NOTSET)
            self.PER_LOGGER_LEVELS[key] = level
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

    def set_level(self, level: str | int, logger: str | None = None):
        # Set the global level
        if isinstance(level, str):
            level = self.NAME_TO_LEVEL[level.lower()]

        key = logger or ""

        stdlogger = logging.getLogger(key)
        self._initial_logger_levels[key] = self.PER_LOGGER_LEVELS.get(key, logging.NOTSET)

        self.PER_LOGGER_LEVELS[key] = level
        stdlogger.setLevel(level)

    def clear(self):
        self.entries = []

    # pytest caplog support:
    # TODO: deprecate and remove all of this in tests
    @property
    def records(self):
        records = []
        for entry in self.entries:
            record = logging.LogRecord(
                entry.get("logger", "") or entry.get("logger_name"),
                self.NAME_TO_LEVEL.get(entry.get("log_level"), 0),
                "?",
                0,
                entry["event"],
                (),
                entry.get("exc_info") or entry.get("exception"),
                None,
                None,
            )
            record.message = record.msg
            records.append(record)
        return records

    @property
    def record_tuples(self):
        return [
            (
                entry.get("logger", "") or entry.get("logger_name"),
                self.NAME_TO_LEVEL.get(entry.get("log_level"), 0),
                entry.get("event"),
            )
            for entry in self.entries
        ]
