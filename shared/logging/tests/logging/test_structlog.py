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

import contextlib
import io
import json
import logging
import os
import sys
import textwrap
from datetime import datetime, timezone
from unittest import mock

import pytest
import structlog
from structlog.dev import BLUE, BRIGHT, CYAN, DIM, GREEN, MAGENTA, RESET_ALL as RESET
from structlog.processors import CallsiteParameter

from airflow_shared.logging import structlog as structlog_module
from airflow_shared.logging.structlog import configure_logging

# We don't want to use the caplog fixture in this test, as the main purpose of this file is to capture the
# _rendered_ output of the tests to make sure it is correct.

PY_3_11 = sys.version_info >= (3, 11)


@pytest.fixture(autouse=True)
def set_time(time_machine):
    time_machine.move_to(datetime(1985, 10, 26, microsecond=1, tzinfo=timezone.utc), tick=False)


@pytest.fixture
def structlog_config():
    @contextlib.contextmanager
    def configurer(**kwargs):
        prev_config = structlog.get_config()

        try:
            if kwargs.get("json_output"):
                buff = io.BytesIO()
            else:
                buff = io.StringIO()

            with mock.patch("sys.stdout") as mock_stdout:
                mock_stdout.isatty.return_value = True
                configure_logging(**kwargs, output=buff)

            yield buff
            buff.seek(0)
        finally:
            structlog.configure(**prev_config)

    return configurer


@pytest.mark.parametrize(
    ("get_logger", "config_kwargs", "extra_kwargs", "extra_output"),
    [
        pytest.param(
            structlog.get_logger,
            {},
            {"key1": "value1"},
            f" {CYAN}key1{RESET}={MAGENTA}value1{RESET}",
            id="structlog",
        ),
        pytest.param(
            structlog.get_logger,
            {"callsite_parameters": [CallsiteParameter.PROCESS]},
            {"key1": "value1"},
            f" {CYAN}key1{RESET}={MAGENTA}value1{RESET} {CYAN}process{RESET}={MAGENTA}{os.getpid()}{RESET}",
            id="structlog-callsite",
        ),
        pytest.param(
            logging.getLogger,
            {},
            {},
            "",
            id="stdlib",
        ),
        pytest.param(
            logging.getLogger,
            {"callsite_parameters": [CallsiteParameter.PROCESS]},
            {},
            f" {CYAN}process{RESET}={MAGENTA}{os.getpid()}{RESET}",
            id="stdlib-callsite",
        ),
    ],
)
def test_colorful(structlog_config, get_logger, config_kwargs, extra_kwargs, extra_output):
    with structlog_config(colors=True, **config_kwargs) as sio:
        logger = get_logger("my.logger")
        # Test that interoplations work too
        x = "world"
        logger.info("Hello %s", x, **extra_kwargs)

    written = sio.getvalue()
    # This _might_ be a little bit too specific to structlog's ConsoleRender format
    assert (
        written == f"{DIM}1985-10-26T00:00:00.000001Z{RESET} [{GREEN}{BRIGHT}info     {RESET}]"
        f" {BRIGHT}Hello world                   {RESET}"
        f" [{RESET}{BRIGHT}{BLUE}my.logger{RESET}]{RESET}" + extra_output + "\n"
    )


@pytest.mark.parametrize(
    ("no_color", "force_color", "is_tty", "colors_param", "expected_colors"),
    [
        # NO_COLOR takes precedence over everything
        pytest.param("1", "", True, True, False, id="no_color_set_tty_colors_true"),
        pytest.param("1", "", True, False, False, id="no_color_set_tty_colors_false"),
        pytest.param("1", "", False, True, False, id="no_color_set_no_tty_colors_true"),
        pytest.param("1", "", False, False, False, id="no_color_set_no_tty_colors_false"),
        pytest.param("1", "1", True, True, False, id="no_color_and_force_color_tty_colors_true"),
        pytest.param("1", "1", True, False, False, id="no_color_and_force_color_tty_colors_false"),
        pytest.param("1", "1", False, True, False, id="no_color_and_force_color_no_tty_colors_true"),
        pytest.param("1", "1", False, False, False, id="no_color_and_force_color_no_tty_colors_false"),
        # FORCE_COLOR takes precedence when NO_COLOR is not set
        pytest.param("", "1", True, True, True, id="force_color_tty_colors_true"),
        pytest.param("", "1", True, False, True, id="force_color_tty_colors_false"),
        pytest.param("", "1", False, True, True, id="force_color_no_tty_colors_true"),
        pytest.param("", "1", False, False, True, id="force_color_no_tty_colors_false"),
        # When neither NO_COLOR nor FORCE_COLOR is set, check TTY and colors param
        pytest.param("", "", True, True, True, id="tty_colors_true"),
        pytest.param("", "", True, False, False, id="tty_colors_false"),
        pytest.param("", "", False, True, False, id="no_tty_colors_true"),
        pytest.param("", "", False, False, False, id="no_tty_colors_false"),
    ],
)
def test_color_config(monkeypatch, no_color, force_color, is_tty, colors_param, expected_colors):
    """Test all combinations of NO_COLOR, FORCE_COLOR, is_atty(), and colors parameter."""

    monkeypatch.setenv("NO_COLOR", no_color)
    monkeypatch.setenv("FORCE_COLOR", force_color)

    with mock.patch("sys.stdout") as mock_stdout:
        mock_stdout.isatty.return_value = is_tty

        with mock.patch.object(structlog_module, "structlog_processors") as mock_processors:
            mock_processors.return_value = ([], None, None)

            structlog_module.configure_logging(colors=colors_param)

            mock_processors.assert_called_once()
            assert mock_processors.call_args.kwargs["colors"] == expected_colors


@pytest.mark.parametrize(
    ("get_logger", "extra_kwargs", "extra_output"),
    [
        pytest.param(
            structlog.get_logger,
            {"key1": "value1"},
            f" {CYAN}key1{RESET}={MAGENTA}value1{RESET}",
            id="structlog",
        ),
        pytest.param(
            logging.getLogger,
            {},
            "",
            id="stdlib",
        ),
    ],
)
def test_precent_fmt(structlog_config, get_logger, extra_kwargs, extra_output):
    with structlog_config(colors=True, log_format="%(blue)s[%(asctime)s]%(reset)s %(message)s") as sio:
        logger = get_logger("my.logger")
        logger.info("Hello", **extra_kwargs)

    written = sio.getvalue()
    print(written)
    assert written == f"{BLUE}[1985-10-26T00:00:00.000001Z]{RESET} Hello" + extra_output + "\n"


def test_precent_fmt_force_no_colors(
    structlog_config,
):
    with structlog_config(
        colors=False,
        log_format="%(blue)s[%(asctime)s]%(reset)s {%(filename)s:%(lineno)d} %(log_color)s%(levelname)s - %(message)s",
    ) as sio:
        logger = structlog.get_logger("my.logger")
        logger.info("Hello", key1="value1")

        lineno = sys._getframe().f_lineno - 2

    written = sio.getvalue()
    assert (
        written == f"[1985-10-26T00:00:00.000001Z] {{test_structlog.py:{lineno}}} INFO - Hello key1=value1\n"
    )


def test_log_timestamp_format(structlog_config):
    """Test that log_timestamp_format controls the timestamp format in component logs."""
    with structlog_config(colors=False, log_timestamp_format="%Y-%m-%d %H:%M:%S") as sio:
        logger = structlog.get_logger("my.logger")
        logger.info("Hello")

    written = sio.getvalue()
    assert "1985-10-26 00:00:00" in written


@pytest.mark.parametrize(
    ("get_logger", "config_kwargs", "log_kwargs", "expected_kwargs"),
    [
        pytest.param(
            structlog.get_logger,
            {},
            {"key1": "value1"},
            {"key1": "value1"},
            id="structlog",
        ),
        pytest.param(
            structlog.get_logger,
            {"callsite_parameters": [CallsiteParameter.PROCESS]},
            {"key1": "value1"},
            {"key1": "value1", "process": os.getpid()},
            id="structlog-callsite",
        ),
        pytest.param(
            logging.getLogger,
            {},
            {},
            {},
            id="stdlib",
        ),
        pytest.param(
            logging.getLogger,
            {"callsite_parameters": [CallsiteParameter.PROCESS]},
            {},
            {"process": os.getpid()},
            id="stdlib-callsite",
        ),
    ],
)
def test_json(structlog_config, get_logger, config_kwargs, log_kwargs, expected_kwargs):
    with structlog_config(json_output=True, **(config_kwargs or {})) as bio:
        logger = get_logger("my.logger")
        logger.info("Hello", **log_kwargs)

    written = json.load(bio)
    assert written == {
        "event": "Hello",
        "level": "info",
        **expected_kwargs,
        "logger": "my.logger",
        "timestamp": "1985-10-26T00:00:00.000001Z",
    }


@pytest.mark.parametrize(
    ("get_logger"),
    [
        pytest.param(
            structlog.get_logger,
            id="structlog",
        ),
        pytest.param(
            logging.getLogger,
            id="stdlib",
        ),
    ],
)
def test_precent_fmt_exc(structlog_config, get_logger, monkeypatch):
    monkeypatch.setenv("DEV", "")
    with structlog_config(
        log_format="%(message)s",
        colors=False,
    ) as sio:
        lineno = sys._getframe().f_lineno + 2
        try:
            1 / 0
        except ZeroDivisionError:
            get_logger("logger").exception("Error")
    written = sio.getvalue()

    expected = textwrap.dedent(f"""\
        Error
        Traceback (most recent call last):
          File "{__file__}", line {lineno}, in test_precent_fmt_exc
            1 / 0
    """)
    if PY_3_11:
        expected += "    ~~^~~\n"
    expected += "ZeroDivisionError: division by zero\n"
    assert written == expected


@pytest.mark.parametrize(
    ("get_logger"),
    [
        pytest.param(
            structlog.get_logger,
            id="structlog",
        ),
        pytest.param(
            logging.getLogger,
            id="stdlib",
        ),
    ],
)
def test_json_exc(structlog_config, get_logger, monkeypatch):
    with structlog_config(json_output=True) as bio:
        lineno = sys._getframe().f_lineno + 2
        try:
            1 / 0
        except ZeroDivisionError:
            get_logger("logger").exception("Error")
    written = bio.getvalue()

    written = json.load(bio)
    assert written == {
        "event": "Error",
        "exception": [
            {
                "exc_notes": [],
                "exc_type": "ZeroDivisionError",
                "exc_value": "division by zero",
                "exceptions": [],
                "frames": [
                    {
                        "filename": __file__,
                        "lineno": lineno,
                        "name": "test_json_exc",
                    },
                ],
                "is_cause": False,
                "is_group": False,
                "syntax_error": None,
            },
        ],
        "level": "error",
        "logger": "logger",
        "timestamp": "1985-10-26T00:00:00.000001Z",
    }


@pytest.mark.parametrize(
    "levels",
    (
        pytest.param("my.logger=warn", id="str"),
        pytest.param({"my.logger": "warn"}, id="dict"),
    ),
)
def test_logger_filtering(structlog_config, levels):
    with structlog_config(
        colors=False,
        log_format="[%(name)s] %(message)s",
        log_level="DEBUG",
        namespace_log_levels=levels,
    ) as sio:
        structlog.get_logger("my").info("Hello", key1="value1")
        structlog.get_logger("my.logger").info("Hello", key1="value2")
        structlog.get_logger("my.logger.sub").info("Hello", key1="value3")
        structlog.get_logger("other.logger").info("Hello", key1="value4")
        structlog.get_logger("my.logger.sub").warning("Hello", key1="value5")

    written = sio.getvalue()
    assert written == textwrap.dedent("""\
        [my] Hello key1=value1
        [other.logger] Hello key1=value4
        [my.logger.sub] Hello key1=value5
        """)


def test_logger_respects_configured_level(structlog_config):
    with structlog_config(
        colors=False,
        log_format="[%(name)s] %(message)s",
        log_level="DEBUG",
    ) as sio:
        my_logger = logging.getLogger("my_logger")
        my_logger.debug("Debug message")

    written = sio.getvalue()
    assert "[my_logger] Debug message\n" in written


def test_excepthook_installed_when_json_output_true(structlog_config):
    import sys

    original = sys.excepthook
    try:
        with structlog_config(json_output=True):
            assert sys.excepthook is not original
    finally:
        sys.excepthook = original


def test_excepthook_not_installed_when_json_output_false(structlog_config):
    import sys

    original = sys.excepthook
    with structlog_config(json_output=False):
        assert sys.excepthook is original


def test_excepthook_routes_unhandled_exception_through_structlog(structlog_config):
    import sys

    original = sys.excepthook
    try:
        with structlog_config(json_output=True) as sio:
            sys.excepthook(ValueError, ValueError("boom"), None)
        output = sio.getvalue().decode()
        assert "unhandled_exception" in output
        assert "boom" in output
    finally:
        sys.excepthook = original


def test_excepthook_passes_keyboard_interrupt_to_original():
    import sys

    from airflow_shared.logging.structlog import _install_excepthook

    calls = []
    original = sys.excepthook

    def spy(et, ev, tb):
        calls.append(et)

    sys.excepthook = spy
    try:
        _install_excepthook()
        sys.excepthook(KeyboardInterrupt, KeyboardInterrupt(), None)
        assert calls == [KeyboardInterrupt]
    finally:
        sys.excepthook = original


class TestWarningsInterceptor:
    @pytest.fixture(autouse=True)
    def reset(self):
        from airflow_shared.logging.structlog import _WarningsInterceptor

        _WarningsInterceptor.reset()
        yield
        _WarningsInterceptor.reset()

    def test_register_replaces_showwarning(self):
        import warnings

        from airflow_shared.logging.structlog import _WarningsInterceptor

        current = warnings.showwarning
        sentinel = mock.MagicMock()
        _WarningsInterceptor.register(sentinel)
        assert warnings.showwarning is sentinel
        assert _WarningsInterceptor._original_showwarning is current

    def test_register_is_idempotent(self):
        import warnings

        from airflow_shared.logging.structlog import _WarningsInterceptor

        pre_register = warnings.showwarning
        _WarningsInterceptor.register(mock.MagicMock())
        _WarningsInterceptor.register(mock.MagicMock())
        assert _WarningsInterceptor._original_showwarning is pre_register

    def test_reset_restores_original(self):
        import warnings

        from airflow_shared.logging.structlog import _WarningsInterceptor

        pre_register = warnings.showwarning
        _WarningsInterceptor.register(mock.MagicMock())
        _WarningsInterceptor.reset()
        assert warnings.showwarning is pre_register
        assert _WarningsInterceptor._original_showwarning is None

    def test_reset_when_not_registered_is_noop(self):
        import warnings

        from airflow_shared.logging.structlog import _WarningsInterceptor

        pre_reset = warnings.showwarning
        _WarningsInterceptor.reset()
        assert warnings.showwarning is pre_reset

    def test_emit_warning_delegates_to_original(self):
        from airflow_shared.logging.structlog import _WarningsInterceptor

        sentinel = mock.MagicMock()
        _WarningsInterceptor._original_showwarning = sentinel
        _WarningsInterceptor.emit_warning("msg", UserWarning, "file.py", 1)
        sentinel.assert_called_once_with("msg", UserWarning, "file.py", 1)
        _WarningsInterceptor._original_showwarning = None

    def test_emit_warning_when_not_registered_is_noop(self):
        from airflow_shared.logging.structlog import _WarningsInterceptor

        _WarningsInterceptor._original_showwarning = None
        _WarningsInterceptor.emit_warning("msg", UserWarning, "file.py", 1)


class TestShowwarning:
    @pytest.fixture(autouse=True)
    def reset(self):
        from airflow_shared.logging.structlog import _WarningsInterceptor

        _WarningsInterceptor.reset()
        yield
        _WarningsInterceptor.reset()

    def test_with_file_delegates_to_original(self):
        from airflow_shared.logging.structlog import _showwarning, _WarningsInterceptor

        sentinel = mock.MagicMock()
        _WarningsInterceptor._original_showwarning = sentinel
        fake_file = mock.MagicMock()
        _showwarning("msg", UserWarning, "file.py", 42, file=fake_file)
        sentinel.assert_called_once_with("msg", UserWarning, "file.py", 42, fake_file, None)
        _WarningsInterceptor._original_showwarning = None

    def test_without_file_logs_to_structlog(self):
        from airflow_shared.logging.structlog import _showwarning

        with structlog.testing.capture_logs() as captured:
            _showwarning("deprecated feature", DeprecationWarning, "myfile.py", 10)

        assert len(captured) == 1
        event = captured[0]
        assert event["log_level"] == "warning"
        assert event["event"] == "deprecated feature"
        assert event["category"] == "DeprecationWarning"
        assert event["filename"] == "myfile.py"
        assert event["lineno"] == 10

    def test_without_file_uses_py_warnings_logger(self):
        from airflow_shared.logging import structlog as structlog_module
        from airflow_shared.logging.structlog import _showwarning

        with mock.patch.object(structlog_module.structlog, "get_logger") as mock_get_logger:
            mock_bound = mock.MagicMock()
            mock_bound.bind.return_value = mock_bound
            mock_get_logger.return_value = mock_bound
            with mock.patch.object(structlog_module, "reconfigure_logger", return_value=mock_bound):
                _showwarning("some warning", UserWarning, "foo.py", 1)

        mock_get_logger.assert_called_once_with("py.warnings")
