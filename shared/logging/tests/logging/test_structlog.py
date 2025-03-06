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
import sys
import textwrap
from datetime import datetime, timezone

import pytest
import structlog
from structlog.dev import BLUE, BRIGHT, CYAN, DIM, GREEN, MAGENTA, RESET_ALL as RESET

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

            configure_logging(**kwargs, output=buff)
            yield buff
            buff.seek(0)
        finally:
            structlog.configure(**prev_config)

    return configurer


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
def test_colorful(structlog_config, get_logger, extra_kwargs, extra_output):
    with structlog_config(colors=True) as sio:
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
        log_format="%(blue)s[%(asctime)s]%(reset)s %(log_color)s%(levelname)s - %(message)s",
    ) as sio:
        logger = structlog.get_logger("my.logger")
        logger.info("Hello", key1="value1")

    written = sio.getvalue()
    assert written == "[1985-10-26T00:00:00.000001Z] INFO - Hello key1=value1\n"


@pytest.mark.parametrize(
    ("get_logger", "extra_kwargs"),
    [
        pytest.param(
            structlog.get_logger,
            {"key1": "value1"},
            id="structlog",
        ),
        pytest.param(
            logging.getLogger,
            {},
            id="stdlib",
        ),
    ],
)
def test_json(structlog_config, get_logger, extra_kwargs):
    with structlog_config(json_output=True) as bio:
        logger = get_logger("my.logger")
        logger.info("Hello", **extra_kwargs)

    written = json.load(bio)
    assert written == {
        "event": "Hello",
        "level": "info",
        **extra_kwargs,
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
    ("levels",),
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
        log_levels=levels,
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
