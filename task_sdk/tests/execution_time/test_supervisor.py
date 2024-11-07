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

import inspect
import logging
import os
import signal
import sys
from unittest.mock import MagicMock

import pytest
import structlog
import structlog.testing

import airflow.sdk.api.client
from airflow.sdk.api.datamodels.ti import TaskInstance
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.utils import timezone as tz


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


@pytest.mark.usefixtures("disable_capturing")
class TestWatchedSubprocess:
    def test_reading_from_pipes(self, captured_logs, time_machine):
        # Ignore anything lower than INFO for this test. Captured_logs resets things for us afterwards
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

        line = lineno()

        def subprocess_main():
            # This is run in the subprocess!

            # Flush calls are to ensure ordering of output for predictable tests
            import logging
            import warnings

            print("I'm a short message")
            sys.stdout.write("Message ")
            sys.stdout.write("split across two writes\n")
            sys.stdout.flush()

            print("stderr message", file=sys.stderr)
            sys.stderr.flush()

            logging.getLogger("airflow.foobar").error("An error message")

            warnings.warn("Warning should be captured too", stacklevel=1)

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id="a",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=airflow.sdk.api.client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 0
        assert captured_logs == [
            {
                "chan": "stdout",
                "event": "I'm a short message",
                "level": "info",
                "logger": "task",
                "timestamp": "2024-11-07T12:34:56.078901Z",
            },
            {
                "chan": "stdout",
                "event": "Message split across two writes",
                "level": "info",
                "logger": "task",
                "timestamp": "2024-11-07T12:34:56.078901Z",
            },
            {
                "chan": "stderr",
                "event": "stderr message",
                "level": "error",
                "logger": "task",
                "timestamp": "2024-11-07T12:34:56.078901Z",
            },
            {
                "event": "An error message",
                "level": "error",
                "logger": "airflow.foobar",
                "timestamp": instant.replace(tzinfo=None),
            },
            {
                "category": "UserWarning",
                "event": "Warning should be captured too",
                "filename": __file__,
                "level": "warning",
                "lineno": line + 19,
                "logger": "py.warnings",
                "timestamp": instant.replace(tzinfo=None),
            },
        ]

    def test_subprocess_sigkilled(self):
        main_pid = os.getpid()

        def subprocess_main():
            # This is run in the subprocess!
            assert os.getpid() != main_pid
            os.kill(os.getpid(), signal.SIGKILL)

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id="a",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=airflow.sdk.api.client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == -9
