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
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import MagicMock
from uuid import UUID

import pytest
import structlog
import structlog.testing

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.supervisor import WatchedSubprocess
from airflow.utils import timezone as tz

if TYPE_CHECKING:
    import kgb


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

            # Ensure we follow the "protocol" and get the startup message before we do anything
            sys.stdin.readline()

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
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
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
                "lineno": line + 22,
                "logger": "py.warnings",
                "timestamp": instant.replace(tzinfo=None),
            },
        ]

    def test_subprocess_sigkilled(self):
        main_pid = os.getpid()

        def subprocess_main():
            # Ensure we follow the "protocol" and get the startup message before we do anything
            sys.stdin.readline()

            assert os.getpid() != main_pid
            os.kill(os.getpid(), signal.SIGKILL)

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == -9

    def test_regular_heartbeat(self, spy_agency: kgb.SpyAgency, monkeypatch):
        """Test that the WatchedSubprocess class regularly sends heartbeat requests, up to a certain frequency"""
        import airflow.sdk.execution_time.supervisor

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "FASTEST_HEARTBEAT_INTERVAL", 0.1)

        def subprocess_main():
            sys.stdin.readline()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        id = UUID("4d828a62-a417-4936-a7a6-2b3fabacecab")
        spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id=id,
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=sdk_client.Client(base_url="", dry_run=True, token=""),
            target=subprocess_main,
        )
        assert proc.wait() == 0
        assert spy.called_with(id, pid=proc.pid)  # noqa: PGH005
        # The exact number we get will depend on timing behaviour, so be a little lenient
        assert 2 <= len(spy.calls) <= 4
