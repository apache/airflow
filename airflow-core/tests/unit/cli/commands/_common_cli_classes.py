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

import os
import re
import time
from contextlib import suppress

import psutil
import pytest
from psutil import Error, NoSuchProcess
from rich.console import Console

from airflow.cli import cli_parser
from airflow.utils.cli import setup_locations

console = Console(width=400, color_system="standard")


class _CommonCLIGunicornTestClass:
    main_process_regexp: str = "process_to_look_for"

    @pytest.fixture(autouse=True)
    def _make_parser(self):
        self.parser = cli_parser.get_parser()

    def _check_processes(self, ignore_running: bool):
        if self.main_process_regexp == "process_to_look_for":
            raise Exception(
                "The main_process_regexp must be set in the subclass to something different than"
                " 'process_to_look_for'"
            )
        # Confirm that nmain procss hasn't been launched.
        # pgrep returns exit status 1 if no process matched.
        # Use more specific regexps (^) to avoid matching pytest run when running specific method.
        # For instance, we want to be able to do: pytest -k 'gunicorn'
        airflow_internal_api_pids = self._find_all_processes(self.main_process_regexp)
        gunicorn_pids = self._find_all_processes(r"gunicorn: ")
        if airflow_internal_api_pids or gunicorn_pids:
            console.print("[blue]Some processes are still running")
            for pid in gunicorn_pids + airflow_internal_api_pids:
                with suppress(NoSuchProcess):
                    console.print(psutil.Process(pid).as_dict(attrs=["pid", "name", "cmdline"]))
            console.print("[blue]Here list of processes ends")
            if airflow_internal_api_pids:
                console.print(f"[yellow]Forcefully killing {self.main_process_regexp} processes")
                for pid in airflow_internal_api_pids:
                    with suppress(NoSuchProcess):
                        psutil.Process(pid).kill()
            if gunicorn_pids:
                console.print("[yellow]Forcefully killing all gunicorn processes")
                for pid in gunicorn_pids:
                    with suppress(NoSuchProcess):
                        psutil.Process(pid).kill()
            if not ignore_running:
                raise AssertionError(
                    "Background processes are running that prevent the test from passing successfully."
                )

    @pytest.fixture(autouse=True)
    def _cleanup(self):
        self._check_processes(ignore_running=True)
        self._clean_pidfiles()

        yield

        self._check_processes(ignore_running=True)
        self._clean_pidfiles()

    def _clean_pidfiles(self):
        pidfile_internal_api = setup_locations("internal-api")[0]
        pidfile_monitor = setup_locations("internal-api-monitor")[0]
        if os.path.exists(pidfile_internal_api):
            console.print(f"[blue]Removing pidfile{pidfile_internal_api}")
            os.remove(pidfile_internal_api)
        if os.path.exists(pidfile_monitor):
            console.print(f"[blue]Removing pidfile{pidfile_monitor}")
            os.remove(pidfile_monitor)

    def _wait_pidfile(self, pidfile):
        start_time = time.monotonic()
        while True:
            try:
                with open(pidfile) as file:
                    return int(file.read())
            except Exception:
                if start_time - time.monotonic() > 60:
                    raise
                console.print(f"[blue]Waiting for pidfile {pidfile} to be created ...")
                time.sleep(1)

    def _find_process(self, regexp_match: str, print_found_process=False) -> int | None:
        """
        Find if process is running by matching its command line with a regexp.
        :param regexp_match: regexp to match the command line of the process
        :param print_found_process: if True, print the process found
        :return: PID of the process if found, None otherwise
        """
        matcher = re.compile(regexp_match)
        for proc in psutil.process_iter():
            try:
                proc_cmdline = " ".join(proc.cmdline())
            except Error:
                # only check processes we can access and are existing
                continue
            if matcher.search(proc_cmdline):
                if print_found_process:
                    console.print(proc.as_dict(attrs=["pid", "name", "cmdline"]))
                return proc.pid
        return None

    def _find_all_processes(self, regexp_match: str, print_found_process=False) -> list[int]:
        """
        Find all running process matching their command line with a regexp and return the list of pids
        of the processes. found
        :param regexp_match: regexp to match the command line of the processes
        :param print_found_process: if True, print the processes found
        :return: list of PID of the processes matching the regexp
        """
        matcher = re.compile(regexp_match)
        pids: list[int] = []
        for proc in psutil.process_iter():
            try:
                proc_cmdline = " ".join(proc.cmdline())
            except Error:
                # only check processes we can access and are existing
                continue
            if matcher.match(proc_cmdline):
                if print_found_process:
                    console.print(proc.as_dict(attrs=["pid", "name", "cmdline"]))
                pids.append(proc.pid)
        return pids

    def _terminate_multiple_process(self, pid_list):
        process = []
        for pid in pid_list:
            proc = psutil.Process(pid)
            proc.terminate()
            process.append(proc)
        gone, alive = psutil.wait_procs(process, timeout=120)
        for p in alive:
            p.kill()
