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
"""FastAPI API command."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import textwrap
from contextlib import suppress
from pathlib import Path
from time import sleep
from typing import NoReturn

import psutil
from lockfile.pidlockfile import read_pid_from_pidfile
from uvicorn.workers import UvicornWorker

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.cli.commands.webserver_command import GunicornMonitor
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


# This shouldn't be necessary but there seems to be an issue in uvloop that causes bad file descriptor
# errors when shutting down workers. Despite the 'closed' status of the issue it is not solved,
# more info here: https://github.com/benoitc/gunicorn/issues/1877#issuecomment-1911136399
AirflowUvicornWorker = UvicornWorker
AirflowUvicornWorker.CONFIG_KWARGS = {"loop": "asyncio", "http": "auto"}


@cli_utils.action_cli
@providers_configuration_loaded
def fastapi_api(args):
    """Start Airflow FastAPI API."""
    print(settings.HEADER)

    access_logfile = args.access_logfile or "-"
    error_logfile = args.error_logfile or "-"
    access_logformat = args.access_logformat
    num_workers = args.workers
    worker_timeout = args.worker_timeout

    worker_class = "airflow.cli.commands.fastapi_api_command.AirflowUvicornWorker"

    from airflow.api_fastapi.app import create_app

    if args.debug:
        print(f"Starting the FastAPI API server on port {args.port} and host {args.hostname} debug.")
        log.warning("Running in dev mode, ignoring gunicorn args")

        run_args = [
            "fastapi",
            "dev",
            "airflow/api_fastapi/main.py",
            "--port",
            str(args.port),
            "--host",
            str(args.hostname),
        ]

        with subprocess.Popen(
            run_args,
            close_fds=True,
        ) as process:
            process.wait()
    else:
        log.info(
            textwrap.dedent(
                f"""\
                Running the Gunicorn Server with:
                Workers: {num_workers} {worker_class}
                Host: {args.hostname}:{args.port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                Access Logformat: {access_logformat}
                ================================================================="""
            )
        )

        pid_file, _, _, _ = setup_locations("fastapi-api", pid=args.pid)
        run_args = [
            sys.executable,
            "-m",
            "gunicorn",
            "--workers",
            str(num_workers),
            "--worker-class",
            str(worker_class),
            "--timeout",
            str(worker_timeout),
            "--bind",
            args.hostname + ":" + str(args.port),
            "--name",
            "airflow-fastapi-api",
            "--pid",
            pid_file,
            "--access-logfile",
            str(access_logfile),
            "--error-logfile",
            str(error_logfile),
            "--config",
            "python:airflow.api_fastapi.gunicorn_config",
        ]

        if args.access_logformat and args.access_logformat.strip():
            run_args += ["--access-logformat", str(args.access_logformat)]

        if args.daemon:
            run_args += ["--daemon"]

        run_args += ["airflow.api_fastapi.app:cached_app()"]

        # To prevent different workers creating the web app and
        # all writing to the database at the same time, we use the --preload option.
        # With the preload option, the app is loaded before the workers are forked, and each worker will
        # then have a copy of the app
        run_args += ["--preload"]

        def kill_proc(signum: int, gunicorn_master_proc: psutil.Process | subprocess.Popen) -> NoReturn:
            log.info("Received signal: %s. Closing gunicorn.", signum)
            gunicorn_master_proc.terminate()
            with suppress(TimeoutError):
                gunicorn_master_proc.wait(timeout=30)
            if isinstance(gunicorn_master_proc, subprocess.Popen):
                still_running = gunicorn_master_proc.poll() is not None
            else:
                still_running = gunicorn_master_proc.is_running()
            if still_running:
                gunicorn_master_proc.kill()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_proc: psutil.Process | subprocess.Popen) -> NoReturn:
            # Register signal handlers
            signal.signal(signal.SIGINT, lambda signum, _: kill_proc(signum, gunicorn_master_proc))
            signal.signal(signal.SIGTERM, lambda signum, _: kill_proc(signum, gunicorn_master_proc))

            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            GunicornMonitor(
                gunicorn_master_pid=gunicorn_master_proc.pid,
                num_workers_expected=num_workers,
                master_timeout=120,
                worker_refresh_interval=30,
                worker_refresh_batch_size=1,
                reload_on_plugin_change=False,
            ).start()

        def start_and_monitor_gunicorn(args):
            if args.daemon:
                subprocess.Popen(run_args, close_fds=True)

                # Reading pid of gunicorn master as it will be different that
                # the one of process spawned above.
                gunicorn_master_proc_pid = None
                while not gunicorn_master_proc_pid:
                    sleep(0.1)
                    gunicorn_master_proc_pid = read_pid_from_pidfile(pid_file)

                # Run Gunicorn monitor
                gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                monitor_gunicorn(gunicorn_master_proc)
            else:
                with subprocess.Popen(run_args, close_fds=True) as gunicorn_master_proc:
                    monitor_gunicorn(gunicorn_master_proc)

        if args.daemon:
            # This makes possible errors get reported before daemonization
            os.environ["SKIP_DAGS_PARSING"] = "True"
            create_app()
            os.environ.pop("SKIP_DAGS_PARSING")

        pid_file_path = Path(pid_file)
        monitor_pid_file = str(pid_file_path.with_name(f"{pid_file_path.stem}-monitor{pid_file_path.suffix}"))
        run_command_with_daemon_option(
            args=args,
            process_name="fastapi-api",
            callback=lambda: start_and_monitor_gunicorn(args),
            should_setup_logging=True,
            pid_file=monitor_pid_file,
        )
