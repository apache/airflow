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
"""Internal API command."""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import textwrap
from contextlib import suppress
from tempfile import gettempdir
from time import sleep

import daemon
import psutil
from daemon.pidfile import TimeoutPIDLockFile
from flask import Flask
from flask_appbuilder import SQLA
from flask_caching import Cache
from flask_wtf.csrf import CSRFProtect
from lockfile.pidlockfile import read_pid_from_pidfile
from sqlalchemy.engine.url import make_url

from airflow import settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.cli.commands.webserver_command import GunicornMonitor
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.logging_config import configure_logging
from airflow.models import import_all_models
from airflow.utils import cli as cli_utils
from airflow.utils.cli import setup_locations, setup_logging
from airflow.utils.process_utils import check_if_pidfile_process_is_running
from airflow.www.extensions.init_dagbag import init_dagbag
from airflow.www.extensions.init_jinja_globals import init_jinja_globals
from airflow.www.extensions.init_manifest_files import configure_manifest_files
from airflow.www.extensions.init_security import init_xframe_protection
from airflow.www.extensions.init_views import init_api_internal, init_error_handlers

log = logging.getLogger(__name__)
app: Flask | None = None


@cli_utils.action_cli
def internal_api(args):
    """Starts Airflow Internal API."""
    print(settings.HEADER)

    access_logfile = args.access_logfile or "-"
    error_logfile = args.error_logfile or "-"
    access_logformat = args.access_logformat
    num_workers = args.workers
    worker_timeout = args.worker_timeout

    if args.debug:
        log.info(f"Starting the Internal API server on port {args.port} and host {args.hostname}.")
        app = create_app(testing=conf.getboolean("core", "unit_test_mode"))
        app.run(
            debug=True,
            use_reloader=not app.config["TESTING"],
            port=args.port,
            host=args.hostname,
        )
    else:
        pid_file, stdout, stderr, log_file = setup_locations(
            "internal-api", args.pid, args.stdout, args.stderr, args.log_file
        )

        # Check if Internal APi is already running if not, remove old pidfile
        check_if_pidfile_process_is_running(pid_file=pid_file, process_name="internal-api")

        log.info(
            textwrap.dedent(
                f"""\
                Running the Gunicorn Server with:
                Workers: {num_workers} {args.workerclass}
                Host: {args.hostname}:{args.port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile} {error_logfile}
                Access Logformat: {access_logformat}
                ================================================================="""
            )
        )

        run_args = [
            sys.executable,
            "-m",
            "gunicorn",
            "--workers",
            str(num_workers),
            "--worker-class",
            str(args.workerclass),
            "--timeout",
            str(worker_timeout),
            "--bind",
            args.hostname + ":" + str(args.port),
            "--name",
            "airflow-internal-api",
            "--pid",
            pid_file,
            "--access-logfile",
            str(access_logfile),
            "--error-logfile",
            str(error_logfile),
        ]

        if args.access_logformat and args.access_logformat.strip():
            run_args += ["--access-logformat", str(args.access_logformat)]

        if args.daemon:
            run_args += ["--daemon"]

        run_args += ["airflow.cli.commands.internal_api_command:cached_app()"]

        # To prevent different workers creating the web app and
        # all writing to the database at the same time, we use the --preload option.
        # With the preload option, the app is loaded before the workers are forked, and each worker will
        # then have a copy of the app
        run_args += ["--preload"]

        gunicorn_master_proc: psutil.Process | None = None

        def kill_proc(signum, _):
            log.info("Received signal: %s. Closing gunicorn.", signum)
            gunicorn_master_proc.terminate()
            with suppress(TimeoutError):
                gunicorn_master_proc.wait(timeout=30)
            if gunicorn_master_proc.is_running():
                gunicorn_master_proc.kill()
            sys.exit(0)

        def monitor_gunicorn(gunicorn_master_pid: int):
            # Register signal handlers
            signal.signal(signal.SIGINT, kill_proc)
            signal.signal(signal.SIGTERM, kill_proc)

            # These run forever until SIG{INT, TERM, KILL, ...} signal is sent
            GunicornMonitor(
                gunicorn_master_pid=gunicorn_master_pid,
                num_workers_expected=num_workers,
                master_timeout=120,
                worker_refresh_interval=30,
                worker_refresh_batch_size=1,
                reload_on_plugin_change=False,
            ).start()

        if args.daemon:
            # This makes possible errors get reported before daemonization
            os.environ["SKIP_DAGS_PARSING"] = "True"
            app = create_app(None)
            os.environ.pop("SKIP_DAGS_PARSING")

            handle = setup_logging(log_file)

            base, ext = os.path.splitext(pid_file)
            with open(stdout, "a") as stdout, open(stderr, "a") as stderr:
                stdout.truncate(0)
                stderr.truncate(0)

                ctx = daemon.DaemonContext(
                    pidfile=TimeoutPIDLockFile(f"{base}-monitor{ext}", -1),
                    files_preserve=[handle],
                    stdout=stdout,
                    stderr=stderr,
                    umask=int(settings.DAEMON_UMASK, 8),
                )
                with ctx:
                    subprocess.Popen(run_args, close_fds=True)

                    # Reading pid of gunicorn main process as it will be different that
                    # the one of process spawned above.
                    while True:
                        sleep(0.1)
                        gunicorn_master_proc_pid = read_pid_from_pidfile(pid_file)
                        if gunicorn_master_proc_pid:
                            break

                    # Run Gunicorn monitor
                    gunicorn_master_proc = psutil.Process(gunicorn_master_proc_pid)
                    monitor_gunicorn(gunicorn_master_proc.pid)

        else:
            with subprocess.Popen(run_args, close_fds=True) as gunicorn_master_proc:
                monitor_gunicorn(gunicorn_master_proc.pid)


def create_app(config=None, testing=False):
    """Create a new instance of Airflow Internal API app"""
    flask_app = Flask(__name__)

    flask_app.config["APP_NAME"] = "Airflow Internal API"
    flask_app.config["TESTING"] = testing
    flask_app.config["SQLALCHEMY_DATABASE_URI"] = conf.get("database", "SQL_ALCHEMY_CONN")

    url = make_url(flask_app.config["SQLALCHEMY_DATABASE_URI"])
    if url.drivername == "sqlite" and url.database and not url.database.startswith("/"):
        raise AirflowConfigException(
            f'Cannot use relative path: `{conf.get("database", "SQL_ALCHEMY_CONN")}` to connect to sqlite. '
            "Please use absolute path such as `sqlite:////tmp/airflow.db`."
        )

    flask_app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

    flask_app.config["SESSION_COOKIE_HTTPONLY"] = True
    flask_app.config["SESSION_COOKIE_SAMESITE"] = "Lax"

    if config:
        flask_app.config.from_mapping(config)

    if "SQLALCHEMY_ENGINE_OPTIONS" not in flask_app.config:
        flask_app.config["SQLALCHEMY_ENGINE_OPTIONS"] = settings.prepare_engine_args()

    InternalApiConfig.force_database_direct_access()

    csrf = CSRFProtect()
    csrf.init_app(flask_app)

    db = SQLA()
    db.session = settings.Session
    db.init_app(flask_app)

    init_dagbag(flask_app)

    cache_config = {"CACHE_TYPE": "flask_caching.backends.filesystem", "CACHE_DIR": gettempdir()}
    Cache(app=flask_app, config=cache_config)

    configure_logging()
    configure_manifest_files(flask_app)

    import_all_models()

    with flask_app.app_context():
        init_error_handlers(flask_app)
        init_api_internal(flask_app, standalone_api=True)

        init_jinja_globals(flask_app)
        init_xframe_protection(flask_app)
    return flask_app


def cached_app(config=None, testing=False):
    """Return cached instance of Airflow Internal API app"""
    global app
    if not app:
        app = create_app(config=config, testing=testing)
    return app
