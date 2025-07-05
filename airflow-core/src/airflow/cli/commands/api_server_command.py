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
import subprocess
import sys
import textwrap

import uvicorn

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.exceptions import AirflowConfigException
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


# This shouldn't be necessary but there seems to be an issue in uvloop that causes bad file descriptor
# errors when shutting down workers. Despite the 'closed' status of the issue it is not solved,
# more info here: https://github.com/benoitc/gunicorn/issues/1877#issuecomment-1911136399


def _run_api_server(
    args, apps: str, access_logfile: str, num_workers: int, worker_timeout: int, proxy_headers: bool
):
    """Run the API server."""
    log.info(
        textwrap.dedent(
            f"""\
            Running the uvicorn with:
            Apps: {apps}
            Workers: {num_workers}
            Host: {args.host}:{args.port}
            Timeout: {worker_timeout}
            Logfiles: {access_logfile}
            ================================================================="""
        )
    )
    # get ssl cert and key filepaths here instead of passing them as arguments to reduce the number of arguments
    ssl_cert, ssl_key = _get_ssl_cert_and_key_filepaths(args)

    # setproctitle causes issue on Mac OS: https://github.com/benoitc/gunicorn/issues/3021
    os_type = sys.platform
    if os_type == "darwin":
        log.debug("Mac OS detected, skipping setproctitle")
    else:
        from setproctitle import setproctitle

        setproctitle(f"airflow api_server -- host:{args.host} port:{args.port}")

    uvicorn.run(
        "airflow.api_fastapi.main:app",
        host=args.host,
        port=args.port,
        workers=num_workers,
        timeout_keep_alive=worker_timeout,
        timeout_graceful_shutdown=worker_timeout,
        ssl_keyfile=ssl_key,
        ssl_certfile=ssl_cert,
        access_log=access_logfile,  # type: ignore[arg-type]
        proxy_headers=proxy_headers,
    )


@cli_utils.action_cli
@providers_configuration_loaded
def api_server(args):
    """Start Airflow API server."""
    print(settings.HEADER)

    apps = args.apps
    access_logfile = args.access_logfile or "-"
    num_workers = args.workers
    worker_timeout = args.worker_timeout
    proxy_headers = args.proxy_headers

    # Ensure we set this now, so that each subprocess gets the same value
    from airflow.api_fastapi.auth.tokens import get_signing_args

    get_signing_args()

    if args.dev:
        print(f"Starting the API server on port {args.port} and host {args.host} in development mode.")
        log.warning("Running in dev mode, ignoring uvicorn args")

        run_args = [
            "fastapi",
            "dev",
            "airflow-core/src/airflow/api_fastapi/main.py",
            "--port",
            str(args.port),
            "--host",
            str(args.host),
        ]

        if args.proxy_headers:
            run_args.append("--proxy-headers")

        # There is no way to pass the apps to airflow/api_fastapi/main.py in the development mode
        # because fastapi dev command does not accept any additional arguments
        # so environment variable is being used to pass it
        os.environ["AIRFLOW_API_APPS"] = apps
        with subprocess.Popen(
            run_args,
            close_fds=True,
        ) as process:
            process.wait()
        os.environ.pop("AIRFLOW_API_APPS")
    else:
        # We leave the logs here intentionally, since run_command_with_daemon_option will first daemonize the process, then run the callback
        if args.daemon:
            log.info("Daemonized the API server process PID: %s", os.getpid())

        run_command_with_daemon_option(
            args=args,
            process_name="api_server",
            callback=lambda: _run_api_server(
                args=args,
                apps=apps,
                access_logfile=access_logfile,
                num_workers=num_workers,
                worker_timeout=worker_timeout,
                proxy_headers=proxy_headers,
            ),
            should_setup_logging=False,
        )


def _get_ssl_cert_and_key_filepaths(cli_arguments) -> tuple[str | None, str | None]:
    error_template_1 = "Need both, have provided {} but not {}"
    error_template_2 = "SSL related file does not exist {}"

    ssl_cert, ssl_key = cli_arguments.ssl_cert, cli_arguments.ssl_key
    if ssl_cert and ssl_key:
        if not os.path.isfile(ssl_cert):
            raise AirflowConfigException(error_template_2.format(ssl_cert))
        if not os.path.isfile(ssl_key):
            raise AirflowConfigException(error_template_2.format(ssl_key))

        return (ssl_cert, ssl_key)
    if ssl_cert:
        raise AirflowConfigException(error_template_1.format("SSL certificate", "SSL key"))
    if ssl_key:
        raise AirflowConfigException(error_template_1.format("SSL key", "SSL certificate"))

    return (None, None)
