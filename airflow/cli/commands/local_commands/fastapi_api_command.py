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
import textwrap

import uvicorn
from gunicorn.util import daemonize
from setproctitle import setproctitle

from airflow import settings
from airflow.exceptions import AirflowConfigException
from airflow.utils import cli as cli_utils
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

log = logging.getLogger(__name__)


# This shouldn't be necessary but there seems to be an issue in uvloop that causes bad file descriptor
# errors when shutting down workers. Despite the 'closed' status of the issue it is not solved,
# more info here: https://github.com/benoitc/gunicorn/issues/1877#issuecomment-1911136399


@cli_utils.action_cli
@providers_configuration_loaded
def fastapi_api(args):
    """Start Airflow FastAPI API."""
    print(settings.HEADER)

    apps = args.apps
    access_logfile = args.access_logfile or "-"
    access_logformat = args.access_logformat
    num_workers = args.workers
    worker_timeout = args.worker_timeout
    proxy_headers = args.proxy_headers

    if args.debug:
        print(f"Starting the FastAPI API server on port {args.port} and host {args.hostname} debug.")
        log.warning("Running in dev mode, ignoring uvicorn args")

        run_args = [
            "fastapi",
            "dev",
            "airflow/api_fastapi/main.py",
            "--port",
            str(args.port),
            "--host",
            str(args.hostname),
        ]

        if args.proxy_headers:
            run_args.append("--proxy-headers")

        # There is no way to pass the apps to airflow/api_fastapi/main.py in the debug mode
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
        if args.daemon:
            daemonize()
            log.info("Daemonized the FastAPI API server process PID: %s", os.getpid())

        log.info(
            textwrap.dedent(
                f"""\
                Running the uvicorn with:
                Apps: {apps}
                Workers: {num_workers}
                Host: {args.hostname}:{args.port}
                Timeout: {worker_timeout}
                Logfiles: {access_logfile}
                Access Logformat: {access_logformat}
                ================================================================="""
            )
        )
        ssl_cert, ssl_key = _get_ssl_cert_and_key_filepaths(args)
        setproctitle(f"airflow fastapi_api -- host:{args.hostname} port:{args.port}")
        uvicorn.run(
            "airflow.api_fastapi.main:app",
            host=args.hostname,
            port=args.port,
            workers=num_workers,
            timeout_keep_alive=worker_timeout,
            timeout_graceful_shutdown=worker_timeout,
            ssl_keyfile=ssl_key,
            ssl_certfile=ssl_cert,
            access_log=access_logfile,
            proxy_headers=proxy_headers,
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
    elif ssl_cert:
        raise AirflowConfigException(error_template_1.format("SSL certificate", "SSL key"))
    elif ssl_key:
        raise AirflowConfigException(error_template_1.format("SSL key", "SSL certificate"))

    return (None, None)
