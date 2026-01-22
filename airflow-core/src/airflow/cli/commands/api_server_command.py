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
import time
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING, TypeVar

import uvicorn

from airflow import settings
from airflow.cli.commands.daemon_utils import run_command_with_daemon_option
from airflow.configuration import conf
from airflow.exceptions import AirflowConfigException
from airflow.typing_compat import ParamSpec
from airflow.utils import cli as cli_utils
from airflow.utils.memray_utils import MemrayTraceComponents, enable_memray_trace
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

PS = ParamSpec("PS")
RT = TypeVar("RT")

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from argparse import Namespace

# This shouldn't be necessary but there seems to be an issue in uvloop that causes bad file descriptor
# errors when shutting down workers. Despite the 'closed' status of the issue it is not solved,
# more info here: https://github.com/benoitc/gunicorn/issues/1877#issuecomment-1911136399


def _build_uvicorn_command(
    host: str,
    port: int,
    num_workers: int,
    worker_timeout: int,
    ssl_key: str | None,
    ssl_cert: str | None,
    access_log_enabled: bool,
    uvicorn_log_level: str,
    proxy_headers: bool,
    log_config: str | None,
) -> list[str]:
    """Build the uvicorn command line arguments."""
    cmd = [
        sys.executable,
        "-m",
        "uvicorn",
        "airflow.api_fastapi.main:app",
        "--host",
        host,
        "--port",
        str(port),
        "--workers",
        str(num_workers),
        "--timeout-keep-alive",
        str(worker_timeout),
        "--timeout-graceful-shutdown",
        str(worker_timeout),
        "--log-level",
        uvicorn_log_level,
    ]

    if ssl_key:
        cmd.extend(["--ssl-keyfile", ssl_key])
    if ssl_cert:
        cmd.extend(["--ssl-certfile", ssl_cert])
    if not access_log_enabled:
        cmd.append("--no-access-log")
    if proxy_headers:
        cmd.append("--proxy-headers")
    if log_config:
        cmd.extend(["--log-config", log_config])

    return cmd


def _run_api_server_with_monitor(
    args: Namespace,
    num_workers: int,
    worker_timeout: int,
    ssl_key: str | None,
    ssl_cert: str | None,
    access_log_enabled: bool,
    uvicorn_log_level: str,
    proxy_headers: bool,
    log_config: str | None,
    worker_refresh_interval: int,
    worker_refresh_batch_size: int,
):
    """Run the API server with UvicornMonitor for rolling restarts."""
    import psutil

    from airflow.cli.commands.uvicorn_monitor import UvicornMonitor

    # uvicorn needs at least 2 workers to run in multiprocess mode (supervisor + workers).
    # With workers=1, it runs single-process and SIGTTIN/SIGTTOU don't work.
    # So we start with max(2, num_workers) and scale down to num_workers after startup.
    initial_workers = max(2, num_workers)
    need_scale_down = initial_workers > num_workers

    cmd = _build_uvicorn_command(
        host=args.host,
        port=args.port,
        num_workers=initial_workers,
        worker_timeout=worker_timeout,
        ssl_key=ssl_key,
        ssl_cert=ssl_cert,
        access_log_enabled=access_log_enabled,
        uvicorn_log_level=uvicorn_log_level,
        proxy_headers=proxy_headers,
        log_config=log_config,
    )

    log.info("Starting uvicorn with monitor: %s", " ".join(cmd))

    # Start uvicorn as a subprocess
    uvicorn_proc = subprocess.Popen(cmd)

    # Give uvicorn time to fail if there's an immediate issue (e.g., port in use, import error)
    time.sleep(2)
    if uvicorn_proc.poll() is not None:
        log.error("Uvicorn failed to start, exit code: %d", uvicorn_proc.returncode)
        sys.exit(1)

    # Set up signal handler to forward signals to uvicorn
    def signal_handler(signum, frame):
        log.info("Received signal %d, forwarding to uvicorn", signum)
        uvicorn_proc.send_signal(signum)
        if signum in (signal.SIGTERM, signal.SIGINT):
            try:
                uvicorn_proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                log.warning("Uvicorn did not terminate in 30s, sending SIGKILL")
                uvicorn_proc.kill()
                uvicorn_proc.wait()
            sys.exit(0)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Build health check URL (use 127.0.0.1 when binding to 0.0.0.0 since that's not routable)
    health_check_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    health_check_url = f"http://{health_check_host}:{args.port}/api/v2/version"

    # If we started with more workers than needed (to enable multiprocess mode),
    # scale down to the requested count after workers are ready
    if need_scale_down:
        log.info("Scaling down from %d to %d workers", initial_workers, num_workers)
        # Wait for workers to be ready
        uvicorn_parent = psutil.Process(uvicorn_proc.pid)
        for _ in range(60):  # Wait up to 30 seconds
            if len(uvicorn_parent.children()) >= initial_workers:
                break
            time.sleep(0.5)
        # Scale down by sending SIGTTOU for each extra worker
        for _ in range(initial_workers - num_workers):
            uvicorn_proc.send_signal(signal.SIGTTOU)
            time.sleep(0.5)  # Give time for worker to terminate

    # Start the monitor
    monitor = UvicornMonitor(
        uvicorn_parent_pid=uvicorn_proc.pid,
        num_workers_expected=num_workers,
        worker_refresh_interval=worker_refresh_interval,
        worker_refresh_batch_size=worker_refresh_batch_size,
        health_check_url=health_check_url,
    )

    try:
        monitor.start()
    except KeyboardInterrupt:
        log.info("Received keyboard interrupt, shutting down")
        uvicorn_proc.terminate()
        uvicorn_proc.wait()


@enable_memray_trace(component=MemrayTraceComponents.api)
def _run_api_server(args, apps: str, num_workers: int, worker_timeout: int, proxy_headers: bool):
    """Run the API server."""
    log.info(
        textwrap.dedent(
            f"""\
            Running the uvicorn with:
            Apps: {apps}
            Workers: {num_workers}
            Host: {args.host}:{args.port}
            Timeout: {worker_timeout}
            Logfiles: {args.log_file or "-"}
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

    # Get uvicorn logging configuration from Airflow settings
    uvicorn_log_level = conf.get("logging", "uvicorn_logging_level", fallback="info").lower()
    # Control access log based on uvicorn log level - disable for ERROR and above
    access_log_enabled = uvicorn_log_level not in ("error", "critical", "fatal")

    # Rolling worker refresh configuration
    worker_refresh_interval = conf.getint("api", "worker_refresh_interval", fallback=0)
    worker_refresh_batch_size = conf.getint("api", "worker_refresh_batch_size", fallback=1)

    # Get log config if provided
    log_config = None
    if args.log_config and args.log_config != "-":
        log_config = args.log_config

    # If rolling worker refresh is enabled, use the monitor-based approach
    if worker_refresh_interval > 0:
        log.info(
            "Worker refresh enabled: interval=%ds, batch_size=%d",
            worker_refresh_interval,
            worker_refresh_batch_size,
        )
        _run_api_server_with_monitor(
            args=args,
            num_workers=num_workers,
            worker_timeout=worker_timeout,
            ssl_key=ssl_key,
            ssl_cert=ssl_cert,
            access_log_enabled=access_log_enabled,
            uvicorn_log_level=uvicorn_log_level,
            proxy_headers=proxy_headers,
            log_config=log_config,
            worker_refresh_interval=worker_refresh_interval,
            worker_refresh_batch_size=worker_refresh_batch_size,
        )
        return

    uvicorn_kwargs = {
        "host": args.host,
        "port": args.port,
        "workers": num_workers,
        "timeout_keep_alive": worker_timeout,
        "timeout_graceful_shutdown": worker_timeout,
        "timeout_worker_healthcheck": worker_timeout,
        "ssl_keyfile": ssl_key,
        "ssl_certfile": ssl_cert,
        "access_log": access_log_enabled,
        "log_level": uvicorn_log_level,
        "proxy_headers": proxy_headers,
    }
    # Only set the log_config if it is provided, otherwise use the default uvicorn logging configuration.
    if log_config:
        uvicorn_kwargs["log_config"] = log_config

    uvicorn.run(
        "airflow.api_fastapi.main:app",
        **uvicorn_kwargs,
    )


def with_api_apps_env(func: Callable[[Namespace], RT]) -> Callable[[Namespace], RT]:
    """We use AIRFLOW_API_APPS to specify which apps are initialized in the API server."""

    @wraps(func)
    def wrapper(args: Namespace) -> RT:
        apps: str = args.apps
        original_value = os.environ.get("AIRFLOW_API_APPS")
        try:
            log.debug("Setting AIRFLOW_API_APPS to: %s", apps)
            os.environ["AIRFLOW_API_APPS"] = apps
            return func(args)
        finally:
            if original_value is not None:
                os.environ["AIRFLOW_API_APPS"] = original_value
                log.debug("Restored AIRFLOW_API_APPS to: %s", original_value)
            else:
                os.environ.pop("AIRFLOW_API_APPS", None)
                log.debug("Removed AIRFLOW_API_APPS from environment")

    return wrapper


@cli_utils.action_cli
@providers_configuration_loaded
@with_api_apps_env
def api_server(args: Namespace):
    """Start Airflow API server."""
    print(settings.HEADER)

    apps = args.apps
    num_workers = args.workers
    worker_timeout = args.worker_timeout
    proxy_headers = args.proxy_headers

    # Ensure we set this now, so that each subprocess gets the same value
    from airflow.api_fastapi.auth.tokens import get_signing_args

    get_signing_args()

    if cli_utils.should_enable_hot_reload(args):
        print(f"Starting the API server on port {args.port} and host {args.host} in development mode.")
        log.warning("Running in dev mode, ignoring uvicorn args")
        from fastapi_cli.cli import _run

        _run(
            entrypoint="airflow.api_fastapi.main:app",
            port=args.port,
            host=args.host,
            reload=True,
            proxy_headers=args.proxy_headers,
            command="dev",
        )
        return

    run_command_with_daemon_option(
        args=args,
        process_name="api_server",
        callback=lambda: _run_api_server(
            args=args,
            apps=apps,
            num_workers=num_workers,
            worker_timeout=worker_timeout,
            proxy_headers=proxy_headers,
        ),
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
