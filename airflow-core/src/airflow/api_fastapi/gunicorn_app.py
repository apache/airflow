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
"""
Gunicorn application with integrated worker monitoring.

This module provides a custom Gunicorn application that integrates worker
monitoring directly into the arbiter process loop. By subclassing the ``Arbiter``,
we can perform rolling worker restarts without needing a separate monitoring
thread or subprocess.

The pattern follows gunicorn's recommended extension approach:
- Subclass ``BaseApplication`` to configure gunicorn programmatically
- Override ``run()`` to use a custom ``Arbiter``
- Custom ``Arbiter`` hooks into manage_workers() for monitoring logic
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from typing import TYPE_CHECKING, Any

from gunicorn.app.base import BaseApplication
from gunicorn.arbiter import Arbiter

from airflow.configuration import conf

if TYPE_CHECKING:
    from fastapi import FastAPI
    from gunicorn.app.base import Application

log = logging.getLogger(__name__)


class AirflowArbiter(Arbiter):
    """Custom ``Arbiter`` with rolling worker restarts via manage_workers() hook."""

    def __init__(self, app: Application):
        super().__init__(app)

        # Worker refresh configuration
        self.worker_refresh_interval = conf.getint("api", "worker_refresh_interval", fallback=0)
        self.worker_refresh_batch_size = conf.getint("api", "worker_refresh_batch_size", fallback=1)

        # State tracking for rolling restarts
        self._last_refresh_time = time.monotonic()
        self._refresh_in_progress = False
        self._workers_to_replace: set[int] = set()

        # Validate configuration
        if self.worker_refresh_batch_size > self.num_workers:
            log.warning(
                "worker_refresh_batch_size (%d) > num_workers (%d), reducing batch size",
                self.worker_refresh_batch_size,
                self.num_workers,
            )
            self.worker_refresh_batch_size = self.num_workers

        if self.worker_refresh_interval > 0:
            log.info(
                "Worker refresh enabled: interval=%ds, batch_size=%d",
                self.worker_refresh_interval,
                self.worker_refresh_batch_size,
            )

    def manage_workers(self) -> None:
        """Maintain worker count and perform rolling restarts if configured."""
        super().manage_workers()

        # Check if worker refresh is enabled and due
        if self.worker_refresh_interval > 0:
            self._check_worker_refresh()

    def _check_worker_refresh(self) -> None:
        """Check if it's time to start or continue a worker refresh cycle."""
        elapsed = time.monotonic() - self._last_refresh_time

        if not self._refresh_in_progress:
            # Check if it's time to start a new refresh cycle
            if elapsed >= self.worker_refresh_interval:
                self._start_refresh_cycle()
        else:
            # Continue ongoing refresh cycle
            self._continue_refresh_cycle()

    def _start_refresh_cycle(self) -> None:
        """Start a new rolling worker refresh cycle."""
        if not self.WORKERS:
            return

        self._refresh_in_progress = True
        self._workers_to_replace = set(self.WORKERS.keys())
        log.info(
            "Starting worker refresh cycle: %d workers to replace",
            len(self._workers_to_replace),
        )
        self._continue_refresh_cycle()

    def _continue_refresh_cycle(self) -> None:
        """Continue rolling refresh: spawn new workers, kill old ones in batches."""
        # Remove workers that have already exited
        current_pids = set(self.WORKERS.keys())
        self._workers_to_replace &= current_pids

        if not self._workers_to_replace:
            # All original workers have been replaced
            log.info("Worker refresh cycle completed")
            self._refresh_in_progress = False
            self._last_refresh_time = time.monotonic()
            return

        # Check if we have capacity to spawn new workers
        # We temporarily exceed num_workers during rolling restart
        current_count = len(self.WORKERS)
        max_during_refresh = self.num_workers + self.worker_refresh_batch_size

        if current_count < max_during_refresh:
            # Spawn new workers up to batch size
            workers_to_spawn = min(
                self.worker_refresh_batch_size,
                max_during_refresh - current_count,
            )
            if workers_to_spawn > 0:
                log.debug("Spawning %d new worker(s) for refresh", workers_to_spawn)
                for _ in range(workers_to_spawn):
                    self.spawn_worker()

        # If we have more workers than target, kill old ones
        if current_count > self.num_workers:
            workers_to_kill = min(
                current_count - self.num_workers,
                self.worker_refresh_batch_size,
                len(self._workers_to_replace),
            )

            # Kill oldest workers first (FIFO)
            sorted_workers = sorted(
                [(pid, w) for pid, w in self.WORKERS.items() if pid in self._workers_to_replace],
                key=lambda x: x[1].age,
            )

            for pid, worker in sorted_workers[:workers_to_kill]:
                log.info("Killing old worker %d (age: %s) for refresh", pid, worker.age)
                self.kill_worker(pid, signal.SIGTERM)
                self._workers_to_replace.discard(pid)


class AirflowGunicornApp(BaseApplication):
    """Gunicorn application that uses AirflowArbiter for worker management."""

    def __init__(self, options: dict[str, Any] | None = None):
        self.options = options or {}
        self.application: FastAPI | None = None
        super().__init__()

    def load_config(self) -> None:
        """Load configuration from options dict into gunicorn config."""
        for key, value in self.options.items():
            if key in self.cfg.settings and value is not None:
                self.cfg.set(key.lower(), value)

    def load(self) -> Any:
        """Load and return the WSGI/ASGI application."""
        if self.application is None:
            from airflow.api_fastapi.main import app

            self.application = app
        return self.application

    def run(self) -> None:
        """Run the application with AirflowArbiter."""
        try:
            AirflowArbiter(self).run()
        except RuntimeError as e:
            print(f"\nError: {e}\n", file=sys.stderr)
            sys.stderr.flush()
            sys.exit(1)


def create_gunicorn_app(
    host: str,
    port: int,
    num_workers: int,
    worker_timeout: int,
    ssl_cert: str | None = None,
    ssl_key: str | None = None,
    access_log: bool = True,
    log_level: str = "info",
    proxy_headers: bool = False,
) -> AirflowGunicornApp:
    """
    Create a configured AirflowGunicornApp instance.

    :param host: Host to bind to
    :param port: Port to bind to
    :param num_workers: Number of worker processes
    :param worker_timeout: Worker timeout in seconds
    :param ssl_cert: Path to SSL certificate file
    :param ssl_key: Path to SSL key file
    :param access_log: Whether to enable access logging
    :param log_level: Log level (debug, info, warning, error, critical)
    :param proxy_headers: Whether to trust proxy headers
    """
    options = {
        "bind": f"{host}:{port}",
        "workers": num_workers,
        "worker_class": "uvicorn.workers.UvicornWorker",
        "timeout": worker_timeout,
        "graceful_timeout": worker_timeout,
        "keepalive": worker_timeout,
        "loglevel": log_level,
        "preload_app": True,
        # Use our gunicorn_config module for hooks (post_worker_init, worker_exit)
        "config": "python:airflow.api_fastapi.gunicorn_config",
    }

    if ssl_cert and ssl_key:
        options["certfile"] = ssl_cert
        options["keyfile"] = ssl_key

    if access_log:
        options["accesslog"] = "-"  # Log to stdout

    if proxy_headers:
        options["forwarded_allow_ips"] = "*"

    return AirflowGunicornApp(options)
