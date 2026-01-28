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
Gunicorn worker monitor for zero-downtime worker recycling.

This module provides the GunicornMonitor class that monitors a gunicorn master
process and performs rolling worker restarts to prevent memory accumulation
while maintaining service availability.

Unlike uvicorn's multiprocess mode:
- Gunicorn always has a master/worker architecture (even with workers=1)
- SIGTTOU kills the oldest worker (FIFO), not newest (LIFO)
- Well-tested pattern from Airflow 2's webserver GunicornMonitor

The rolling restart pattern:
1. Spawn batch_size new workers (SIGTTIN)
2. Wait for new workers to become ready (process title check)
3. HTTP health check to verify workers can serve requests
4. Kill batch_size old workers (SIGTTOU - kills oldest)
5. Repeat until all workers have been refreshed
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
import sys
import time
from glob import glob
from pathlib import Path

import psutil

from airflow import settings
from airflow.configuration import conf

log = logging.getLogger(__name__)


class GunicornMonitor:
    """
    Monitor gunicorn master process and perform rolling worker restarts.

    This class runs in the main process and monitors the gunicorn master
    process. It periodically restarts workers using a rolling restart
    pattern to prevent memory accumulation.

    The monitor uses:
    - SIGTTIN to spawn new workers
    - SIGTTOU to kill old workers (kills oldest - FIFO order)
    - HTTP health checks to verify workers are ready
    - Process title inspection to track worker readiness

    :param gunicorn_master_pid: PID of the gunicorn master process
    :param num_workers_expected: Expected number of worker processes
    :param worker_refresh_interval: Seconds between worker refresh cycles (0 = disabled)
    :param worker_refresh_batch_size: Number of workers to refresh at a time
    :param reload_on_plugin_change: Whether to reload when plugins change
    :param health_check_url: URL for HTTP health check
    """

    def __init__(
        self,
        gunicorn_master_pid: int,
        num_workers_expected: int,
        worker_refresh_interval: int,
        worker_refresh_batch_size: int,
        reload_on_plugin_change: bool,
        health_check_url: str | None = None,
    ):
        self.gunicorn_master_pid = gunicorn_master_pid
        self.num_workers_expected = num_workers_expected
        self.worker_refresh_interval = worker_refresh_interval
        self.worker_refresh_batch_size = worker_refresh_batch_size
        self.reload_on_plugin_change = reload_on_plugin_change
        self.health_check_url = health_check_url

        self._gunicorn_master_proc: psutil.Process | None = None
        self._last_refresh_time = time.monotonic()
        self._last_plugin_state: str | None = None
        self._should_stop = False

        # Validate configuration
        if self.worker_refresh_batch_size > self.num_workers_expected:
            log.warning(
                "worker_refresh_batch_size (%d) is greater than num_workers_expected (%d), "
                "reducing batch size to match worker count",
                self.worker_refresh_batch_size,
                self.num_workers_expected,
            )
            self.worker_refresh_batch_size = self.num_workers_expected

    def _get_gunicorn_master_proc(self) -> psutil.Process:
        """Get or create the gunicorn master process handle."""
        if self._gunicorn_master_proc is None:
            self._gunicorn_master_proc = psutil.Process(self.gunicorn_master_pid)
        return self._gunicorn_master_proc

    def _get_num_workers_running(self) -> int:
        """Get the number of currently running worker processes."""
        try:
            proc = self._get_gunicorn_master_proc()
            return len(proc.children())
        except psutil.NoSuchProcess:
            return 0

    def _get_worker_pids(self) -> list[int]:
        """Get list of PIDs of all worker processes."""
        try:
            proc = self._get_gunicorn_master_proc()
            return [child.pid for child in proc.children()]
        except psutil.NoSuchProcess:
            return []

    def _get_num_ready_workers(self) -> int:
        """
        Get the number of workers that have signaled they are ready.

        Workers set their process title to include GUNICORN_WORKER_READY_PREFIX
        when they have finished initialization and are ready to serve requests.

        On macOS, setproctitle doesn't work reliably, so we fall back to counting
        all running workers as ready.
        """
        # On macOS, setproctitle doesn't work, so assume all workers are ready
        if sys.platform == "darwin":
            return self._get_num_workers_running()

        ready_prefix = settings.GUNICORN_WORKER_READY_PREFIX
        ready_count = 0
        try:
            proc = self._get_gunicorn_master_proc()
            for child in proc.children():
                try:
                    # cmdline() returns the process title set via setproctitle
                    cmdline = " ".join(child.cmdline())
                    if ready_prefix in cmdline:
                        ready_count += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
        except psutil.NoSuchProcess:
            pass
        return ready_count

    def _spawn_new_workers(self, count: int) -> None:
        """
        Spawn new worker processes by sending SIGTTIN to master.

        SIGTTIN tells gunicorn to add a worker process. We send this
        signal count times to spawn count new workers.
        """
        log.info("Spawning %d new worker(s)", count)
        try:
            for _ in range(count):
                os.kill(self.gunicorn_master_pid, signal.SIGTTIN)
                # Small delay to allow signal processing
                time.sleep(0.1)
        except OSError as e:
            log.error("Failed to send SIGTTIN to gunicorn master: %s", e)
            raise

    def _kill_old_workers(self, count: int) -> None:
        """
        Kill old worker processes by sending SIGTTOU to master.

        SIGTTOU tells gunicorn to remove a worker process. Gunicorn
        removes the oldest worker (FIFO order), which is what we want
        for rolling restarts.
        """
        log.info("Killing %d old worker(s)", count)
        try:
            for _ in range(count):
                os.kill(self.gunicorn_master_pid, signal.SIGTTOU)
                # Small delay to allow signal processing and graceful shutdown
                time.sleep(0.5)
        except OSError as e:
            log.error("Failed to send SIGTTOU to gunicorn master: %s", e)
            raise

    def _wait_for_workers(
        self,
        target_count: int,
        timeout: int = 60,
        check_ready: bool = False,
    ) -> bool:
        """
        Wait for worker count to reach target.

        :param target_count: Target number of workers
        :param timeout: Maximum seconds to wait
        :param check_ready: If True, wait for workers to be ready (process title)
        :return: True if target reached, False if timeout
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if check_ready:
                current = self._get_num_ready_workers()
            else:
                current = self._get_num_workers_running()

            if current >= target_count:
                return True
            time.sleep(1)
        return False

    def _wait_until_healthy(self, timeout: int = 60) -> bool:
        """
        Wait until the server passes HTTP health check.

        :param timeout: Maximum seconds to wait
        :return: True if healthy, False if timeout
        """
        if not self.health_check_url:
            log.debug("No health check URL configured, skipping health check")
            return True

        import httpx

        log.info("Performing health check: %s", self.health_check_url)
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                # verify=False is safe - this is an internal health check to localhost
                response = httpx.get(  # nosec B501
                    self.health_check_url, timeout=5.0, follow_redirects=True, verify=False
                )
                if response.status_code == 200:
                    log.info("Health check passed")
                    return True
                log.debug("Health check returned status %d, retrying...", response.status_code)
            except httpx.RequestError as e:
                log.debug("Health check failed with %s, retrying...", type(e).__name__)
            time.sleep(1)

        log.warning("Health check timed out after %d seconds", timeout)
        return False

    def _get_plugin_state(self) -> str:
        """
        Get a hash of the current plugin state.

        This is used to detect when plugins have changed and a reload is needed.
        """
        plugins_folder = conf.get("core", "plugins_folder", fallback="")
        if not plugins_folder or not os.path.isdir(plugins_folder):
            return ""

        # Hash all Python files in the plugins folder (use sha256 for FIPS compliance)
        hasher = hashlib.sha256()
        for filepath in sorted(glob(os.path.join(plugins_folder, "**/*.py"), recursive=True)):
            try:
                hasher.update(Path(filepath).read_bytes())
            except OSError:
                continue
        return hasher.hexdigest()

    def _check_plugin_changes(self) -> bool:
        """
        Check if plugins have changed since last check.

        :return: True if plugins changed, False otherwise
        """
        current_state = self._get_plugin_state()
        if self._last_plugin_state is None:
            self._last_plugin_state = current_state
            return False

        if current_state != self._last_plugin_state:
            self._last_plugin_state = current_state
            return True
        return False

    def _reload_gunicorn(self) -> None:
        """
        Send SIGHUP to gunicorn master to reload configuration.

        This causes gunicorn to gracefully reload, restarting all workers
        with updated code/configuration.
        """
        log.info("Sending SIGHUP to gunicorn master to reload")
        try:
            os.kill(self.gunicorn_master_pid, signal.SIGHUP)
        except OSError as e:
            log.error("Failed to send SIGHUP to gunicorn master: %s", e)

    def _refresh_workers(self) -> None:
        """
        Perform a rolling worker restart.

        This method:
        1. Spawns batch_size new workers
        2. Waits for them to be ready
        3. Performs health check
        4. Kills batch_size old workers

        This process continues until all original workers have been replaced.
        """
        log.info("Starting worker refresh cycle")
        original_pids = set(self._get_worker_pids())
        batch_size = self.worker_refresh_batch_size

        # Safety limit to prevent infinite loops if workers don't exit
        max_iterations = self.num_workers_expected * 3
        iteration = 0

        while original_pids and iteration < max_iterations:
            iteration += 1
            current_worker_count = self._get_num_workers_running()
            target_after_spawn = current_worker_count + batch_size

            # Step 1: Spawn new workers (temporarily exceeds target for zero-downtime)
            log.info(
                "Rolling restart: spawning %d new worker(s) before killing old ones (current: %d -> %d)",
                batch_size,
                current_worker_count,
                target_after_spawn,
            )
            self._spawn_new_workers(batch_size)

            # Step 2: Wait for new workers to spawn
            if not self._wait_for_workers(target_after_spawn, timeout=60, check_ready=False):
                log.warning("Timeout waiting for new workers to spawn, aborting refresh")
                return

            # Step 3: Wait for new workers to be ready (process title check)
            if not self._wait_for_workers(target_after_spawn, timeout=60, check_ready=True):
                log.warning("Timeout waiting for workers to be ready, aborting refresh")
                return

            # Step 4: Health check
            if not self._wait_until_healthy(timeout=30):
                log.warning("Health check failed after spawning new workers, aborting refresh")
                return

            # Step 5: Kill old workers
            workers_to_kill = min(batch_size, len(original_pids))
            self._kill_old_workers(workers_to_kill)

            # Step 6: Wait for workers to be killed
            target_after_kill = target_after_spawn - workers_to_kill
            if not self._wait_for_workers(target_after_kill, timeout=30, check_ready=False):
                log.warning("Timeout waiting for old workers to exit")

            # Update tracking - remove killed workers from original set
            current_pids = set(self._get_worker_pids())
            killed_pids = original_pids - current_pids
            original_pids -= killed_pids
            log.info("Killed workers: %s, remaining original workers: %d", killed_pids, len(original_pids))

        if original_pids:
            log.error(
                "Worker refresh incomplete: %d original workers remain after %d iterations",
                len(original_pids),
                iteration,
            )
        else:
            log.info("Worker refresh cycle completed")

    def _check_master_alive(self) -> bool:
        """Check if the gunicorn master process is still running."""
        try:
            proc = self._get_gunicorn_master_proc()
            return proc.is_running()
        except psutil.NoSuchProcess:
            return False

    def stop(self) -> None:
        """Signal the monitor to stop."""
        self._should_stop = True

    def start(self) -> None:
        """
        Start the monitoring loop.

        This method runs the main monitoring loop that:
        1. Checks if the gunicorn master is running
        2. Performs rolling worker restarts at configured intervals
        3. Reloads gunicorn if plugins change (if enabled)
        """
        log.info(
            "Starting GunicornMonitor (master_pid=%d, workers=%d, refresh_interval=%ds, batch_size=%d)",
            self.gunicorn_master_pid,
            self.num_workers_expected,
            self.worker_refresh_interval,
            self.worker_refresh_batch_size,
        )

        # Initial delay to let workers start up
        time.sleep(5)

        while not self._should_stop:
            # Check if master is still running
            if not self._check_master_alive():
                log.warning("Gunicorn master process is no longer running, exiting monitor")
                break

            # Check for plugin changes
            if self.reload_on_plugin_change and self._check_plugin_changes():
                log.info("Plugin changes detected, reloading gunicorn")
                self._reload_gunicorn()
                # Reset refresh timer after reload since all workers are new
                self._last_refresh_time = time.monotonic()

            # Check if it's time for a worker refresh
            if self.worker_refresh_interval > 0:
                elapsed = time.monotonic() - self._last_refresh_time
                if elapsed >= self.worker_refresh_interval:
                    self._refresh_workers()
                    self._last_refresh_time = time.monotonic()

            time.sleep(1)

        log.info("GunicornMonitor stopped")


def create_monitor_from_config(
    gunicorn_master_pid: int,
    num_workers: int,
    host: str,
    port: int,
    ssl_enabled: bool = False,
) -> GunicornMonitor:
    """
    Create a GunicornMonitor instance from Airflow configuration.

    :param gunicorn_master_pid: PID of the gunicorn master process
    :param num_workers: Number of expected worker processes
    :param host: Host the server is bound to
    :param port: Port the server is listening on
    :param ssl_enabled: Whether SSL is enabled
    """
    worker_refresh_interval = conf.getint("api", "worker_refresh_interval", fallback=0)
    worker_refresh_batch_size = conf.getint("api", "worker_refresh_batch_size", fallback=1)
    reload_on_plugin_change = conf.getboolean("api", "reload_on_plugin_change", fallback=False)

    # Build health check URL
    # Use 127.0.0.1 if bound to 0.0.0.0 since that's not routable
    # Use /monitor/health endpoint which doesn't require authentication
    health_check_host = "127.0.0.1" if host == "0.0.0.0" else host
    scheme = "https" if ssl_enabled else "http"
    health_check_url = f"{scheme}://{health_check_host}:{port}/api/v2/monitor/health"

    return GunicornMonitor(
        gunicorn_master_pid=gunicorn_master_pid,
        num_workers_expected=num_workers,
        worker_refresh_interval=worker_refresh_interval,
        worker_refresh_batch_size=worker_refresh_batch_size,
        reload_on_plugin_change=reload_on_plugin_change,
        health_check_url=health_check_url,
    )
