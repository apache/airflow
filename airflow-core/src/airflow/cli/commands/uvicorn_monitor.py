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
"""Monitor uvicorn workers and perform rolling restarts for zero-downtime worker recycling."""

from __future__ import annotations

import logging
import os
import signal
import sys
import time
import urllib.error
import urllib.request

import psutil

log = logging.getLogger(__name__)


class UvicornMonitor:
    """
    Monitors uvicorn workers and performs rolling restarts.

    This class provides zero-downtime worker recycling for the API server by using
    uvicorn's SIGTTIN/SIGTTOU signals to dynamically manage worker processes.

    Rolling restart pattern:
    [n / n] ──TTIN──> [n+bs / n+bs] ──health check──> [n+bs ready] ──TTOU──> [n / n]

    This ensures zero-downtime because new workers are ready BEFORE old ones are killed.

    :param uvicorn_parent_pid: PID of the uvicorn parent process to monitor
    :param num_workers_expected: Expected number of worker processes
    :param worker_refresh_interval: Seconds between worker refresh cycles (0 to disable)
    :param worker_refresh_batch_size: Number of workers to refresh at a time
    :param health_check_url: URL to check for worker health before killing old workers
    """

    def __init__(
        self,
        uvicorn_parent_pid: int,
        num_workers_expected: int,
        worker_refresh_interval: int,
        worker_refresh_batch_size: int,
        health_check_url: str,
    ):
        self.uvicorn_parent_proc = psutil.Process(uvicorn_parent_pid)
        self.num_workers_expected = num_workers_expected
        self.worker_refresh_interval = worker_refresh_interval
        self.worker_refresh_batch_size = worker_refresh_batch_size
        self.health_check_url = health_check_url
        self._last_refresh_time = time.monotonic()

    def _get_num_workers_running(self) -> int:
        """Return the number of currently running worker processes."""
        try:
            return len(self.uvicorn_parent_proc.children())
        except psutil.NoSuchProcess:
            return 0

    def _get_worker_pids(self) -> list[int]:
        """Return list of current worker PIDs."""
        try:
            return [child.pid for child in self.uvicorn_parent_proc.children()]
        except psutil.NoSuchProcess:
            return []

    def _spawn_new_workers(self, count: int) -> tuple[bool, list[int]]:
        """
        Spawn new worker processes by sending SIGTTIN signals.

        :param count: Number of workers to spawn
        :return: Tuple of (success, list of new worker PIDs)
        """
        pids_before = set(self._get_worker_pids())
        log.debug("Spawning %d new workers (current PIDs: %s)", count, sorted(pids_before))
        try:
            for _ in range(count):
                self.uvicorn_parent_proc.send_signal(signal.SIGTTIN)
        except psutil.NoSuchProcess:
            log.error("Uvicorn parent process died during worker spawn")
            return False, []

        # Wait for new workers to appear
        target = len(pids_before) + count
        if not self._wait_for_workers(target, wait_for_increase=True, timeout=30):
            return False, []

        pids_after = set(self._get_worker_pids())
        new_pids = sorted(pids_after - pids_before)
        log.info("Spawned new workers: PIDs %s", new_pids)
        return True, new_pids

    def _kill_old_workers(self, count: int, pids_to_kill: list[int]) -> bool:
        """
        Kill specific old worker processes by sending SIGTERM directly to them.

        We send SIGTERM directly to specific PIDs instead of using SIGTTOU because
        SIGTTOU lets uvicorn choose which worker to kill (often the newest one),
        which defeats the purpose of rolling restart.

        :param count: Number of workers to kill
        :param pids_to_kill: Specific PIDs of old workers to kill
        :return: True if successful, False on error
        """
        pids = pids_to_kill[:count]
        log.info("Killing old workers: PIDs %s", pids)
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError as e:
                log.warning("Failed to kill worker PID %d: %s", pid, e)
        # Wait for worker count to decrease
        self._wait_for_workers(self.num_workers_expected, wait_for_increase=False, timeout=30)
        return True

    def _wait_for_workers(self, target: int, wait_for_increase: bool, timeout: int = 30) -> bool:
        """
        Wait for worker count to reach target.

        :param target: Target number of workers
        :param wait_for_increase: If True, wait for >= target; if False, wait for <= target
        :param timeout: Maximum time to wait in seconds
        :return: True if target reached, False if timed out
        """
        start = time.monotonic()
        while True:
            current = self._get_num_workers_running()
            if wait_for_increase and current >= target:
                return True
            if not wait_for_increase and current <= target:
                return True
            if time.monotonic() - start > timeout:
                log.warning(
                    "Timeout waiting for workers: target=%d, current=%d, wait_for_increase=%s",
                    target,
                    current,
                    wait_for_increase,
                )
                return False
            time.sleep(0.5)
        return True

    def _wait_until_healthy(self, timeout: int = 120) -> bool:
        """
        Wait until the health check endpoint responds successfully.

        :param timeout: Maximum time to wait in seconds
        :return: True if healthy, False if timed out
        """
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            try:
                request = urllib.request.Request(self.health_check_url, method="GET")
                with urllib.request.urlopen(request, timeout=5) as response:
                    if response.status == 200:
                        log.debug("Health check passed")
                        return True
            except (urllib.error.URLError, OSError) as e:
                log.debug("Health check failed: %s", e)
            time.sleep(1)
        log.warning("Health check timed out after %d seconds", timeout)
        return False

    def _refresh_workers(self) -> None:
        """
        Perform a rolling restart of workers.

        This spawns new workers first, waits for them to be healthy,
        then kills the old workers to ensure zero-downtime.
        """
        batch_size = min(self.worker_refresh_batch_size, self.num_workers_expected)
        pids_before = self._get_worker_pids()
        log.info("Starting worker refresh: current workers PIDs %s", pids_before)

        success, new_pids = self._spawn_new_workers(batch_size)
        if not success:
            # Parent process died, will be caught by main loop
            self._last_refresh_time = time.monotonic()
            return

        # Wait for new workers to be healthy before killing old ones
        log.info("Waiting for health check before killing old workers...")
        if self._wait_until_healthy():
            self._kill_old_workers(batch_size, pids_before)
            log.info(
                "Worker refresh completed: new PIDs %s replaced old PIDs %s",
                new_pids,
                pids_before[:batch_size],
            )
        else:
            log.warning("New workers not healthy, rolling back by killing new workers")
            # Try to restore original worker count by killing the new workers
            try:
                for _ in range(batch_size):
                    self.uvicorn_parent_proc.send_signal(signal.SIGTTOU)
                self._wait_for_workers(self.num_workers_expected, wait_for_increase=False, timeout=30)
            except psutil.NoSuchProcess:
                pass

        self._last_refresh_time = time.monotonic()

    def start(self) -> None:
        """
        Start the monitor loop.

        This runs indefinitely, checking if workers need to be refreshed
        and performing rolling restarts when necessary.
        """
        log.info(
            "Starting UvicornMonitor: refresh_interval=%d, batch_size=%d, workers=%d",
            self.worker_refresh_interval,
            self.worker_refresh_batch_size,
            self.num_workers_expected,
        )

        while True:
            if not self.uvicorn_parent_proc.is_running():
                log.error("Uvicorn parent process died, exiting monitor")
                sys.exit(1)

            if self.worker_refresh_interval > 0:
                elapsed = time.monotonic() - self._last_refresh_time
                if elapsed >= self.worker_refresh_interval:
                    self._refresh_workers()

            time.sleep(1)
