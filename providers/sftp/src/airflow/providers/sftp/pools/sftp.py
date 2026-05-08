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

import asyncio
import os
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field
from threading import Lock
from typing import TYPE_CHECKING
from weakref import WeakKeyDictionary

from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    import asyncssh


@dataclass
class _LoopState:
    """Per-event-loop state for SFTP client pool."""

    idle: asyncio.LifoQueue = field(default_factory=asyncio.LifoQueue)
    in_use: set[tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]] = field(default_factory=set)
    semaphore: asyncio.Semaphore | None = None
    init_lock: asyncio.Lock | None = None
    initialized: bool = False
    closed: bool = False


class SFTPClientPool(LoggingMixin):
    """Lazy Thread-safe and Async-safe Singleton SFTP pool that keeps SSH and SFTP clients alive until exit, and limits concurrent usage to pool_size."""

    _instances: dict[str, SFTPClientPool] = {}
    _lock = Lock()
    _create_connection_max_retries = 2
    _create_connection_retry_base_delay = 0.2
    _create_connection_retry_max_delay = 1.0

    @staticmethod
    def _resolve_pool_size(pool_size: int | None) -> int:
        resolved_pool_size = (os.cpu_count() or 1) if pool_size is None else pool_size
        if resolved_pool_size < 1:
            raise ValueError(f"pool_size must be greater than or equal to 1, got {resolved_pool_size}.")
        return resolved_pool_size

    def __new__(cls, sftp_conn_id: str, pool_size: int | None = None):
        with cls._lock:
            if sftp_conn_id not in cls._instances:
                instance = super().__new__(cls)
                instance._pre_init(sftp_conn_id, pool_size)
                cls._instances[sftp_conn_id] = instance
            else:
                instance = cls._instances[sftp_conn_id]
                if pool_size is not None and pool_size != instance.pool_size:
                    instance.log.debug(
                        "SFTPClientPool for sftp_conn_id '%s' is already initialized with "
                        "pool_size=%d; ignoring requested pool_size=%d and reusing the "
                        "existing singleton.",
                        sftp_conn_id,
                        instance.pool_size,
                        pool_size,
                    )
            return cls._instances[sftp_conn_id]

    def __init__(self, sftp_conn_id: str, pool_size: int | None = None):
        # Prevent parent __init__ argument errors
        pass

    def _pre_init(self, sftp_conn_id: str, pool_size: int | None):
        """Initialize the singleton synchronously, deferring asyncio primitives to the active event loop."""
        LoggingMixin.__init__(self)
        self.sftp_conn_id = sftp_conn_id
        self.pool_size = self._resolve_pool_size(pool_size)
        self._loop_states: WeakKeyDictionary[asyncio.AbstractEventLoop, _LoopState] = WeakKeyDictionary()
        self._loop_states_lock = Lock()
        self.log.info("SFTPClientPool with size %d initialised...", self.pool_size)

    def _get_loop_state(self) -> _LoopState:
        """Get or create the state container for the current event loop."""
        running_loop = asyncio.get_running_loop()
        with self._loop_states_lock:
            state = self._loop_states.get(running_loop)
            if state is None:
                state = _LoopState(
                    semaphore=asyncio.Semaphore(self.pool_size),
                    init_lock=asyncio.Lock(),
                )
                self._loop_states[running_loop] = state
            return state

    async def _ensure_initialized(self):
        """Ensure pool primitives exist for the current loop and the pool is open."""
        state = self._get_loop_state()
        if state.init_lock is None:
            raise RuntimeError("SFTPClientPool init lock is not initialized")

        if state.initialized and not state.closed:
            return

        async with state.init_lock:
            if not state.initialized or state.closed:
                self.log.info(
                    "Initializing / resetting SFTPClientPool for '%s' with size %d",
                    self.sftp_conn_id,
                    self.pool_size,
                )
                state.idle = asyncio.LifoQueue()
                state.in_use.clear()
                state.closed = False
                state.initialized = True

    async def _create_connection(
        self,
    ) -> tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]:
        ssh_conn = await SFTPHookAsync(sftp_conn_id=self.sftp_conn_id)._get_conn()
        sftp = await ssh_conn.start_sftp_client()
        self.log.info("Created new SFTP connection for sftp_conn_id '%s'", self.sftp_conn_id)
        return ssh_conn, sftp

    async def _create_connection_with_retry(
        self,
    ) -> tuple[asyncssh.SSHClientConnection, asyncssh.SFTPClient]:
        max_attempts = self._create_connection_max_retries + 1
        for attempt in range(1, max_attempts + 1):
            try:
                return await self._create_connection()
            except Exception as exc:
                if attempt >= max_attempts:
                    self.log.warning(
                        "Failed creating SFTP connection for '%s' after %d attempts: %s",
                        self.sftp_conn_id,
                        max_attempts,
                        exc,
                    )
                    raise

                delay = min(
                    self._create_connection_retry_base_delay * (2 ** (attempt - 1)),
                    self._create_connection_retry_max_delay,
                )
                self.log.warning(
                    "Failed creating SFTP connection for '%s' (attempt %d/%d): %s. Retrying in %.2fs",
                    self.sftp_conn_id,
                    attempt,
                    max_attempts,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)

        # Unreachable, but keeps type checkers happy.
        raise RuntimeError("Unable to create SFTP connection")

    async def acquire(self):
        await self._ensure_initialized()
        state = self._get_loop_state()

        if state.closed:
            raise RuntimeError("Cannot acquire from a closed SFTPClientPool")

        if state.semaphore is None:
            raise RuntimeError("SFTPClientPool is not initialized")

        self.log.debug("Acquiring SFTP connection for '%s'", self.sftp_conn_id)

        await state.semaphore.acquire()

        try:
            try:
                pair = state.idle.get_nowait()
            except asyncio.QueueEmpty:
                pair = await self._create_connection_with_retry()

            state.in_use.add(pair)
            return pair
        except Exception:
            state.semaphore.release()
            raise

    def _close_connection_pair(self, pair) -> None:
        ssh, sftp = pair
        with suppress(Exception):
            sftp.exit()
        with suppress(Exception):
            ssh.close()

    async def _release_pair(self, pair, state: _LoopState, *, faulty: bool) -> None:
        if pair not in state.in_use:
            self.log.warning("Attempted to release unknown or already released connection")
            return

        if state.semaphore is None:
            raise RuntimeError("SFTPClientPool is not initialized")

        state.in_use.discard(pair)

        if faulty or state.closed:
            self._close_connection_pair(pair)
        else:
            await state.idle.put(pair)

        self.log.debug("Releasing SFTP connection for '%s'", self.sftp_conn_id)
        state.semaphore.release()

    async def release(self, pair):
        state = self._get_loop_state()
        await self._release_pair(pair, state, faulty=False)

    @asynccontextmanager
    async def get_sftp_client(self):
        await self._ensure_initialized()
        state = self._get_loop_state()
        pair = None
        try:
            pair = await self.acquire()
            ssh, sftp = pair
            yield sftp
        except asyncio.CancelledError:
            if pair:
                await self._release_pair(pair, state, faulty=True)
            raise
        except Exception as e:
            self.log.warning("Dropping faulty connection for '%s': %s", self.sftp_conn_id, e)
            if pair:
                await self._release_pair(pair, state, faulty=True)
            raise
        else:
            await self._release_pair(pair, state, faulty=False)

    async def close(self):
        """Gracefully shutdown all connections in the pool for the current event loop."""
        await self._ensure_initialized()
        state = self._get_loop_state()
        if state.init_lock is None:
            raise RuntimeError("SFTPClientPool is not initialized")

        async with state.init_lock:
            if state.closed:
                return

            state.closed = True

            self.log.info("Closing all SFTP connections for '%s'", self.sftp_conn_id)

            while not state.idle.empty():
                pair = await state.idle.get()
                self._close_connection_pair(pair)

            active_in_use = len(state.in_use)
            for pair in list(state.in_use):
                self._close_connection_pair(pair)
                state.in_use.discard(pair)

            if active_in_use:
                self.log.warning("Pool closed with %d active connections", active_in_use)

    async def __aenter__(self):
        await self._ensure_initialized()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Intentionally a no-op: this pool is a process-wide singleton, so
        # exiting a single `async with` block must not close it for all other
        # concurrent users.  Call `close()` explicitly when you truly want to
        # shut down all connections for the current event loop.
        pass
