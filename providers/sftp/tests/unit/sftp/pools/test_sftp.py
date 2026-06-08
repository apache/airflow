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

import pytest

from airflow.providers.sftp.pools.sftp import SFTPClientPool


class TestSFTPClientPool:
    @pytest.fixture(autouse=True)
    def cleanup_singleton(self):
        """Clear SFTPClientPool._instances before and after each test to ensure test isolation."""
        # Clear before test
        SFTPClientPool._instances.clear()
        yield
        # Clear after test
        SFTPClientPool._instances.clear()

    @pytest.mark.asyncio
    async def test_acquire_and_release(self, sftp_hook_mocked):
        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None

            await pool.release((ssh, sftp))
            ssh2, sftp2 = await pool.acquire()
            assert ssh2 is not None
            assert sftp2 is not None
            await pool.release((ssh2, sftp2))

    @pytest.mark.asyncio
    async def test_acquire_blocks_when_pool_full(self, sftp_hook_mocked):
        async with SFTPClientPool("blocking_conn", pool_size=2) as pool:
            first = await pool.acquire()
            second = await pool.acquire()

            # Third acquire should block until one of the held connections is released.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(pool.acquire(), timeout=0.1)

            await pool.release(first)
            third = await asyncio.wait_for(pool.acquire(), timeout=1)

            await pool.release(second)
            await pool.release(third)

    @pytest.mark.asyncio
    async def test_get_sftp_client_context_manager(self, sftp_hook_mocked, mocker):
        async with SFTPClientPool("test_conn", pool_size=1) as pool:
            release_spy = mocker.spy(pool, "_release_pair")
            async with pool.get_sftp_client() as sftp:
                assert sftp is not None

            # If the context manager releases correctly, the single slot can be acquired again.
            ssh2, sftp2 = await asyncio.wait_for(pool.acquire(), timeout=1)
            assert ssh2 is not None
            assert sftp2 is not None
            await pool.release((ssh2, sftp2))

            assert any(call.kwargs.get("faulty") is False for call in release_spy.call_args_list)

    @pytest.mark.asyncio
    async def test_get_sftp_client_marks_connection_faulty_on_exception(self, sftp_hook_mocked, mocker):
        pool = SFTPClientPool("faulty_conn", pool_size=1)
        release_spy = mocker.spy(pool, "_release_pair")

        with pytest.raises(ValueError, match="boom"):
            async with pool.get_sftp_client():
                raise ValueError("boom")

        assert any(call.kwargs.get("faulty") is True for call in release_spy.call_args_list)

    @pytest.mark.asyncio
    async def test_get_sftp_client_marks_connection_faulty_on_cancellation(self, sftp_hook_mocked, mocker):
        pool = SFTPClientPool("cancel_conn", pool_size=1)
        release_spy = mocker.spy(pool, "_release_pair")

        with pytest.raises(asyncio.CancelledError):
            async with pool.get_sftp_client():
                raise asyncio.CancelledError()

        assert any(call.kwargs.get("faulty") is True for call in release_spy.call_args_list)

    @pytest.mark.asyncio
    async def test_acquire_failure_releases_semaphore(self, sftp_hook_mocked, monkeypatch):
        from airflow.providers.sftp.hooks.sftp import SFTPHookAsync

        orig_get_conn = SFTPHookAsync._get_conn

        async def fail_get_conn(self):
            raise Exception("fail")

        monkeypatch.setattr(SFTPHookAsync, "_get_conn", fail_get_conn)

        async with SFTPClientPool("test_conn", pool_size=2) as pool:
            monkeypatch.setattr(pool, "_create_connection_retry_base_delay", 0)
            monkeypatch.setattr(pool, "_create_connection_retry_max_delay", 0)
            with pytest.raises(Exception, match="fail"):
                await pool.acquire()

            monkeypatch.setattr(SFTPHookAsync, "_get_conn", orig_get_conn)
            ssh, sftp = await pool.acquire()
            assert ssh is not None
            assert sftp is not None
            await pool.release((ssh, sftp))

    @pytest.mark.asyncio
    async def test_close(self, sftp_hook_mocked, mocker):
        pool = SFTPClientPool("test_conn", pool_size=2)
        close_spy = mocker.spy(pool, "close")

        async with pool:
            ssh, sftp = await pool.acquire()
            await pool.release((ssh, sftp))

        # __aexit__ must NOT call close() — the pool is a process-wide singleton
        assert close_spy.call_count == 0

        # close() works when called explicitly
        await pool.close()
        assert close_spy.call_count == 1

    @pytest.mark.asyncio
    async def test_aexit_does_not_close_pool(self, sftp_hook_mocked):
        """Exiting async-with must not close the singleton for other concurrent users."""
        pool = SFTPClientPool("no_close_conn", pool_size=2)
        async with pool:
            pass  # exit block

        state = pool._get_loop_state()
        assert not state.closed

    @pytest.mark.asyncio
    async def test_close_warns_when_active_connections_exist(self, sftp_hook_mocked):
        class DummySSH:
            def __init__(self):
                self.closed = False

            def close(self):
                self.closed = True

        class DummySFTP:
            def __init__(self):
                self.exited = False

            def exit(self):
                self.exited = True

        pool = SFTPClientPool("warn_conn", pool_size=2)
        await pool._ensure_initialized()
        state = pool._get_loop_state()
        ssh = DummySSH()
        sftp = DummySFTP()
        pair = (ssh, sftp)
        state.in_use.add(pair)

        await pool.close()

        assert pair not in state.in_use
        assert ssh.closed is True
        assert sftp.exited is True

    def test_pool_size_consistency_validation(self, sftp_hook_mocked):
        """Second construction with a different pool_size keeps the original singleton configuration."""
        pool1 = SFTPClientPool("consistent_conn", pool_size=2)
        pool2 = SFTPClientPool("consistent_conn", pool_size=5)

        assert pool2 is pool1
        assert pool2.pool_size == 2

    def test_pool_size_consistency_with_default(self, sftp_hook_mocked, monkeypatch):
        """Default-first construction keeps its pool_size even if later calls pass another value."""
        monkeypatch.setattr("airflow.providers.sftp.pools.sftp.os.cpu_count", lambda: 3)

        pool1 = SFTPClientPool("default_conn")
        pool2 = SFTPClientPool("default_conn", pool_size=10)

        assert pool2 is pool1
        assert pool2.pool_size == 3

    def test_pool_size_defaults_to_one_when_cpu_count_is_none(self, sftp_hook_mocked, monkeypatch):
        """Default pool size falls back to 1 when os.cpu_count() is unavailable."""
        monkeypatch.setattr("airflow.providers.sftp.pools.sftp.os.cpu_count", lambda: None)

        pool = SFTPClientPool("none_cpu_conn")
        assert pool.pool_size == 1

    def test_pool_size_consistency_same_pool_size(self, sftp_hook_mocked):
        """Test that creating a pool with same pool_size for same conn_id succeeds."""
        # Create first instance with pool_size=4
        pool1 = SFTPClientPool("same_pool_conn", pool_size=4)
        assert pool1.pool_size == 4

        # Create another instance with same conn_id and same pool_size should succeed
        pool2 = SFTPClientPool("same_pool_conn", pool_size=4)
        assert pool2 is pool1  # Should be the same instance (singleton)
        assert pool2.pool_size == 4

    def test_pool_size_is_ignored_on_reuse_even_if_invalid(self, sftp_hook_mocked):
        """Subsequent constructions ignore pool_size and keep first singleton configuration."""
        pool1 = SFTPClientPool("reuse_invalid_conn", pool_size=2)

        # This would be invalid for first construction, but must be ignored on reuse.
        pool2 = SFTPClientPool("reuse_invalid_conn", pool_size=0)

        assert pool2 is pool1
        assert pool2.pool_size == 2

    def test_pool_works_across_separate_asyncio_run_calls(self, sftp_hook_mocked):
        """Regression: pool must not raise when used from two separate asyncio.run() calls.

        Each ``asyncio.run()`` creates and then destroys its own event loop.  The singleton
        pool must lazily create fresh asyncio primitives for each new loop rather than
        reusing primitives that are bound to the now-dead previous loop.  Crucially,
        ``async with`` must NOT close the pool between the two calls.
        """
        results: list[bool] = []

        async def use_pool_once():
            pool = SFTPClientPool("multi_loop_conn", pool_size=2)
            async with pool.get_sftp_client() as sftp:
                results.append(sftp is not None)

        # First run — creates a new event loop, uses the pool, then shuts the loop down.
        asyncio.run(use_pool_once())
        # Second run — creates a *different* event loop; the pool must not reuse the
        # primitives from the first (now-closed) loop.
        asyncio.run(use_pool_once())

        assert results == [True, True]

    def test_pool_singleton_is_preserved_across_asyncio_run_calls(self, sftp_hook_mocked):
        """The same pool instance (singleton) must be returned for the same conn_id
        even when called from successive asyncio.run() invocations."""
        pools: list[SFTPClientPool] = []

        async def capture_pool():
            pools.append(SFTPClientPool("singleton_loop_conn", pool_size=1))

        asyncio.run(capture_pool())
        asyncio.run(capture_pool())

        assert pools[0] is pools[1]

    def test_pool_per_loop_state_isolation(self, sftp_hook_mocked):
        """Regression: two event loops must maintain isolated per-loop state in same singleton pool."""
        loop_states: list[tuple[asyncio.AbstractEventLoop, int]] = []

        async def capture_loop_state():
            pool = SFTPClientPool("isolated_conn", pool_size=2)
            state = pool._get_loop_state()
            loop = asyncio.get_running_loop()
            # Acquire and hold a connection to track state per loop
            con = await pool.acquire()
            loop_states.append((loop, len(state.in_use)))
            await pool.release(con)

        asyncio.run(capture_loop_state())
        asyncio.run(capture_loop_state())

        # Both runs should have recorded state, but from different loops
        assert len(loop_states) == 2
        loop1, count1 = loop_states[0]
        loop2, count2 = loop_states[1]
        assert loop1 is not loop2
        assert count1 == 1  # One connection in use during acquire
        assert count2 == 1  # Same state captured in the second loop's context

    def test_connection_reuse_within_same_loop(self, sftp_hook_mocked):
        """Verify that connections acquired and released in the same loop are reused."""

        async def acquire_release_reuse():
            pool = SFTPClientPool("reuse_conn", pool_size=1)
            # First acquire
            con1 = await pool.acquire()
            await pool.release(con1)
            # Second acquire should get the same connection back (from idle queue)
            con2 = await pool.acquire()
            assert con1 is con2  # Same object reused
            await pool.release(con2)

        asyncio.run(acquire_release_reuse())

    def test_no_semaphore_leak_on_cross_loop_release(self, sftp_hook_mocked):
        """Regression: releasing a connection acquired in one loop but released in another
        should not leak semaphore permits or raise errors.

        Since asyncio primitives are per-loop, we expect release to work on the current loop's state only.
        """
        acquired_pair = None

        async def acquire_in_loop1():
            nonlocal acquired_pair
            pool = SFTPClientPool("leak_test_conn", pool_size=1)
            acquired_pair = await pool.acquire()

        async def release_in_loop2():
            # This will be called with a different event loop
            pool = SFTPClientPool("leak_test_conn", pool_size=1)
            state = pool._get_loop_state()
            # The pair was acquired in loop1, but we're now in loop2
            # The loop2 state should show empty in_use (since the pair was acquired in loop1's state)
            # Attempting to release should not crash but log a warning
            if acquired_pair:
                await pool.release(acquired_pair)
            # Verify the new loop's state is clean
            assert len(state.in_use) == 0

        asyncio.run(acquire_in_loop1())
        asyncio.run(release_in_loop2())

    @pytest.mark.asyncio
    async def test_create_connection_retries_then_succeeds(self, sftp_hook_mocked, monkeypatch):
        pool = SFTPClientPool("retry_success_conn", pool_size=1)
        await pool._ensure_initialized()

        call_count = 0

        async def flaky_create_connection():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionError("transient")
            return "ssh", "sftp"

        monkeypatch.setattr(pool, "_create_connection", flaky_create_connection)
        monkeypatch.setattr(pool, "_create_connection_retry_base_delay", 0)
        monkeypatch.setattr(pool, "_create_connection_retry_max_delay", 0)

        pair = await pool.acquire()
        assert pair == ("ssh", "sftp")
        assert call_count == 2
        await pool.release(pair)

    @pytest.mark.asyncio
    async def test_create_connection_retries_exhaust_and_releases_permit(self, sftp_hook_mocked, monkeypatch):
        pool = SFTPClientPool("retry_fail_conn", pool_size=1)
        await pool._ensure_initialized()
        state = pool._get_loop_state()

        async def always_fail_create_connection():
            raise ConnectionError("still failing")

        monkeypatch.setattr(pool, "_create_connection", always_fail_create_connection)
        monkeypatch.setattr(pool, "_create_connection_retry_base_delay", 0)
        monkeypatch.setattr(pool, "_create_connection_retry_max_delay", 0)
        monkeypatch.setattr(pool, "_create_connection_max_retries", 1)

        with pytest.raises(ConnectionError, match="still failing"):
            await pool.acquire()

        # Ensure permit is returned after failure so a later acquire can proceed.
        assert state.semaphore is not None
        assert state.semaphore._value == pool.pool_size
