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
"""Run a coroutine to completion from synchronous code."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import os
import threading
import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Coroutine


def run_coroutine_sync(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """
    Run ``coroutine`` to completion from synchronous code.

    When no event loop is running in this thread the coroutine is driven with
    :func:`asyncio.run`. When one is (an async caller), it runs on a worker thread
    with its own loop, so the caller's loop is never nested. Either way it sees the
    caller's context variables.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(contextvars.copy_context().run, asyncio.run, coroutine).result()
    # Outside the except block, so what the coroutine raises is not chained to the RuntimeError.
    return asyncio.run(coroutine)


def run_coroutine_on_thread_loop(coroutine: Coroutine[Any, Any, Any]) -> Any:
    """
    Run ``coroutine`` to completion on this thread's event loop, and leave the loop open.

    This is the loop ``asyncio.get_event_loop()`` returns, the one ``Runner.run_sync`` in the
    OpenAI Agents SDK uses and keeps open between calls. An async client is tied to the loop
    it first runs in, so the SDK's shared HTTP client, or one ``AsyncOpenAI`` given to several
    runs, keeps working across calls only on that loop. An ``asyncio.run()`` between two
    calls clears the thread's current loop; the next call puts the kept one back. Tasks the
    call started and left unfinished are cancelled before it returns. When a loop is already
    running in this thread, the coroutine runs as :func:`run_coroutine_sync` runs it, on a
    new loop each time.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        return run_coroutine_sync(coroutine)
    loop = _get_thread_event_loop()
    earlier_tasks = asyncio.all_tasks(loop)
    task = loop.create_task(coroutine)
    try:
        return loop.run_until_complete(task)
    finally:
        # An exception raised from a signal handler (execution_timeout, SIGTERM) stops the loop
        # with the task still pending, and a failed gather() leaves its other children running.
        # Cancel them now, so their cleanup runs and they don't resume during a later call.
        leftovers = asyncio.all_tasks(loop) - earlier_tasks
        for leftover in leftovers:
            leftover.cancel()
        if leftovers:
            loop.run_until_complete(asyncio.gather(*leftovers, return_exceptions=True))


class _KeptLoop(threading.local):
    pid: int | None = None
    loop: asyncio.AbstractEventLoop | None = None


_kept = _KeptLoop()


def _get_thread_event_loop() -> asyncio.AbstractEventLoop:
    with warnings.catch_warnings():
        # Python 3.12 and 3.13 warn when the thread has no current loop yet; 3.14 raises instead.
        warnings.simplefilter("ignore", DeprecationWarning)
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None
    # Python 3.11 leaves the parent's loop current in a forked child (3.12+ clears it), and
    # its selector is shared with the parent, so a child never uses the loop its parent kept.
    if loop is None or loop.is_closed() or (loop is _kept.loop and _kept.pid != os.getpid()):
        # asyncio.run(), run_coroutine_sync() included, clears the current loop when it
        # finishes. Put back the loop the last call kept, unless it belongs to the process
        # this one was forked from.
        kept = _kept.loop if _kept.pid == os.getpid() else None
        loop = kept if kept is not None and not kept.is_closed() else asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    _kept.pid, _kept.loop = os.getpid(), loop
    return loop
