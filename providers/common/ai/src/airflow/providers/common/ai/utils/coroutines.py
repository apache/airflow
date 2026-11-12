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
import functools
from typing import TYPE_CHECKING, Any, TypeVar

from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from collections.abc import Callable, Coroutine

P = ParamSpec("P")
R = TypeVar("R")

# Blocking calls to the supervisor made from a coroutine run here, never on asyncio's default
# executor: ``asend()`` reads its reply on that executor while it holds the comms lock, so
# blocking calls that fill it leave nothing to run the read and the task hangs. One worker also
# serializes these calls on Airflow 3.0 and 3.1, where the channel to the supervisor has no
# lock of its own.
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="common-ai-blocking")


async def run_off_loop(fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    """
    Run the blocking ``fn`` on this provider's own worker thread, keeping the event loop free.

    ``fn`` sees the caller's context variables (task logging binds them). Calls are serialized
    across the process, so ``fn`` must not wait on another call made through this function.
    """
    call = functools.partial(contextvars.copy_context().run, fn, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(_executor, call)


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
