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

from typing import TYPE_CHECKING

from airflow.providers.common.compat._compat_utils import create_module_getattr
from airflow.providers.common.compat.version_compat import (
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_PLUS,
    AIRFLOW_V_3_2_PLUS,
)

_IMPORT_MAP: dict[str, str | tuple[str, ...]] = {
    # Re-export from sdk (which handles Airflow 2.x/3.x fallbacks)
    "AsyncExecutionCallableRunner": "airflow.providers.common.compat.sdk",
    "BaseOperator": "airflow.providers.common.compat.sdk",
    "BaseAsyncOperator": "airflow.providers.common.compat.sdk",
    "create_async_executable_runner": "airflow.providers.common.compat.sdk",
    "get_current_context": "airflow.providers.common.compat.sdk",
    "is_async_callable": "airflow.providers.common.compat.sdk",
    # Standard provider items with direct fallbacks
    "PythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "ShortCircuitOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "_SERIALIZERS": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
}

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
    from airflow.sdk.execution_time.callback_runner import (
        AsyncExecutionCallableRunner,
        create_async_executable_runner,
    )
    from airflow.sdk.types import OutletEventAccessorsProtocol
elif AIRFLOW_V_3_2_PLUS:
    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
    from airflow.sdk.execution_time.callback_runner import (
        AsyncExecutionCallableRunner,
        create_async_executable_runner,
    )
else:
    import asyncio
    import contextlib
    import inspect
    import logging
    from asyncio import AbstractEventLoop
    from collections.abc import AsyncIterator, Awaitable, Callable, Generator
    from contextlib import suppress
    from functools import partial
    from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar, cast

    from typing_extensions import ParamSpec

    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import BaseOperator
        from airflow.sdk.bases.decorator import _TaskDecorator
        from airflow.sdk.definitions.asset.metadata import Metadata
        from airflow.sdk.definitions.mappedoperator import OperatorPartial
    else:
        from airflow.datasets.metadata import Metadata
        from airflow.decorators.base import _TaskDecorator
        from airflow.models import BaseOperator
        from airflow.models.mappedoperator import OperatorPartial

    P = ParamSpec("P")
    R = TypeVar("R")

    @contextlib.contextmanager
    def event_loop() -> Generator[AbstractEventLoop]:
        new_event_loop = False
        loop = None
        try:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    raise RuntimeError
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                new_event_loop = True
            yield loop
        finally:
            if new_event_loop and loop is not None:
                with contextlib.suppress(AttributeError):
                    loop.close()
                    asyncio.set_event_loop(None)

    def unwrap_partial(fn):
        while isinstance(fn, partial):
            fn = fn.func
        return fn

    def unwrap_callable(func):
        # Airflow-specific unwrap
        if isinstance(func, (_TaskDecorator, OperatorPartial)):
            func = getattr(func, "function", getattr(func, "_func", func))

        # Unwrap functools.partial
        func = unwrap_partial(func)

        # Unwrap @functools.wraps chains
        with suppress(Exception):
            func = inspect.unwrap(func)

        return func

    def is_async_callable(func):
        """Detect if a callable (possibly wrapped) is an async function."""
        func = unwrap_callable(func)

        if not callable(func):
            return False

        # Direct async function
        if inspect.iscoroutinefunction(func):
            return True

        # Callable object with async __call__
        if not inspect.isfunction(func):
            call = type(func).__call__  # Bandit-safe
            with suppress(Exception):
                call = inspect.unwrap(call)
            if inspect.iscoroutinefunction(call):
                return True

        return False

    class _AsyncExecutionCallableRunner(Generic[P, R]):
        @staticmethod
        async def run(*args: P.args, **kwargs: P.kwargs) -> R: ...  # type: ignore[empty-body]

    class AsyncExecutionCallableRunner(Protocol):
        def __call__(
            self,
            func: Callable[P, R],
            outlet_events: OutletEventAccessorsProtocol,
            *,
            logger: logging.Logger | Logger,
        ) -> _AsyncExecutionCallableRunner[P, R]: ...

    def create_async_executable_runner(
        func: Callable[P, Awaitable[R] | AsyncIterator],
        outlet_events: OutletEventAccessorsProtocol,
        *,
        logger: logging.Logger | Logger,
    ) -> _AsyncExecutionCallableRunner[P, R]:
        """
        Run an async execution callable against a task context and given arguments.

        If the callable is a simple function, this simply calls it with the supplied
        arguments (including the context). If the callable is a generator function,
        the generator is exhausted here, with the yielded values getting fed back
        into the task context automatically for execution.

        This convoluted implementation of inner class with closure is so *all*
        arguments passed to ``run()`` can be forwarded to the wrapped function. This
        is particularly important for the argument "self", which some use cases
        need to receive. This is not possible if this is implemented as a normal
        class, where "self" needs to point to the runner object, not the object
        bounded to the inner callable.

        :meta private:
        """

        class _AsyncExecutionCallableRunnerImpl(_AsyncExecutionCallableRunner):
            @staticmethod
            async def run(*args: P.args, **kwargs: P.kwargs) -> R:
                if not inspect.isasyncgenfunction(func):
                    result = cast("Awaitable[R]", func(*args, **kwargs))
                    return await result

                results: list[Any] = []

                async for result in func(*args, **kwargs):
                    if isinstance(result, Metadata):
                        outlet_events[result.asset].extra.update(result.extra)
                        if result.alias:
                            outlet_events[result.alias].add(result.asset, extra=result.extra)

                    results.append(result)

                return cast("R", results)

        return cast("_AsyncExecutionCallableRunner[P, R]", _AsyncExecutionCallableRunnerImpl)

    class BaseAsyncOperator(BaseOperator):
        """
        Base class for async-capable operators.

        As opposed to deferred operators which are executed on the triggerer, async operators are executed
        on the worker.
        """

        @property
        def is_async(self) -> bool:
            return True

        if not AIRFLOW_V_3_1_PLUS:

            @property
            def xcom_push(self) -> bool:
                return self.do_xcom_push

            @xcom_push.setter
            def xcom_push(self, value: bool):
                self.do_xcom_push = value

        async def aexecute(self, context):
            """Async version of execute(). Subclasses should implement this."""
            raise NotImplementedError()

        def execute(self, context):
            """Run `aexecute()` inside an event loop."""
            with event_loop() as loop:
                if self.execution_timeout:
                    return loop.run_until_complete(
                        asyncio.wait_for(
                            self.aexecute(context),
                            timeout=self.execution_timeout.total_seconds(),
                        )
                    )
                return loop.run_until_complete(self.aexecute(context))


__getattr__ = create_module_getattr(import_map=_IMPORT_MAP)

__all__ = sorted(_IMPORT_MAP.keys())
