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
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

_IMPORT_MAP: dict[str, str | tuple[str, ...]] = {
    # Re-export from sdk (which handles Airflow 2.x/3.x fallbacks)
    "BaseOperator": "airflow.providers.common.compat.sdk",
    "BaseAsyncOperator": "airflow.providers.common.compat.sdk",
    "get_current_context": "airflow.providers.common.compat.sdk",
    "is_async_callable": "airflow.providers.common.compat.sdk",
    # Standard provider items with direct fallbacks
    "PythonOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "ShortCircuitOperator": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
    "_SERIALIZERS": ("airflow.providers.standard.operators.python", "airflow.operators.python"),
}

if TYPE_CHECKING:
    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
elif AIRFLOW_V_3_2_PLUS:
    from airflow.sdk.bases.decorator import is_async_callable
    from airflow.sdk.bases.operator import BaseAsyncOperator
else:
    import asyncio
    import contextlib
    import inspect

    from asyncio import AbstractEventLoop
    from collections.abc import Generator
    from contextlib import suppress
    from functools import partial

    if AIRFLOW_V_3_0_PLUS:
        from airflow.sdk import BaseOperator
    else:
        from airflow.models import BaseOperator

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
        from airflow.sdk.bases.decorator import _TaskDecorator
        from airflow.sdk.definitions.mappedoperator import OperatorPartial

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

    class BaseAsyncOperator(BaseOperator):
        """
        Base class for async-capable operators.

        As opposed to deferred operators which are executed on the triggerer, async operators are executed
        on the worker.
        """

        @property
        def is_async(self) -> bool:
            return True

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
