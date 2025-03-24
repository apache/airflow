#
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

import inspect
import logging
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, Callable, Protocol, TypeVar

from airflow.typing_compat import ParamSpec
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from airflow.sdk.types import OutletEventAccessorsProtocol

P = ParamSpec("P")
R = TypeVar("R")


class KeywordParameters:
    """
    Wrapper representing ``**kwargs`` to a callable.

    The actual ``kwargs`` can be obtained by calling either ``unpacking()`` or
    ``serializing()``. They behave almost the same and are only different if
    the containing ``kwargs`` is an Airflow Context object, and the calling
    function uses ``**kwargs`` in the argument list.

    In this particular case, ``unpacking()`` uses ``lazy-object-proxy`` to
    prevent the Context from emitting deprecation warnings too eagerly when it's
    unpacked by ``**``. ``serializing()`` does not do this, and will allow the
    warnings to be emitted eagerly, which is useful when you want to dump the
    content and use it somewhere else without needing ``lazy-object-proxy``.
    """

    def __init__(self, kwargs: Mapping[str, Any]) -> None:
        self._kwargs = kwargs

    @classmethod
    def determine(
        cls,
        func: Callable[..., Any],
        args: Collection[Any],
        kwargs: Mapping[str, Any],
    ) -> KeywordParameters:
        import inspect
        import itertools

        signature = inspect.signature(func)
        has_wildcard_kwargs = any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())

        for name, param in itertools.islice(signature.parameters.items(), len(args)):
            # Keyword-only arguments can't be passed positionally and are not checked.
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                continue
            if param.kind == inspect.Parameter.VAR_KEYWORD:
                continue

            # Check if args conflict with names in kwargs.
            if name in kwargs:
                raise ValueError(f"The key {name!r} in args is a part of kwargs and therefore reserved.")

        if has_wildcard_kwargs:
            # If the callable has a **kwargs argument, it's ready to accept all the kwargs.
            return cls(kwargs)

        # If the callable has no **kwargs argument, it only wants the arguments it requested.
        filtered_kwargs = {key: kwargs[key] for key in signature.parameters if key in kwargs}
        return cls(filtered_kwargs)

    def unpacking(self) -> Mapping[str, Any]:
        """Dump the kwargs mapping to unpack with ``**`` in a function call."""
        return self._kwargs


def determine_kwargs(
    func: Callable[..., Any],
    args: Collection[Any],
    kwargs: Mapping[str, Any],
) -> Mapping[str, Any]:
    """
    Inspect the signature of a callable to determine which kwargs need to be passed to the callable.

    :param func: The callable that you want to invoke
    :param args: The positional arguments that need to be passed to the callable, so we know how many to skip.
    :param kwargs: The keyword arguments that need to be filtered before passing to the callable.
    :return: A dictionary which contains the keyword arguments that are compatible with the callable.
    """
    return KeywordParameters.determine(func, args, kwargs).unpacking()


def make_kwargs_callable(func: Callable[..., R]) -> Callable[..., R]:
    """
    Create a new callable that only forwards necessary arguments from any provided input.

    Make a new callable that can accept any number of positional or keyword arguments
    but only forwards those required by the given callable func.
    """
    import functools

    @functools.wraps(func)
    def kwargs_func(*args, **kwargs):
        kwargs = determine_kwargs(func, args, kwargs)
        return func(*args, **kwargs)

    return kwargs_func


class _ExecutionCallableRunner(Protocol):
    @staticmethod
    def run(*args, **kwargs): ...


def ExecutionCallableRunner(
    func: Callable[P, R],
    outlet_events: OutletEventAccessorsProtocol,
    *,
    logger: logging.Logger,
) -> _ExecutionCallableRunner:
    """
    Run an execution callable against a task context and given arguments.

    If the callable is a simple function, this simply calls it with the supplied
    arguments (including the context). If the callable is a generator function,
    the generator is exhausted here, with the yielded values getting fed back
    into the task context automatically for execution.

    This convoluted implementation of inner class with closure is so *all*
    arguments passed to ``run()`` can be forwarded to the wrapped function. This
    is particularly important for the argument "self", which some use cases
    need to receive. This is not possible if this is implemented as a normal
    class, where "self" needs to point to the ExecutionCallableRunner object.

    The function name violates PEP 8 due to backward compatibility. This was
    implemented as a class previously.

    :meta private:
    """

    class _ExecutionCallableRunnerImpl:
        @staticmethod
        def run(*args: P.args, **kwargs: P.kwargs) -> R:
            from airflow.sdk.definitions.asset.metadata import Metadata

            if not inspect.isgeneratorfunction(func):
                return func(*args, **kwargs)

            result: Any = NOTSET

            def _run():
                nonlocal result
                result = yield from func(*args, **kwargs)

            for metadata in _run():
                if isinstance(metadata, Metadata):
                    outlet_events[metadata.asset].extra.update(metadata.extra)

                    if metadata.alias:
                        outlet_events[metadata.alias].add(metadata.asset, extra=metadata.extra)

                    continue
                logger.warning("Ignoring unknown data of %r received from task", type(metadata))
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug("Full yielded value: %r", metadata)

            return result

    return _ExecutionCallableRunnerImpl
