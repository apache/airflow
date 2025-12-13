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
from typing import TYPE_CHECKING, Any

import structlog

from airflow.sdk._shared.module_loading import import_string, is_valid_dotpath

if TYPE_CHECKING:
    from collections.abc import Callable

log = structlog.getLogger(__name__)


class Callback:
    """
    Base class for Deadline Alert callbacks.

    Callbacks are used to execute custom logic when a deadline is missed.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level callable in a module present on the host where
    it will run.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    path: str
    kwargs: dict[str, Any]

    def __init__(self, callback_callable: Callable | str, kwargs: dict[str, Any] | None = None) -> None:
        self.path = self.get_callback_path(callback_callable)
        if kwargs and "context" in kwargs:
            raise ValueError("context is a reserved kwarg for this class")
        self.kwargs = kwargs or {}

    @classmethod
    def get_callback_path(cls, _callback: str | Callable) -> str:
        """Convert callback to a string path that can be used to import it later."""
        if callable(_callback):
            cls.verify_callable(_callback)

            # TODO:  This implementation doesn't support using a lambda function as a callback.
            #        We should consider that in the future, but the addition is non-trivial.
            # Get the reference path to the callable in the form `airflow.models.deadline.get_from_db`
            return f"{_callback.__module__}.{_callback.__qualname__}"

        if not isinstance(_callback, str) or not is_valid_dotpath(_callback.strip()):
            raise ImportError(f"`{_callback}` doesn't look like a valid dot path.")

        stripped_callback = _callback.strip()

        try:
            # The provided callback is a string which appears to be a valid dotpath, attempt to import it.
            callback = import_string(stripped_callback)
            if not callable(callback):
                # The input is a string which can be imported, but is not callable.
                raise AttributeError(f"Provided callback {callback} is not callable.")

            cls.verify_callable(callback)

        except ImportError:
            # Logging here instead of failing because it is possible that the code for the callable
            # exists somewhere other than on the DAG processor. We are making a best effort to validate,
            # but can't rule out that it may be available at runtime even if it can not be imported here.
            log.debug(
                "Callback is formatted like a callable dot-path, but could not be imported.",
                callable=stripped_callback,
                exc_info=True,
            )

        return stripped_callback

    @classmethod
    def verify_callable(cls, callback: Callable):
        """For additional verification of the callable during initialization in subclasses."""
        pass  # No verification needed in the base class


class AsyncCallback(Callback):
    """
    Asynchronous callback that runs in the triggerer.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level awaitable callable in a module present on the
    triggerer.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    @classmethod
    def verify_callable(cls, callback: Callable):
        if not (inspect.iscoroutinefunction(callback) or hasattr(callback, "__await__")):
            raise AttributeError(f"Provided callback {callback} is not awaitable.")


class SyncCallback(Callback):
    """
    Synchronous callback that runs in the specified or default executor.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level callable in a module present on the executor.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    executor: str | None

    def __init__(
        self,
        callback_callable: Callable | str,
        kwargs: dict | None = None,
        executor: str | None = None,
    ) -> None:
        super().__init__(callback_callable=callback_callable, kwargs=kwargs)
        self.executor = executor
