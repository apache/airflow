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
from abc import ABC
from collections.abc import Callable
from typing import Any

import structlog

from airflow.sdk.module_loading import import_string, is_valid_dotpath

log = structlog.getLogger(__name__)


class Callback(ABC):
    """
    Base class for Deadline Alert callbacks.

    Callbacks are used to execute custom logic when a deadline is missed.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level callable in a module present on the host where
    it will run.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    path: str
    kwargs: dict

    def __init__(self, callback_callable: Callable | str, kwargs: dict[str, Any] | None = None):
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

        except ImportError as e:
            # Logging here instead of failing because it is possible that the code for the callable
            # exists somewhere other than on the DAG processor. We are making a best effort to validate,
            # but can't rule out that it may be available at runtime even if it can not be imported here.
            log.debug(
                "Callback %s is formatted like a callable dotpath, but could not be imported.\n%s",
                stripped_callback,
                e,
            )

        return stripped_callback

    @classmethod
    def verify_callable(cls, callback: Callable):
        """For additional verification of the callable during initialization in subclasses."""
        pass  # No verification needed in the base class

    @classmethod
    def deserialize(cls, data: dict, version):
        path = data.pop("path")
        return cls(callback_callable=path, **data)

    @classmethod
    def serialized_fields(cls) -> tuple[str, ...]:
        return ("path", "kwargs")

    def serialize(self) -> dict[str, Any]:
        return {f: getattr(self, f) for f in self.serialized_fields()}

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self.serialize() == other.serialize()

    def __hash__(self):
        serialized = self.serialize()
        hashable_items = []
        for k, v in serialized.items():
            if isinstance(v, dict):
                hashable_items.append((k, tuple(sorted(v.items()))))
            else:
                hashable_items.append((k, v))
        return hash(tuple(sorted(hashable_items)))


class AsyncCallback(Callback):
    """
    Asynchronous callback that runs in the triggerer.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level awaitable callable in a module present on the
    triggerer.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    def __init__(self, callback_callable: Callable | str, kwargs: dict | None = None):
        super().__init__(callback_callable=callback_callable, kwargs=kwargs)

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
        self, callback_callable: Callable | str, kwargs: dict | None = None, executor: str | None = None
    ):
        super().__init__(callback_callable=callback_callable, kwargs=kwargs)
        self.executor = executor

    @classmethod
    def serialized_fields(cls) -> tuple[str, ...]:
        return super().serialized_fields() + ("executor",)
