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
"""Shared plumbing for Apache Polaris operators."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from functools import cached_property, wraps
from typing import Any

from airflow.providers.apache.polaris.exceptions import PolarisError
from airflow.providers.apache.polaris.hooks.polaris import (
    PolarisCatalogHook,
    PolarisHook,
    PolarisManagementHook,
)
from airflow.providers.common.compat.sdk import AirflowFailException, BaseOperator


def _fail_without_retry(execute: Callable[..., Any]) -> Callable[..., Any]:
    """Turn Polaris errors that a retry cannot fix into ``AirflowFailException``."""

    @wraps(execute)
    def wrapper(self: BaseOperator, *args: Any, **kwargs: Any) -> Any:
        try:
            return execute(self, *args, **kwargs)
        except PolarisError as e:
            if e.retryable:
                raise
            message = str(e).rstrip()
            if not message.endswith("."):
                message += "."
            raise AirflowFailException(f"{message} Not retrying: a retry cannot fix this error.") from e

    return wrapper


class PolarisBaseOperator(BaseOperator):
    """
    Base class for Polaris operators: holds the connection id.

    Operators return only JSON-serializable, non-secret values, so their results are safe in XCom.

    Errors a retry cannot fix (a misconfigured connection, invalid arguments, rejected credentials,
    missing privileges, a bad request, a missing or already existing entity) fail the task
    immediately, without using its retries. Transient errors (network failures, server errors,
    rate limiting, concurrent updates) are raised as usual, so the task's retries apply.

    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = ("polaris_conn_id",)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        if "execute" in cls.__dict__:
            cls.execute = _fail_without_retry(cls.__dict__["execute"])  # type: ignore[method-assign]

    def __init__(self, *, polaris_conn_id: str = PolarisHook.default_conn_name, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.polaris_conn_id = polaris_conn_id


class PolarisManagementBaseOperator(PolarisBaseOperator):
    """Base class for operators that use the Management API."""

    @cached_property
    def hook(self) -> PolarisManagementHook:
        """The Polaris Management API hook for this operator's connection."""
        return PolarisManagementHook(polaris_conn_id=self.polaris_conn_id)


class PolarisCatalogBaseOperator(PolarisBaseOperator):
    """Base class for operators that use the Polaris catalog APIs."""

    @cached_property
    def hook(self) -> PolarisCatalogHook:
        """The Polaris catalog hook for this operator's connection."""
        return PolarisCatalogHook(polaris_conn_id=self.polaris_conn_id)
