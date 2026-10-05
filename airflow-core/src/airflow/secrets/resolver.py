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
"""Asynchronous server-side resolution through configured secrets backends."""

from __future__ import annotations

import contextlib
import inspect
import logging
from typing import TYPE_CHECKING, Any

from starlette.concurrency import run_in_threadpool

from airflow._shared.secrets_backend.base import accepts_kwarg, call_secrets_backend_method
from airflow._shared.secrets_masker import mask_secret
from airflow.configuration import conf, ensure_secrets_loaded
from airflow.exceptions import AirflowNotFoundException
from airflow.models.connection import AirflowSecretsBackendAccessDenied, Connection
from airflow.models.crypto import get_fernet
from airflow.secrets.metastore import MetastoreBackend

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

    from airflow.secrets import BaseSecretsBackend

log = logging.getLogger(__name__)


def _get_secret_cache() -> Any:
    from airflow.sdk import SecretCache  # noqa: SDK001

    return SecretCache


def _find_method_owner(backend: Any, method_name: str) -> int | None:
    if method_name in vars(backend):
        return -1
    for index, cls in enumerate(type(backend).__mro__):
        if method_name in vars(cls):
            return index
    return None


def _get_native_async_method(backend: Any, *, async_name: str, sync_name: str) -> Callable[..., Any] | None:
    if inspect.getattr_static(backend, async_name, None) is None:
        return None
    async_method = getattr(backend, async_name, None)
    if async_method is None:
        return None

    async_owner = _find_method_owner(backend, async_name)
    sync_owner = _find_method_owner(backend, sync_name)
    if async_owner is not None and sync_owner is not None and sync_owner < async_owner:
        return None
    return async_method


async def _call_backend_method(
    method: Callable[..., Any],
    *,
    team_name: str | None,
    session: AsyncSession | None = None,
    **kwargs: Any,
) -> Any:
    if session is not None and accepts_kwarg(method, "session"):
        kwargs["session"] = session

    if inspect.iscoroutinefunction(method):
        result = call_secrets_backend_method(method, team_name=team_name, **kwargs)
    else:
        result = await run_in_threadpool(
            call_secrets_backend_method,
            method,
            team_name=team_name,
            **kwargs,
        )
    if inspect.isawaitable(result):
        return await result
    return result


async def _call_secret_cache(method: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    # ``SecretCache.init()`` stores entries in a ``multiprocessing.Manager`` dict, so every access to an
    # initialized cache is IPC to the manager process. An uninitialized cache only misses or no-ops.
    if _get_secret_cache()._cache is None:
        return method(*args, **kwargs)
    return await run_in_threadpool(method, *args, **kwargs)


async def _load_fernet() -> None:
    # The first ``get_fernet()`` reads ``core.fernet_key``, which may run ``fernet_key_cmd`` or query a
    # secrets backend. Once loaded, encrypting and decrypting stored values is local work. A failed
    # load is not cached, so its error still surfaces where a value needs the key, as before.
    if not get_fernet.cache_info().currsize:
        with contextlib.suppress(Exception):
            await run_in_threadpool(get_fernet)


def _validate_team_name(team_name: str | None, resource_name: str) -> None:
    if team_name and not conf.getboolean("core", "multi_team"):
        raise ValueError(
            "Multi-team mode is not configured in the Airflow environment but the task trying "
            f"to access the {resource_name} belongs to a team"
        )


async def _load_backends() -> list[BaseSecretsBackend]:
    # While only the two default backends are loaded, every call rebuilds the list, which imports and
    # constructs any configured custom backend.
    return await run_in_threadpool(ensure_secrets_loaded)


async def resolve_variable(
    key: str, team_name: str | None = None, *, session: AsyncSession | None = None
) -> str:
    """Resolve a Variable without blocking the caller's event loop."""
    _validate_team_name(team_name, "variable")

    SecretCache = _get_secret_cache()

    try:
        value = await _call_secret_cache(SecretCache.get_variable, key, team_name=team_name)
    except SecretCache.NotPresentException:
        pass
    else:
        if value is None:
            raise KeyError(f"Variable {key} does not exist.")
        mask_secret(value, key)
        return value

    await _load_fernet()
    value = None
    for backend in await _load_backends():
        try:
            async_method = _get_native_async_method(
                backend,
                async_name="aget_variable",
                sync_name="get_variable",
            )
            if async_method is not None:
                value = await _call_backend_method(
                    async_method,
                    team_name=team_name,
                    session=session if isinstance(backend, MetastoreBackend) else None,
                    key=key,
                )
            else:
                value = await _call_backend_method(
                    backend.get_variable,
                    team_name=team_name,
                    key=key,
                )
            if value is not None:
                break
        except AirflowSecretsBackendAccessDenied:
            raise
        except Exception:
            log.exception(
                "Unable to retrieve variable from secrets backend (%s). Checking subsequent secrets backend.",
                type(backend).__name__,
            )

    await _call_secret_cache(SecretCache.save_variable, key, value, team_name=team_name)
    if value is None:
        raise KeyError(f"Variable {key} does not exist.")
    mask_secret(value, key)
    return value


async def resolve_connection(
    conn_id: str, team_name: str | None = None, *, session: AsyncSession | None = None
) -> Connection:
    """Resolve a Connection without blocking the caller's event loop."""
    _validate_team_name(team_name, "connection")

    SecretCache = _get_secret_cache()
    await _load_fernet()

    try:
        uri = await _call_secret_cache(SecretCache.get_connection_uri, conn_id, team_name=team_name)
    except SecretCache.NotPresentException:
        pass
    else:
        return Connection(conn_id=conn_id, uri=uri)

    for backend in await _load_backends():
        try:
            async_method = _get_native_async_method(
                backend,
                async_name="aget_connection",
                sync_name="get_connection",
            )
            if async_method is not None:
                connection = await _call_backend_method(
                    async_method,
                    team_name=team_name,
                    session=session if isinstance(backend, MetastoreBackend) else None,
                    conn_id=conn_id,
                )
            else:
                connection = await _call_backend_method(
                    backend.get_connection,
                    team_name=team_name,
                    conn_id=conn_id,
                )
            if connection:
                await _call_secret_cache(
                    SecretCache.save_connection_uri,
                    conn_id,
                    connection.get_uri(),
                    team_name=team_name,
                )
                return connection
        except AirflowSecretsBackendAccessDenied:
            raise
        except Exception:
            log.debug(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(backend).__name__,
                exc_info=True,
            )

    raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
