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

import json
import logging
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.compat.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


log = logging.getLogger(__name__)


async def get_async_connection(conn_id: str, hook: BaseHook | type[BaseHook] | None = None) -> Connection:
    """
    Get an asynchronous Airflow connection that is backwards compatible.

    :param conn_id: The provided connection ID.
    :param hook: Hook (class or instance) to resolve the connection through, so a
        subclass override of ``aget_connection``/``get_connection`` is honored.
        Defaults to ``BaseHook``.
    :returns: Connection
    """
    from asgiref.sync import sync_to_async

    hook = hook or BaseHook
    hook_name = hook.__name__ if isinstance(hook, type) else type(hook).__name__
    if hasattr(hook, "aget_connection"):
        log.debug("Get connection using `%s.aget_connection()`.", hook_name)
        return await hook.aget_connection(conn_id=conn_id)
    log.debug("Get connection using `%s.get_connection()`.", hook_name)
    return await sync_to_async(hook.get_connection)(conn_id=conn_id)


async def get_async_extra_dejson(conn: Connection) -> dict[str, Any]:
    """
    Get the connection's extra as a dict, asynchronously and backwards compatible.

    The async counterpart of ``Connection.extra_dejson``, for hooks running on an event loop
    (async tasks, triggers). ``extra_dejson`` masks the extra's secrets with a synchronous
    call to the supervisor, which raises ``DeadlockImminentError`` when another async call
    is in flight on the same event loop.

    * Airflow 3.3.2+: awaits ``Connection.aextra_dejson()``, which masks the secrets asynchronously.
    * Airflow 3.2 to 3.3.1, and Airflow 2: runs the synchronous ``extra_dejson`` in a worker
      thread, the same way :func:`get_async_connection` falls back to ``get_connection``. From
      Airflow 3.2 the supervisor channel serializes a worker thread's send against the event
      loop's in-flight ``asend()``; Airflow 2 has no supervisor.
    * Airflow 3.0 and 3.1: deserializes ``Connection.extra`` without masking it. Their supervisor
      channel is not thread-safe, so a send from a worker thread could interleave with the event
      loop's in-flight call; this is what async hooks did before this helper existed.

    :param conn: The connection, e.g. from :func:`get_async_connection`.
    :returns: The deserialized extra, with its secrets masked except on Airflow 3.0 and 3.1.
    """
    if hasattr(conn, "aextra_dejson"):
        log.debug("Get connection extra using `Connection.aextra_dejson()`.")
        return await conn.aextra_dejson()

    if AIRFLOW_V_3_2_PLUS or not AIRFLOW_V_3_0_PLUS:
        from asgiref.sync import sync_to_async

        log.debug("Get connection extra using `Connection.extra_dejson` in a worker thread.")
        return await sync_to_async(lambda: conn.extra_dejson)()

    log.debug("Get connection extra by deserializing `Connection.extra`, without masking it.")
    return json.loads(conn.extra) if conn.extra else {}


__all__ = [
    "get_async_connection",
    "get_async_extra_dejson",
]
