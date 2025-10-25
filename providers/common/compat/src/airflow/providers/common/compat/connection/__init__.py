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

import logging
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Connection


log = logging.getLogger(__name__)


async def get_async_connection(conn_id: str) -> Connection:
    """
    Get an asynchronous Airflow connection that is backwards compatible.

    :param conn_id: The provided connection ID.
    :returns: Connection
    """
    from asgiref.sync import sync_to_async

    if hasattr(BaseHook, "aget_connection"):
        log.debug("Get connection using `BaseHook.aget_connection().")
        return await BaseHook.aget_connection(conn_id=conn_id)
    log.debug("Get connection using `BaseHook.get_connection().")
    return await sync_to_async(BaseHook.get_connection)(conn_id=conn_id)


__all__ = [
    "get_async_connection",
]
