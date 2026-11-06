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

from airflow.providers.common.compat.sdk import BaseHook

log = logging.getLogger(__name__)


async def get_async_hook(conn_id: str, hook_params: dict | None = None) -> BaseHook:
    """
    Get an asynchronous Airflow hook that is backwards compatible.

    :param conn_id: The provided connection ID.
    :param hook_params: Additional hook params.
    :returns: BaseHook
    """
    from asgiref.sync import sync_to_async

    if hasattr(BaseHook, "aget_hook"):
        log.debug("Get hook using `BaseHook.aget_hook()`.")
        return await BaseHook.aget_hook(conn_id=conn_id, hook_params=hook_params)
    log.debug("Get hook using `BaseHook.get_hook()`.")
    return await sync_to_async(BaseHook.get_hook)(conn_id=conn_id, hook_params=hook_params)


__all__ = [
    "get_async_hook",
]
