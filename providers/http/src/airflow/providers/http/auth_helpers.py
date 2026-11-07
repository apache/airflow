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

import logging
from importlib import import_module
from typing import TYPE_CHECKING

from airflow.sdk.bases.hook import BaseHook

if TYPE_CHECKING:
    from aiohttp import BasicAuth
    from requests.auth import AuthBase

log = logging.getLogger(__name__)

def serialize_auth_type(auth: str | type | None) -> str | None:
    """Convert an auth_type object to the qualname string representation."""
    if auth is None:
        return None
    if isinstance(auth, str):
        return auth
    return f"{auth.__module__}.{auth.__qualname__}"


def deserialize_auth_type(path: str | None) -> type | None:
    """Import an auth_type serialized string from a qualname"""
    if path is None:
        return None
    module_path, cls_name = path.rsplit(".", 1)
    return getattr(import_module(module_path), cls_name)

def resolve_auth_type(auth_type: type[AuthBase] | type[BasicAuth] | None, http_conn_id: str) -> type[AuthBase] | type[BasicAuth] | None:
    """
    Resolve the authentication type for the HTTP request.

    If auth_type is not explicitly set, attempt to infer it from the connection configuration.
    For connections with login/password, default to BasicAuth.

    :return: The resolved authentication type class, or None if no auth is provided.
    """
    if auth_type is not None:
        return auth_type

    try:
        conn = BaseHook.get_connection(http_conn_id)
        if conn.login or conn.password:
            return BasicAuth
    except Exception as e:
        log.warning("Failed to resolve auth type from connection: %s", e)

    return None