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

import contextlib
import functools
import logging
import typing

from airflow.hooks.base import BaseHook

if typing.TYPE_CHECKING:
    from typing_extensions import Self

    from airflow.providers.common.sql.hooks.sql import DbApiHook

logger = logging.getLogger(__name__)


class Database:
    """Represent a database."""

    def __init__(self, *, conn_id: str) -> None:
        self._conn_id = conn_id

    @classmethod
    def default(cls, *, protocol: str) -> Self:
        from airflow.providers_manager import ProvidersManager

        if (hook_class := ProvidersManager().hooks.get(protocol)) is None:
            raise KeyError(f"Hook definition not found for {protocol}")
        if (conn_id := getattr(hook_class, hook_class.connection_id_attribute_name, None)) is None:
            raise RuntimeError(f"No default connection for {protocol}")
        return cls(conn_id=conn_id)

    @functools.cached_property
    def uri(self) -> str:
        return BaseHook.get_connection(self._conn_id).get_uri()

    def create_hook(self, **kwargs) -> DbApiHook:
        return BaseHook.get_connection(self._conn_id).get_hook(hook_params=kwargs)

    def connect(self, **kwargs) -> typing.ContextManager:
        return contextlib.closing(self.create_hook(**kwargs).get_conn())
