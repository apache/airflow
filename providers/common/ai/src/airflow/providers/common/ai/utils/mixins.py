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

from typing import TYPE_CHECKING

from airflow.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook


class CommonAIHookMixin:
    """Mixin for Common AI."""

    def get_db_api_hook(self, conn_id: str) -> DbApiHook:
        """Get the given connection's database hook."""
        connection = BaseHook.get_connection(conn_id)
        return connection.get_hook()
