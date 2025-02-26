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
#
# This is automatically generated stub for the `common.sql` provider
#
# This file is generated automatically by the `update-common-sql-api stubs` pre-commit
# and the .pyi file represents part of the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""
Definition of the public interface for airflow.providers.common.sql.triggers.sql
isort:skip_file
"""

from collections.abc import AsyncIterator
from typing import Any

from airflow.triggers.base import BaseTrigger as BaseTrigger, TriggerEvent as TriggerEvent

class SQLExecuteQueryTrigger(BaseTrigger):
    def __init__(
        self, sql: str | list[str], conn_id: str, hook_params: dict | None = None, **kwargs
    ) -> None: ...
    def serialize(self) -> tuple[str, dict[str, Any]]: ...
    async def run(self) -> AsyncIterator[TriggerEvent]: ...  # type: ignore
