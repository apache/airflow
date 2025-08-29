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
# This file is generated automatically by the `update-common-sql-api stubs` prek hook
# and the .pyi file represents part of the "public" API that the
# `common.sql` provider exposes to other providers.
#
# Any, potentially breaking change in the stubs will require deliberate manual action from the contributor
# making a change to the `common.sql` provider. Those stubs are also used by MyPy automatically when checking
# if only public API of the common.sql provider is used by all the other providers.
#
# You can read more in the README_API.md file
#
"""Definition of the public interface for airflow.providers.common.sql.operators.generic_transfer."""

from collections.abc import Sequence
from functools import cached_property as cached_property
from typing import Any, ClassVar

import jinja2
from _typeshed import Incomplete as Incomplete

from airflow.models import BaseOperator
from airflow.providers.common.sql.hooks.sql import DbApiHook as DbApiHook
from airflow.utils.context import Context as Context

class GenericTransfer(BaseOperator):
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: ClassVar[dict]
    ui_color: str
    sql: Incomplete
    destination_table: Incomplete
    source_conn_id: Incomplete
    source_hook_params: Incomplete
    destination_conn_id: Incomplete
    destination_hook_params: Incomplete
    preoperator: Incomplete
    insert_args: Incomplete
    page_size: Incomplete
    def __init__(
        self,
        *,
        sql: str,
        destination_table: str,
        source_conn_id: str,
        source_hook_params: dict | None = None,
        destination_conn_id: str,
        destination_hook_params: dict | None = None,
        preoperator: str | list[str] | None = None,
        insert_args: dict | None = None,
        page_size: int | None = None,
        **kwargs,
    ) -> None: ...
    @classmethod
    def get_hook(cls, conn_id: str, hook_params: dict | None = None) -> DbApiHook: ...
    @cached_property
    def source_hook(self) -> DbApiHook: ...
    @cached_property
    def destination_hook(self) -> DbApiHook: ...
    def get_paginated_sql(self, offset: int) -> str: ...
    def render_template_fields(
        self, context: Context, jinja_env: jinja2.Environment | None = None
    ) -> None: ...
    def execute(self, context: Context): ...
    def execute_complete(self, context: Context, event: dict[Any, Any] | None = None) -> Any: ...
