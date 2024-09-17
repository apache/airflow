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
Definition of the public interface for airflow.providers.common.sql.operators.sql
isort:skip_file
"""
from _typeshed import Incomplete
from airflow.exceptions import (
    AirflowException as AirflowException,
    AirflowFailException as AirflowFailException,
)
from airflow.hooks.base import BaseHook as BaseHook
from airflow.models import BaseOperator as BaseOperator, SkipMixin as SkipMixin
from airflow.providers.common.sql.hooks.sql import (
    DbApiHook as DbApiHook,
    fetch_all_handler as fetch_all_handler,
    return_single_query_results as return_single_query_results,
)
from airflow.providers.openlineage.extractors import OperatorLineage as OperatorLineage
from airflow.utils.context import Context as Context
from airflow.utils.helpers import merge_dicts as merge_dicts
from functools import cached_property as cached_property
from typing import Any, Callable, Iterable, Mapping, Sequence, SupportsAbs, Optional

def _parse_boolean(val: str) -> str | bool: ...
def parse_boolean(val: str) -> str | bool: ...

class BaseSQLOperator(BaseOperator):
    conn_id_field: str
    template_fields: Sequence[str]
    conn_id: Incomplete
    database: Incomplete
    hook_params: Incomplete
    retry_on_failure: Incomplete
    def __init__(
        self,
        *,
        conn_id: str | None = None,
        database: str | None = None,
        hook_params: dict | None = None,
        retry_on_failure: bool = True,
        **kwargs,
    ) -> None: ...
    def get_db_hook(self) -> DbApiHook: ...

class SQLExecuteQueryOperator(BaseSQLOperator):
    def _raise_exception(self, exception_string: str) -> Incomplete: ...
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    ui_color: str
    sql: Incomplete
    autocommit: Incomplete
    parameters: Incomplete
    handler: Incomplete
    split_statements: Incomplete
    return_last: Incomplete
    show_return_value_in_logs: Incomplete
    def __init__(
        self,
        *,
        sql: str | list[str],
        autocommit: bool = False,
        parameters: Mapping | Iterable | None = None,
        handler: Callable[[Any], Optional[list[tuple]]] = ...,
        conn_id: str | None = None,
        database: str | None = None,
        split_statements: bool | None = None,
        return_last: bool = True,
        show_return_value_in_logs: bool = False,
        **kwargs,
    ) -> None: ...
    def execute(self, context): ...
    def prepare_template(self) -> None: ...
    def get_openlineage_facets_on_start(self) -> OperatorLineage | None: ...
    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage | None: ...

class SQLColumnCheckOperator(BaseSQLOperator):
    template_fields: Sequence[str]
    template_fields_renderers: Incomplete
    sql_check_template: str
    column_checks: Incomplete
    table: Incomplete
    column_mapping: Incomplete
    partition_clause: Incomplete
    accept_none: Incomplete
    sql: Incomplete
    def __init__(
        self,
        *,
        table: str,
        column_mapping: dict[str, dict[str, Any]],
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        accept_none: bool = True,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...

class SQLTableCheckOperator(BaseSQLOperator):
    template_fields: Sequence[str]
    template_fields_renderers: Incomplete
    sql_check_template: str
    table: Incomplete
    checks: Incomplete
    partition_clause: Incomplete
    sql: Incomplete
    def __init__(
        self,
        *,
        table: str,
        checks: dict[str, dict[str, Any]],
        partition_clause: str | None = None,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...

class SQLCheckOperator(BaseSQLOperator):
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    ui_color: str
    sql: Incomplete
    parameters: Incomplete
    def __init__(
        self,
        *,
        sql: str,
        conn_id: str | None = None,
        database: str | None = None,
        parameters: Iterable | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...

class SQLValueCheckOperator(BaseSQLOperator):
    __mapper_args__: Incomplete
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    ui_color: str
    sql: Incomplete
    pass_value: Incomplete
    tol: Incomplete
    has_tolerance: Incomplete
    def __init__(
        self,
        *,
        sql: str,
        pass_value: Any,
        tolerance: Any = None,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ) -> None: ...
    def check_value(self, records) -> None: ...
    def execute(self, context: Context): ...

class SQLIntervalCheckOperator(BaseSQLOperator):
    __mapper_args__: Incomplete
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    ui_color: str
    ratio_formulas: Incomplete
    ratio_formula: Incomplete
    ignore_zero: Incomplete
    table: Incomplete
    metrics_thresholds: Incomplete
    metrics_sorted: Incomplete
    date_filter_column: Incomplete
    days_back: Incomplete
    sql1: Incomplete
    sql2: Incomplete
    def __init__(
        self,
        *,
        table: str,
        metrics_thresholds: dict[str, int],
        date_filter_column: str | None = "ds",
        days_back: SupportsAbs[int] = -7,
        ratio_formula: str | None = "max_over_min",
        ignore_zero: bool = True,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...

class SQLThresholdCheckOperator(BaseSQLOperator):
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    sql: Incomplete
    min_threshold: Incomplete
    max_threshold: Incomplete
    def __init__(
        self,
        *,
        sql: str,
        min_threshold: Any,
        max_threshold: Any,
        conn_id: str | None = None,
        database: str | None = None,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...
    def push(self, meta_data) -> None: ...

class BranchSQLOperator(BaseSQLOperator, SkipMixin):
    template_fields: Sequence[str]
    template_ext: Sequence[str]
    template_fields_renderers: Incomplete
    ui_color: str
    ui_fgcolor: str
    sql: Incomplete
    parameters: Incomplete
    follow_task_ids_if_true: Incomplete
    follow_task_ids_if_false: Incomplete
    def __init__(
        self,
        *,
        sql: str,
        follow_task_ids_if_true: list[str],
        follow_task_ids_if_false: list[str],
        conn_id: str = "default_conn_id",
        database: str | None = None,
        parameters: Iterable | Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None: ...
    def execute(self, context: Context): ...
