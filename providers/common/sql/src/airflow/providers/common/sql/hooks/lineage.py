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
from enum import Enum
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector
from airflow.providers.common.sql.hooks.handlers import get_row_count

if TYPE_CHECKING:
    from airflow.providers.common.compat.lineage.hook import LineageContext


log = logging.getLogger(__name__)


class SqlJobHookLineageExtra(str, Enum):
    """
    Keys for the SQL job hook-level lineage extra entry.

    Reported via ``get_hook_lineage_collector().add_extra()``. ``KEY`` is the
    extra entry key; ``VALUE__*`` are the keys inside the value dict (one entry
    per SQL statement so job_id, SQL text, row count, default_db, etc. stay stitched).
    """

    KEY = "sql_job"
    VALUE__SQL_STATEMENT = "sql"
    VALUE__SQL_STATEMENT_PARAMETERS = "sql_parameters"
    VALUE__JOB_ID = "job_id"
    VALUE__ROW_COUNT = "row_count"
    VALUE__DEFAULT_DB = "default_db"
    VALUE__DEFAULT_SCHEMA = "default_schema"
    VALUE__EXTRA = "extra"

    @classmethod
    def value_keys(cls) -> tuple[SqlJobHookLineageExtra, ...]:
        """Value-dict keys only (KEY excluded). Use when iterating or validating the value dict."""
        return (
            cls.VALUE__SQL_STATEMENT,
            cls.VALUE__SQL_STATEMENT_PARAMETERS,
            cls.VALUE__JOB_ID,
            cls.VALUE__ROW_COUNT,
            cls.VALUE__DEFAULT_DB,
            cls.VALUE__DEFAULT_SCHEMA,
            cls.VALUE__EXTRA,
        )


def send_sql_hook_lineage(
    *,
    context: LineageContext,
    sql: str | list[str],
    sql_parameters: Any = None,
    cur: Any = None,
    job_id: str | None = None,
    row_count: int | None = None,
    default_db: str | None = None,
    default_schema: str | None = None,
    extra: dict[str, Any] | None = None,
) -> None:
    """
    Report a single SQL execution to the hook lineage collector.

    Call this after running a SQL statement so that hook lineage collectors can associate the execution
    with the task. Each call produces one extra entry in the collector; when executing multiple statements
    in one hook run, one should call this function separately for each sql job, so that job_id, SQL text,
    row count, and other fields stay tied together per statement.

    Usable from any hook: pass the hook instance as ``context``. Not limited to
    ``DbApiHook`` subclasses.

    :param context: Lineage context, typically the hook instance. Must be valid for
        ``get_hook_lineage_collector().add_extra(context=..., ...)``.
    :param sql: The SQL statement that was executed (or a representative string).
    :param sql_parameters: Optional parameters bound to the statement.
    :param cur: Optional DB-API cursor after execution. If given, job_id is taken
        from ``query_id`` or ``sfqid`` when not provided explicitly, and row_count
        from ``cur.rowcount`` when applicable (PEP 249).
    :param job_id: Explicit job ID; used instead of cursor-derived value when set.
    :param row_count: Explicit row count; used instead of cursor-derived value when set.
    :param default_db: Default database/catalog name for this execution context.
    :param default_schema: Default schema name for this execution context.
    :param extra: Optional additional key-value data to attach to this lineage entry.
    """
    try:
        sql = "; ".join(sql) if isinstance(sql, list) else sql
        value: dict[str, Any] = {SqlJobHookLineageExtra.VALUE__SQL_STATEMENT.value: sql}
        if sql_parameters:
            value[SqlJobHookLineageExtra.VALUE__SQL_STATEMENT_PARAMETERS.value] = sql_parameters

        # Get SQL job_id: either explicitly or from cursor
        if job_id is not None:
            value[SqlJobHookLineageExtra.VALUE__JOB_ID.value] = job_id
        elif cur is not None:
            for attr in ("query_id", "sfqid"):
                if (cursor_job_id := getattr(cur, attr, None)) is not None:
                    value[SqlJobHookLineageExtra.VALUE__JOB_ID.value] = cursor_job_id
                    break

        # Get row count: either explicitly or from cursor
        if row_count is None and cur is not None:
            row_count = get_row_count(cur)
        if row_count is not None and row_count >= 0:
            value[SqlJobHookLineageExtra.VALUE__ROW_COUNT.value] = row_count

        if default_db is not None:
            value[SqlJobHookLineageExtra.VALUE__DEFAULT_DB.value] = default_db
        if default_schema is not None:
            value[SqlJobHookLineageExtra.VALUE__DEFAULT_SCHEMA.value] = default_schema
        if extra:
            value[SqlJobHookLineageExtra.VALUE__EXTRA.value] = extra

        get_hook_lineage_collector().add_extra(
            context=context,
            key=SqlJobHookLineageExtra.KEY.value,
            value=value,
        )
    except Exception as e:
        log.warning("Sending SQL hook level lineage failed: %s", f"{e.__class__.__name__}: {str(e)}")
        log.debug("Exception details:", exc_info=True)
