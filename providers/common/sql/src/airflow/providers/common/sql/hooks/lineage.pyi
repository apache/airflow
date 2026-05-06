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
"""
Definition of the public interface for
airflow.providers.common.sql.src.airflow.providers.common.sql.hooks.lineage.
"""

from enum import Enum
from typing import Any

from airflow.providers.common.compat.lineage.hook import LineageContext

class SqlJobHookLineageExtra(str, Enum):
    KEY = "sql_job"
    VALUE__SQL_STATEMENT = "sql"
    VALUE__SQL_STATEMENT_PARAMETERS = "sql_parameters"
    VALUE__JOB_ID = "job_id"
    VALUE__ROW_COUNT = "row_count"
    VALUE__DEFAULT_DB = "default_db"
    VALUE__DEFAULT_SCHEMA = "default_schema"
    VALUE__EXTRA = "extra"
    @classmethod
    def value_keys(cls) -> tuple[SqlJobHookLineageExtra, ...]: ...

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
) -> None: ...
