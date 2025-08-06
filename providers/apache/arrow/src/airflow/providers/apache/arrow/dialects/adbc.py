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

from collections.abc import Callable

from methodtools import lru_cache

from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.sql import T


class AdbcDialect(Dialect):
    @lru_cache(maxsize=None)
    def get_column_names(
        self, table: str, schema: str | None = None, predicate: Callable[[T], bool] = lambda column: True
    ) -> list[str] | None:
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        # column_names = list(
        #     column["name"]
        #     for column in filter(
        #         predicate,
        #
        #     )
        # )
        column_names = self.hook.get_conn().adbc_get_table_schema(
            table_name=table,
            db_schema_filter=schema,
        ).names
        self.log.info("Column names for table '%s': %s", table, column_names)
        return column_names
