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

import re
from typing import TYPE_CHECKING

from methodtools import lru_cache

from airflow.providers.common.sql.dialects.dialect import Dialect, T

if TYPE_CHECKING:
    from collections.abc import Callable

# A plain SAS name: what a library or member may be when written into the SQL.
_SAS_IDENTIFIER_RE = re.compile(r"\w+")


class SASDialect(Dialect):
    """SAS dialect implementation."""

    @property
    def name(self) -> str:
        return "sas"

    @staticmethod
    def _to_row(row):
        notnull = row[2] if row[2] else "no"
        return {
            "name": row[0],
            "type": row[1],
            "nullable": notnull.casefold() != "yes",
            "default": None,
            "autoincrement": False,
            "identity": False,
            "ordinal": int(row[3]) if row[3] is not None else 0,
        }

    @lru_cache(maxsize=None)
    def get_column_names(
        self,
        table: str,
        schema: str | None = None,
        predicate: Callable[[T], bool] = lambda column: True,
    ) -> list[str] | None:
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        table = self.unescape_word(table) or table
        schema = self.unescape_word(schema) if schema else None
        # The probe names the table as an identifier, which a bind parameter
        # cannot do (it becomes a quoted literal), so both names are checked
        # to be plain SAS names before they are written into the SQL.
        for name in (schema, table):
            if not name or not _SAS_IDENTIFIER_RE.fullmatch(name):
                raise ValueError(
                    f"SAS table must be <library>.<member> with plain SAS names, got {schema!r}.{table!r}"
                )
        query = f"""
                SELECT name, type, notnull, varnum
                FROM (SELECT "X" AS name, "X" AS type, "X" AS notnull, 0 AS varnum FROM {schema}.{table}(obs=0)) probe
                UNION ALL
                SELECT name, type, notnull, varnum FROM DICTIONARY.COLUMNS
                WHERE LIBNAME=%s AND MEMNAME=%s
        """
        column_names = [
            row["name"]
            for row in map(
                self._to_row,
                self.get_records(query, (schema, table)),
            )
            if predicate(row)
        ]
        self.log.debug("Column names for table '%s': %s", table, column_names)
        return column_names
