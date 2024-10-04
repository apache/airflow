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

from typing import TYPE_CHECKING, Any, Callable, Iterable, Mapping, TypeVar

from methodtools import lru_cache

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector

T = TypeVar("T")


class Dialect(LoggingMixin):
    """Generic dialect implementation."""

    def __init__(self, hook, **kwargs) -> None:
        super().__init__(**kwargs)

        from airflow.providers.common.sql.hooks.sql import DbApiHook

        if not isinstance(hook, DbApiHook):
            raise TypeError(f"hook must be an instance of {DbApiHook.__class__.__name__}")

        self.hook: DbApiHook = hook

    @property
    def placeholder(self) -> str:
        return self.hook.placeholder

    @property
    def inspector(self) -> Inspector:
        return self.hook.inspector

    @property
    def _insert_statement_format(self) -> str:
        return self.hook._insert_statement_format  # type: ignore

    @property
    def _replace_statement_format(self) -> str:
        return self.hook._replace_statement_format  # type: ignore

    @classmethod
    def _extract_schema_from_table(cls, table: str) -> list[str]:
        return table.split(".")[::-1]

    @lru_cache(maxsize=None)
    def get_column_names(self, table: str) -> list[str]:
        column_names = list(
            column["name"] for column in self.inspector.get_columns(*self._extract_schema_from_table(table))
        )
        self.log.debug("Column names for table '%s': %s", table, column_names)
        return column_names

    @lru_cache(maxsize=None)
    def get_primary_keys(self, table: str) -> list[str]:
        primary_keys = self.inspector.get_pk_constraint(*self._extract_schema_from_table(table)).get(
            "constrained_columns", []
        )
        self.log.debug("Primary keys for table '%s': %s", table, primary_keys)
        return primary_keys

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping[str, Any] | None = None,
        handler: Callable[[Any], T] | None = None,
        split_statements: bool = False,
        return_last: bool = True,
    ) -> tuple | list[tuple] | list[list[tuple] | tuple] | None:
        return self.hook.run(sql, autocommit, parameters, handler, split_statements, return_last)

    def generate_insert_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated INSERT SQL statement
        """
        placeholders = [
            self.placeholder,
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ""

        return self._insert_statement_format.format(table, target_fields, ",".join(placeholders))

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the REPLACE SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated REPLACE SQL statement
        """
        placeholders = [
            self.placeholder,
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ""

        return self._replace_statement_format.format(table, target_fields, ",".join(placeholders))
