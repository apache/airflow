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
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, Callable, TypeVar

from methodtools import lru_cache

from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from sqlalchemy.engine import Inspector

T = TypeVar("T")


class Dialect(LoggingMixin):
    """Generic dialect implementation."""

    pattern = re.compile(r"[^\w]")

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
    def insert_statement_format(self) -> str:
        return self.hook.insert_statement_format

    @property
    def replace_statement_format(self) -> str:
        return self.hook.replace_statement_format

    @property
    def escape_word_format(self) -> str:
        return self.hook.escape_word_format

    @property
    def escape_column_names(self) -> bool:
        return self.hook.escape_column_names

    def escape_word(self, word: str) -> str:
        """
        Escape the word if necessary.

        If the word is a reserved word or contains special characters or if the ``escape_column_names``
        property is set to True in connection extra field, then the given word will be escaped.

        :param word: Name of the column
        :return: The escaped word
        """
        if word != self.escape_word_format.format(self.unescape_word(word)) and (
            self.escape_column_names or word.casefold() in self.reserved_words or self.pattern.search(word)
        ):
            return self.escape_word_format.format(word)
        return word

    def unescape_word(self, word: str | None) -> str | None:
        """
        Remove escape characters from each part of a dotted identifier (e.g., schema.table).

        :param word: Escaped schema, table, or column name, potentially with multiple segments.
        :return: The word without escaped characters.
        """
        if not word:
            return word

        escape_char_start = self.escape_word_format[0]
        escape_char_end = self.escape_word_format[-1]

        def unescape_part(part: str) -> str:
            if part.startswith(escape_char_start) and part.endswith(escape_char_end):
                return part[1:-1]
            return part

        return ".".join(map(unescape_part, word.split(".")))

    @classmethod
    def extract_schema_from_table(cls, table: str) -> tuple[str, str | None]:
        parts = table.split(".")
        return tuple(parts[::-1]) if len(parts) == 2 else (table, None)  # type: ignore[return-value]

    @lru_cache(maxsize=None)
    def get_column_names(
        self, table: str, schema: str | None = None, predicate: Callable[[T], bool] = lambda column: True
    ) -> list[str] | None:
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        column_names = list(
            column["name"]
            for column in filter(
                predicate,
                self.inspector.get_columns(
                    table_name=self.unescape_word(table),
                    schema=self.unescape_word(schema) if schema else None,
                ),
            )
        )
        self.log.debug("Column names for table '%s': %s", table, column_names)
        return column_names

    @lru_cache(maxsize=None)
    def get_target_fields(self, table: str, schema: str | None = None) -> list[str] | None:
        target_fields = self.get_column_names(
            table,
            schema,
            lambda column: not column.get("identity", False) and not column.get("autoincrement", False),
        )
        self.log.debug("Target fields for table '%s': %s", table, target_fields)
        return target_fields

    @lru_cache(maxsize=None)
    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str] | None:
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        primary_keys = self.inspector.get_pk_constraint(
            table_name=self.unescape_word(table),
            schema=self.unescape_word(schema) if schema else None,
        ).get("constrained_columns", [])
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

    def get_records(
        self,
        sql: str | list[str],
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> Any:
        return self.hook.get_records(sql=sql, parameters=parameters)

    @property
    def reserved_words(self) -> set[str]:
        return self.hook.reserved_words

    def _joined_placeholders(self, values) -> str:
        placeholders = [
            self.placeholder,
        ] * len(values)
        return ",".join(placeholders)

    def _joined_target_fields(self, target_fields) -> str:
        if target_fields:
            target_fields = ", ".join(map(self.escape_word, target_fields))
            return f"({target_fields})"
        return ""

    def generate_insert_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated INSERT SQL statement
        """
        return self.insert_statement_format.format(
            table, self._joined_target_fields(target_fields), self._joined_placeholders(values)
        )

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the REPLACE SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :return: The generated REPLACE SQL statement
        """
        return self.replace_statement_format.format(
            table, self._joined_target_fields(target_fields), self._joined_placeholders(values)
        )
