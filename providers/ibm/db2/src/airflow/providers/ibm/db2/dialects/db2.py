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

from airflow.providers.common.sql.dialects.dialect import Dialect, T


class Db2Dialect(Dialect):
    """
    Db2-specific SQL dialect implementation.

    Provides Db2-specific SQL generation, particularly for MERGE (upsert) operations.
    """

    @property
    def name(self) -> str:
        return "db2"

    @lru_cache(maxsize=256)
    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str] | None:
        """
        Get the table's primary key columns using DB2 system catalog.

        :param table: Name of the target table
        :param schema: Name of the target schema (optional)
        :return: Primary key columns list
        """
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        table = self.unescape_word(table) or table
        schema = self.unescape_word(schema) if schema else None

        # Query DB2 system catalog for primary key columns
        query = """
            SELECT COLNAME
            FROM SYSCAT.KEYCOLUSE
            WHERE TABSCHEMA = ? AND TABNAME = ? AND CONSTNAME IN (
                SELECT CONSTNAME
                FROM SYSCAT.TABCONST
                WHERE TABSCHEMA = ? AND TABNAME = ? AND TYPE = 'P'
            )
            ORDER BY COLSEQ
        """
        pk_columns = [row[0] for row in self.get_records(query, (schema, table, schema, table))]
        return pk_columns or None

    @staticmethod
    def _to_row(row):
        """Convert DB2 system catalog row to column metadata dictionary."""
        return {
            "name": row[0],
            "type": row[1],
            "nullable": row[2] == "Y",
            "default": row[3],
            "identity": row[4] == "Y",
        }

    @lru_cache(maxsize=256)
    def get_column_names(
        self,
        table: str,
        schema: str | None = None,
        predicate: Callable[[T], bool] | None = None,
    ) -> list[str] | None:
        """
        Get column names for a table using DB2 system catalog.

        By default, excludes identity columns (GENERATED ALWAYS AS IDENTITY)
        as they should not be specified in INSERT statements.

        :param table: Name of the target table
        :param schema: Name of the target schema (optional)
        :param predicate: Optional filter function for columns (defaults to excluding identity columns)
        :return: List of column names
        """
        # Default predicate excludes identity columns
        if predicate is None:
            predicate = lambda col: not col["identity"]
        if schema is None:
            table, schema = self.extract_schema_from_table(table)
        table = self.unescape_word(table) or table
        schema = self.unescape_word(schema) if schema else None

        # Query DB2 system catalog for column information
        query = """
            SELECT COLNAME, TYPENAME, NULLS, DEFAULT, IDENTITY
            FROM SYSCAT.COLUMNS
            WHERE TABSCHEMA = ? AND TABNAME = ?
            ORDER BY COLNO
        """
        column_names = []
        for row in map(
            self._to_row,
            self.get_records(query, (schema, table)),
        ):
            if predicate(row):
                column_names.append(row["name"])

        self.log.debug("Column names for table '%s': %s", table, column_names)
        return column_names

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate MERGE SQL statement for Db2.

        Db2 doesn't support REPLACE INTO syntax. Instead, it uses
        MERGE statements for upsert operations.

        :param table: Name of the target table
        :param values: The row to insert/update
        :param target_fields: The names of the columns to fill in the table
        :param kwargs: Additional parameters, including:
            - replace_index: List of column names to use for matching (defaults to primary keys)
        :return: The generated MERGE SQL statement

        Example generated SQL:
            MERGE INTO table AS t
            USING (VALUES (?, ?, ?)) AS s(col1, col2, col3)
            ON t.pk = s.pk
            WHEN MATCHED THEN
                UPDATE SET t.col1 = s.col1, t.col2 = s.col2
            WHEN NOT MATCHED THEN
                INSERT (col1, col2, col3) VALUES (s.col1, s.col2, s.col3)
        """
        # Validate target_fields
        if not target_fields:
            raise ValueError("Db2 MERGE syntax requires column names")

        # Get the columns to use for matching (ON clause)
        replace_index = kwargs.get("replace_index") or self.get_primary_keys(table)

        if not replace_index:
            raise ValueError(
                f"Cannot generate MERGE statement for table '{table}': "
                "no primary key found and no replace_index provided. "
                "Specify replace_index parameter with column names to use for matching."
            )

        # Convert string to list if needed
        if isinstance(replace_index, str):
            replace_index = [replace_index]

        # Escape column names
        escaped_target_fields = [self.escape_word(field) for field in target_fields]
        escaped_replace_index = [self.escape_word(field) for field in replace_index]

        # Build placeholders for VALUES clause
        placeholders = self._joined_placeholders(values)

        # Build column list for USING clause
        columns = ", ".join(escaped_target_fields)

        # Build ON clause (matching condition)
        on_conditions = [f"t.{col} = s.{col}" for col in escaped_replace_index]
        on_clause = " AND ".join(on_conditions)

        # Build UPDATE SET clause (exclude primary key columns from update)
        update_fields = [field for field in escaped_target_fields if field not in escaped_replace_index]
        if update_fields:
            update_assignments = [f"t.{field} = s.{field}" for field in update_fields]
            update_clause = ", ".join(update_assignments)
            when_matched = f"WHEN MATCHED THEN UPDATE SET {update_clause}"
        else:
            # If all columns are in the primary key, no update needed
            when_matched = ""

        # Build INSERT clause
        insert_cols = ", ".join(escaped_target_fields)
        insert_vals = ", ".join([f"s.{field}" for field in escaped_target_fields])

        # Construct the full MERGE statement
        sql = f"MERGE INTO {table} AS t USING (VALUES ({placeholders})) AS s({columns}) ON {on_clause}"
        if when_matched:
            sql += f" {when_matched}"
        sql += f" WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        return sql
