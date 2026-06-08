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

from airflow.providers.common.sql.dialects.dialect import Dialect


class Db2Dialect(Dialect):
    """
    Db2-specific SQL dialect implementation.

    Provides Db2-specific SQL generation, particularly for MERGE (upsert) operations.
    """

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
