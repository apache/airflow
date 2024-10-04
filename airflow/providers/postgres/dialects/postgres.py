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

from methodtools import lru_cache

from airflow.providers.common.sql.dialects.dialect import Dialect


class PostgresDialect(Dialect):
    """Postgres dialect implementation."""

    @lru_cache(maxsize=None)
    def get_primary_keys(self, table: str, schema: str | None = None) -> list[str]:
        """
        Get the table's primary key.

        :param table: Name of the target table
        :param schema: Name of the target schema, public by default
        :return: Primary key columns list
        """
        sql = """
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'PRIMARY KEY'
            and kcu.table_schema = %s
            and kcu.table_name = %s
        """
        pk_columns = [row[0] for row in self.get_records(sql, (schema, table))]
        return pk_columns or None

    def generate_insert_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :return: The generated INSERT or REPLACE SQL statement
        """
        placeholders = [
            self.placeholder,
        ] * len(values)

        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""

        return f"INSERT INTO {table} {target_fields_fragment} VALUES ({','.join(placeholders)})"

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        """
        Generate the REPLACE SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :return: The generated INSERT or REPLACE SQL statement
        """
        if not target_fields:
            raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")

        replace_index = kwargs.get("replace_index") or self.get_primary_keys(table)

        if isinstance(replace_index, str):
            replace_index = [replace_index]

        sql = self.generate_insert_sql(table, values, target_fields, **kwargs)
        on_conflict_str = f" ON CONFLICT ({', '.join(replace_index)})"
        replace_target = [f for f in target_fields if f not in replace_index]

        if replace_target:
            replace_target_str = ", ".join(f"{col} = excluded.{col}" for col in replace_target)
            sql += f"{on_conflict_str} DO UPDATE SET {replace_target_str}"
        else:
            sql += f"{on_conflict_str} DO NOTHING"

        return sql
