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
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler


class MsSqlDialect(Dialect):
    """Microsoft SQL Server dialect implementation."""

    @lru_cache(maxsize=None)
    def get_primary_keys(self, table: str) -> list[str] | None:
        primary_keys = self.run(
            f"""
                SELECT c.name
                FROM sys.columns c
                WHERE c.object_id =  OBJECT_ID('{table}')
                    AND EXISTS (SELECT 1 FROM sys.index_columns ic
                        INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        WHERE i.is_primary_key = 1
                        AND ic.object_id = c.object_id
                        AND ic.column_id = c.column_id);
                """,
            handler=fetch_all_handler,
        )
        primary_keys = [pk[0] for pk in primary_keys] if primary_keys else []  # type: ignore
        self.log.debug("Primary keys for table '%s': %s", table, primary_keys)
        return primary_keys  # type: ignore

    def generate_replace_sql(self, table, values, target_fields, **kwargs) -> str:
        primary_keys = self.get_primary_keys(table)
        columns = [
            self.escape_reserved_word(target_field)
            for target_field in target_fields
            if target_field in set(target_fields).difference(set(primary_keys))
        ]

        self.log.debug("primary_keys: %s", primary_keys)
        self.log.debug("columns: %s", columns)

        return f"""MERGE INTO {table} AS target
            USING (SELECT {', '.join(map(lambda column: f'{self.placeholder} AS {column}', target_fields))}) AS source
            ON {' AND '.join(map(lambda column: f'target.{self.escape_reserved_word(column)} = source.{column}', primary_keys))}
            WHEN MATCHED THEN
                UPDATE SET {', '.join(map(lambda column: f'target.{column} = source.{column}', columns))}
            WHEN NOT MATCHED THEN
                INSERT ({', '.join(target_fields)}) VALUES ({', '.join(map(lambda column: f'source.{self.escape_reserved_word(column)}', target_fields))});"""
