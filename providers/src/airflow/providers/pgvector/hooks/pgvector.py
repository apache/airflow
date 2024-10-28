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

from airflow.providers.postgres.hooks.postgres import PostgresHook


class PgVectorHook(PostgresHook):
    """Extend PostgresHook for working with PostgreSQL and pgvector extension for vector data types."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize a PgVectorHook."""
        super().__init__(*args, **kwargs)

    def create_table(
        self, table_name: str, columns: list[str], if_not_exists: bool = True
    ) -> None:
        """
        Create a table in the Postgres database.

        :param table_name: The name of the table to create.
        :param columns: A list of column definitions for the table.
        :param if_not_exists: If True, only create the table if it does not already exist.
        """
        create_table_sql = "CREATE TABLE"
        if if_not_exists:
            create_table_sql = f"{create_table_sql} IF NOT EXISTS"
        create_table_sql = f"{create_table_sql} {table_name} ({', '.join(columns)})"
        self.run(create_table_sql)

    def create_extension(self, extension_name: str, if_not_exists: bool = True) -> None:
        """
        Create a PostgreSQL extension.

        :param extension_name: The name of the extension to create.
        :param if_not_exists: If True, only create the extension if it does not already exist.
        """
        create_extension_sql = "CREATE EXTENSION"
        if if_not_exists:
            create_extension_sql = f"{create_extension_sql} IF NOT EXISTS"
        create_extension_sql = f"{create_extension_sql} {extension_name}"
        self.run(create_extension_sql)

    def drop_table(self, table_name: str, if_exists: bool = True) -> None:
        """
        Drop a table from the Postgres database.

        :param table_name: The name of the table to drop.
        :param if_exists: If True, only drop the table if it exists.
        """
        drop_table_sql = "DROP TABLE"
        if if_exists:
            drop_table_sql = f"{drop_table_sql} IF EXISTS"
        drop_table_sql = f"{drop_table_sql} {table_name}"
        self.run(drop_table_sql)

    def truncate_table(self, table_name: str, restart_identity: bool = True) -> None:
        """
        Truncate a table, removing all rows.

        :param table_name: The name of the table to truncate.
        :param restart_identity: If True, restart the serial sequence if the table has one.
        """
        truncate_sql = f"TRUNCATE TABLE {table_name}"
        if restart_identity:
            truncate_sql = f"{truncate_sql} RESTART IDENTITY"
        self.run(truncate_sql)
