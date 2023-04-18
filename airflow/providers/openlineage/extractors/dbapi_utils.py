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

import logging
from collections import defaultdict
from contextlib import closing
from typing import TYPE_CHECKING, Dict, Iterator, List

from attrs import define, field

from openlineage.client.facet import SchemaDatasetFacet, SchemaField
from openlineage.client.run import Dataset

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook


logger = logging.getLogger(__name__)

_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
# Use 'udt_name' which is the underlying type of column
# (ex: int4, timestamp, varchar, etc)
_UDT_NAME = 4
# Database is optional as 5th column
_TABLE_DATABASE = 5

TablesHierarchy = Dict[str, Dict[str, List[str]]]


@define
class TableSchema:
    """Temporary object used to construct OpenLineage Dataset"""

    table: str = field()
    schema: str | None = field()
    database: str | None = field()
    fields: list[SchemaField] = field()

    def to_dataset(self, namespace: str, database: str | None = None) -> Dataset:
        # Prefix the table name with database and schema name using
        # the format: {database_name}.{table_schema}.{table_name}.
        name = ".".join(
            filter(
                lambda x: x is not None,  # type: ignore
                [self.database if self.database else database, self.schema, self.table],
            )
        )
        return Dataset(
            namespace=namespace,
            name=name,
            facets={"schema": SchemaDatasetFacet(fields=self.fields)} if len(self.fields) is not None else {},
        )


def execute_query_on_hook(hook: BaseHook, query: str) -> Iterator[tuple]:
    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            return cursor.execute(query).fetchall()


def get_table_schemas(
    hook: BaseHook,
    namespace: str,
    database: str | None,
    in_query: str | None,
    out_query: str | None,
) -> tuple[list[Dataset], ...]:
    """
    This function queries database for table schemas using provided hook.
    Responsibility to provide queries for this function is on particular extractors.
    If query for input or output table isn't provided, the query is skipped.
    """
    in_datasets: list[Dataset] = []
    out_datasets: list[Dataset] = []

    # Do not query if we did not get both queries
    if not in_query and not out_query:
        return [], []

    with closing(hook.get_conn()) as conn:
        with closing(conn.cursor()) as cursor:
            if in_query:
                cursor.execute(in_query)
                in_datasets += [x.to_dataset(namespace, database) for x in parse_query_result(cursor)]
            if out_query:
                cursor.execute(out_query)
                out_datasets += [x.to_dataset(namespace, database) for x in parse_query_result(cursor)]
    return in_datasets, out_datasets


def parse_query_result(cursor) -> list[TableSchema]:
    schemas: dict = {}
    columns: dict = defaultdict(list)
    for row in cursor.fetchall():
        table_schema_name: str = row[_TABLE_SCHEMA]
        table_name: str = row[_TABLE_NAME]
        table_column: SchemaField = SchemaField(
            name=row[_COLUMN_NAME],
            type=row[_UDT_NAME],
            description=None,
        )
        ordinal_position = row[_ORDINAL_POSITION]
        try:
            table_database = row[_TABLE_DATABASE]
        except IndexError:
            table_database = None

        # Attempt to get table schema
        table_key = ".".join(filter(None, [table_database, table_schema_name, table_name]))

        schemas[table_key] = TableSchema(
            table=table_name, schema=table_schema_name, database=table_database, fields=[]
        )
        columns[table_key].append((ordinal_position, table_column))

    for schema in schemas.values():
        table_key = ".".join(filter(None, [schema.database, schema.schema, schema.table]))
        schema.fields = [x for _, x in sorted(columns[table_key])]

    return list(schemas.values())


def create_information_schema_query(
    columns: list[str],
    information_schema_table_name: str,
    tables_hierarchy: TablesHierarchy,
    uppercase_names: bool = False,
    allow_trailing_semicolon: bool = True,
) -> str:
    """This function creates query for getting table schemas from information schema."""
    sqls = []
    for db, schema_mapping in tables_hierarchy.items():
        filter_clauses = create_filter_clauses(schema_mapping, uppercase_names)
        source = information_schema_table_name
        if db:
            source = f"{db.upper() if uppercase_names else db}." f"{source}"
        table_columns = [f'{source}."{column}"' for column in columns]
        sqls.append(
            f"SELECT {', '.join(table_columns)} " f"FROM {source} " f"WHERE {' OR '.join(filter_clauses)}"
        )
    sql = " UNION ALL ".join(sqls)

    # For some databases such as Trino, trailing semicolon can cause a syntax error.
    if allow_trailing_semicolon:
        sql += ";"

    return sql


def create_filter_clauses(schema_mapping, uppercase_names: bool = False) -> list[str]:
    filter_clauses = []
    for schema, tables in schema_mapping.items():
        table_names = ",".join(
            map(
                lambda name: f"'{name.upper() if uppercase_names else name}'",
                tables,
            )
        )
        if schema:
            filter_clauses.append(
                f"( table_schema = '{schema.upper() if uppercase_names else schema}' "
                f"AND table_name IN ({table_names}) )"
            )
        else:
            filter_clauses.append(f"( table_name IN ({table_names}) )")
    return filter_clauses
