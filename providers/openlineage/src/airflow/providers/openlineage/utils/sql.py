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
from enum import IntEnum
from typing import TYPE_CHECKING

from attrs import define
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset

from airflow.exceptions import AirflowOptionalProviderFeatureException

if TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.engine import Engine
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.providers.common.compat.sdk import BaseHook


log = logging.getLogger(__name__)


class ColumnIndex(IntEnum):
    """Enumerates the indices of columns in information schema view."""

    SCHEMA = 0
    TABLE_NAME = 1
    COLUMN_NAME = 2
    ORDINAL_POSITION = 3
    # Use 'udt_name' which is the underlying type of column
    UDT_NAME = 4
    # Database is optional as 6th column
    DATABASE = 5


TablesHierarchy = dict[str | None, dict[str | None, list[str]]]


@define
class TableSchema:
    """Temporary object used to construct OpenLineage Dataset."""

    table: str
    schema: str | None
    database: str | None
    fields: list[schema_dataset.SchemaDatasetFacetFields]

    def to_dataset(self, namespace: str, database: str | None = None, schema: str | None = None) -> Dataset:
        # Prefix the table name with database and schema name using
        # the format: {database_name}.{table_schema}.{table_name}.
        name = ".".join(
            part
            for part in [self.database or database, self.schema or schema, self.table]
            if part is not None
        )
        return Dataset(
            namespace=namespace,
            name=name,
            facets={"schema": schema_dataset.SchemaDatasetFacet(fields=self.fields)} if self.fields else {},
        )


def get_table_schemas(
    hook: BaseHook,
    namespace: str,
    schema: str | None,
    database: str | None,
    in_query: str | None,
    out_query: str | None,
) -> tuple[list[Dataset], list[Dataset]]:
    """
    Query database for table schemas.

    Uses provided hook. Responsibility to provide queries for this function is on particular extractors.
    If query for input or output table isn't provided, the query is skipped.
    """
    # Do not query if we did not get both queries
    if not in_query and not out_query:
        return [], []

    log.debug("Starting to query database for table schemas")
    with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cursor:
        if in_query:
            cursor.execute(in_query)
            in_datasets = [x.to_dataset(namespace, database, schema) for x in parse_query_result(cursor)]
        else:
            in_datasets = []
        if out_query:
            cursor.execute(out_query)
            out_datasets = [x.to_dataset(namespace, database, schema) for x in parse_query_result(cursor)]
        else:
            out_datasets = []
    log.debug("Got table schema query result from database.")
    return in_datasets, out_datasets


def parse_query_result(cursor) -> list[TableSchema]:
    """
    Fetch results from DB-API 2.0 cursor and creates list of table schemas.

    For each row it creates :class:`TableSchema`.
    """
    schemas: dict = {}
    columns: dict = defaultdict(list)
    for row in cursor.fetchall():
        table_schema_name: str = row[ColumnIndex.SCHEMA]
        table_name: str = row[ColumnIndex.TABLE_NAME]
        table_column = schema_dataset.SchemaDatasetFacetFields(
            name=row[ColumnIndex.COLUMN_NAME],
            type=row[ColumnIndex.UDT_NAME],
            description=None,
        )
        ordinal_position = row[ColumnIndex.ORDINAL_POSITION]
        try:
            table_database = row[ColumnIndex.DATABASE]
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
    use_flat_cross_db_query: bool = False,
    sqlalchemy_engine: Engine | None = None,
) -> str:
    """Create query for getting table schemas from information schema."""
    try:
        from sqlalchemy import Column, MetaData, Table, union_all
    except ImportError:
        raise AirflowOptionalProviderFeatureException(
            "sqlalchemy is required for SQL schema query generation. "
            "Install it with: pip install 'apache-airflow-providers-openlineage[sqlalchemy]'"
        )
    metadata = MetaData()
    select_statements = []
    # Don't iterate over tables hierarchy, just pass it to query single information schema table
    if use_flat_cross_db_query:
        information_schema_table = Table(
            information_schema_table_name,
            metadata,
            *[Column(column) for column in columns],
            quote=False,
        )
        filter_clauses = create_filter_clauses(
            tables_hierarchy,
            information_schema_table,
            uppercase_names=uppercase_names,
        )
        select_statements.append(information_schema_table.select().filter(filter_clauses))
    else:
        for db, schema_mapping in tables_hierarchy.items():
            # Information schema table name is expected to be "< information_schema schema >.<view/table name>"
            # usually "information_schema.columns". In order to use table identifier correct for various table
            # we need to pass first part of dot-separated identifier as `schema` argument to `sqlalchemy.Table`.
            if db:
                # Use database as first part of table identifier.
                schema = db
                table_name = information_schema_table_name
            else:
                # When no database passed, use schema as first part of table identifier.
                schema, table_name = information_schema_table_name.split(".")
            information_schema_table = Table(
                table_name,
                metadata,
                *[Column(column) for column in columns],
                schema=schema,
                quote=False,
            )
            filter_clauses = create_filter_clauses(
                {None: schema_mapping},
                information_schema_table,
                uppercase_names=uppercase_names,
            )
            select_statements.append(information_schema_table.select().filter(filter_clauses))
    return str(
        union_all(*select_statements).compile(sqlalchemy_engine, compile_kwargs={"literal_binds": True})
    )


def create_filter_clauses(
    mapping: dict,
    information_schema_table: Table,
    uppercase_names: bool = False,
) -> ColumnElement[bool]:
    """
    Create comprehensive filter clauses for all tables in one database.

    :param mapping: a nested dictionary of database, schema names and list of tables in each
    :param information_schema_table: `sqlalchemy.Table` instance used to construct clauses
        For most SQL dbs it contains `table_name` and `table_schema` columns,
        therefore it is expected the table has them defined.
    :param uppercase_names: if True use schema and table names uppercase
    """
    try:
        from sqlalchemy import and_, or_
    except ImportError:
        raise AirflowOptionalProviderFeatureException(
            "sqlalchemy is required for SQL filter clause generation. "
            "Install it with: pip install 'apache-airflow-providers-openlineage[sqlalchemy]'"
        )
    table_schema_column_name = information_schema_table.columns[ColumnIndex.SCHEMA].name
    table_name_column_name = information_schema_table.columns[ColumnIndex.TABLE_NAME].name
    try:
        table_database_column_name = information_schema_table.columns[ColumnIndex.DATABASE].name
    except IndexError:
        table_database_column_name = ""

    filter_clauses = []
    for db, schema_mapping in mapping.items():
        schema_level_clauses = []
        for schema, tables in schema_mapping.items():
            filter_clause: ColumnElement[bool] = information_schema_table.c[table_name_column_name].in_(
                [name.upper() if uppercase_names else name for name in tables]
            )
            if schema:
                schema_upper = schema.upper() if uppercase_names else schema
                filter_clause = and_(
                    information_schema_table.c[table_schema_column_name] == schema_upper, filter_clause
                )
            schema_level_clauses.append(filter_clause)
        if db and table_database_column_name:
            db_upper = db.upper() if uppercase_names else db
            filter_clause = and_(
                information_schema_table.c[table_database_column_name] == db_upper, or_(*schema_level_clauses)
            )
            filter_clauses.append(filter_clause)
        else:
            filter_clauses.extend(schema_level_clauses)
    return or_(*filter_clauses)
