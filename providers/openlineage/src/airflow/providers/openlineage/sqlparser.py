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
from collections.abc import Callable
from typing import TYPE_CHECKING, TypedDict

import sqlparse
from attrs import define
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import column_lineage_dataset, extraction_error_run, sql_job
from openlineage.common.sql import DbTableMeta, SqlMeta, parse

from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.utils.sql import (
    TablesHierarchy,
    create_information_schema_query,
    get_table_schemas,
)
from airflow.providers.openlineage.utils.utils import should_use_external_connection
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from openlineage.client.facet_v2 import JobFacet, RunFacet
    from sqlalchemy.engine import Engine

    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.sdk import BaseHook

log = logging.getLogger(__name__)

DEFAULT_NAMESPACE = "default"
DEFAULT_INFORMATION_SCHEMA_COLUMNS = [
    "table_schema",
    "table_name",
    "column_name",
    "ordinal_position",
    "udt_name",
]
DEFAULT_INFORMATION_SCHEMA_TABLE_NAME = "information_schema.columns"


def default_normalize_name_method(name: str) -> str:
    return name.lower()


class GetTableSchemasParams(TypedDict):
    """get_table_schemas params."""

    normalize_name: Callable[[str], str]
    is_cross_db: bool
    information_schema_columns: list[str]
    information_schema_table: str
    use_flat_cross_db_query: bool
    is_uppercase_names: bool
    database: str | None


@define
class DatabaseInfo:
    """
    Contains database specific information needed to process SQL statement parse result.

    :param scheme: Scheme part of URI in OpenLineage namespace.
    :param authority: Authority part of URI in OpenLineage namespace.
        For most cases it should return `{host}:{port}` part of Airflow connection.
        See: https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
    :param database: Takes precedence over parsed database name.
    :param information_schema_columns: List of columns names from information schema table.
    :param information_schema_table_name: Information schema table name.
    :param use_flat_cross_db_query: Specifies whether a single, "global" information schema table should
        be used for cross-database queries (e.g., in Redshift), or if multiple, per-database "local"
        information schema tables should be queried individually.

        If True, assumes a single, universal information schema table is available
        (for example, in Redshift, the `SVV_REDSHIFT_COLUMNS` view)
        [https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_REDSHIFT_COLUMNS.html].
        In this mode, we query only `information_schema_table_name` directly.
        Depending on the `is_information_schema_cross_db` argument, you can also filter
        by database name in the WHERE clause.

        If False, treats each database as having its own local information schema table containing
        metadata for that database only. As a result, one query per database may be generated
        and then combined (often via `UNION ALL`).
        This approach is necessary for dialects that do not maintain a single global view of
        all metadata or that require per-database queries.
        Depending on the `is_information_schema_cross_db` argument, queries can
        include or omit database information in both identifiers and filters.

        See `is_information_schema_cross_db` which also affects how final queries are constructed.
    :param is_information_schema_cross_db: Specifies whether database information should be tracked
        and included in queries that retrieve schema information from the information_schema_table.
        In short, this determines whether queries are capable of spanning multiple databases.

        If True, database identifiers are included wherever applicable, allowing retrieval of
        metadata from more than one database. For instance, in Snowflake or MS SQL
        (where each database is treated as a top-level namespace), you might have a query like:

        ```
        SELECT ...
        FROM db1.information_schema.columns WHERE ...
        UNION ALL
        SELECT ...
        FROM db2.information_schema.columns WHERE ...
        ```

        In Redshift, setting this to True together with `use_flat_cross_db_query=True` allows
        adding database filters to the query, for example:

        ```
        SELECT ...
        FROM SVV_REDSHIFT_COLUMNS
        WHERE
        SVV_REDSHIFT_COLUMNS.database == db1  # This is skipped when False
        AND SVV_REDSHIFT_COLUMNS.schema == schema1
        AND SVV_REDSHIFT_COLUMNS.table IN (table1, table2)
        OR ...
        ```

        However, certain databases (e.g., PostgreSQL) do not permit true cross-database queries.
        In such dialects, enabling cross-database support may lead to errors or be unnecessary.
        Always consult your dialect's documentation or test sample queries to confirm if
        cross-database querying is supported.

        If False, database qualifiers are ignored, effectively restricting queries to a single
        database (or making the database-level qualifier optional). This is typically
        safer for databases that do not support cross-database operations or only provide a
        two-level namespace (schema + table) instead of a three-level one (database + schema + table).
        For example, some MySQL or PostgreSQL contexts might not need or permit cross-database queries at all.

        See `use_flat_cross_db_query` which also affects how final queries are constructed.
    :param is_uppercase_names: Specifies if database accepts only uppercase names (e.g. Snowflake).
    :param normalize_name_method: Method to normalize database, schema and table names.
        Defaults to `name.lower()`.
    """

    scheme: str
    authority: str | None = None
    database: str | None = None
    information_schema_columns: list[str] = DEFAULT_INFORMATION_SCHEMA_COLUMNS
    information_schema_table_name: str = DEFAULT_INFORMATION_SCHEMA_TABLE_NAME
    use_flat_cross_db_query: bool = False
    is_information_schema_cross_db: bool = False
    is_uppercase_names: bool = False
    normalize_name_method: Callable[[str], str] = default_normalize_name_method


def from_table_meta(
    table_meta: DbTableMeta, database: str | None, namespace: str, is_uppercase: bool
) -> Dataset:
    if table_meta.database:
        name = table_meta.qualified_name
    elif database:
        name = f"{database}.{table_meta.schema}.{table_meta.name}"
    else:
        name = f"{table_meta.schema}.{table_meta.name}"
    return Dataset(namespace=namespace, name=name if not is_uppercase else name.upper())


class SQLParser(LoggingMixin):
    """
    Interface for openlineage-sql.

    :param dialect: dialect specific to the database
    :param default_schema: schema applied to each table with no schema parsed
    """

    def __init__(self, dialect: str | None = None, default_schema: str | None = None) -> None:
        super().__init__()
        self.dialect = dialect
        self.default_schema = default_schema

    def parse(self, sql: list[str] | str) -> SqlMeta | None:
        """Parse a single or a list of SQL statements."""
        self.log.debug(
            "OpenLineage calling SQL parser with SQL %s dialect %s schema %s",
            sql,
            self.dialect,
            self.default_schema,
        )
        return parse(sql=sql, dialect=self.dialect, default_schema=self.default_schema)

    def parse_table_schemas(
        self,
        hook: BaseHook,
        inputs: list[DbTableMeta],
        outputs: list[DbTableMeta],
        database_info: DatabaseInfo,
        namespace: str = DEFAULT_NAMESPACE,
        database: str | None = None,
        sqlalchemy_engine: Engine | None = None,
    ) -> tuple[list[Dataset], ...]:
        """Parse schemas for input and output tables."""
        database_kwargs: GetTableSchemasParams = {
            "normalize_name": database_info.normalize_name_method,
            "is_cross_db": database_info.is_information_schema_cross_db,
            "information_schema_columns": database_info.information_schema_columns,
            "information_schema_table": database_info.information_schema_table_name,
            "is_uppercase_names": database_info.is_uppercase_names,
            "database": database or database_info.database,
            "use_flat_cross_db_query": database_info.use_flat_cross_db_query,
        }
        return get_table_schemas(
            hook,
            namespace,
            self.default_schema,
            database or database_info.database,
            self.create_information_schema_query(
                tables=inputs, sqlalchemy_engine=sqlalchemy_engine, **database_kwargs
            )
            if inputs
            else None,
            self.create_information_schema_query(
                tables=outputs, sqlalchemy_engine=sqlalchemy_engine, **database_kwargs
            )
            if outputs
            else None,
        )

    def get_metadata_from_parser(
        self,
        inputs: list[DbTableMeta],
        outputs: list[DbTableMeta],
        database_info: DatabaseInfo,
        namespace: str = DEFAULT_NAMESPACE,
        database: str | None = None,
    ) -> tuple[list[Dataset], ...]:
        database = database if database else database_info.database
        return [
            from_table_meta(dataset, database, namespace, database_info.is_uppercase_names)
            for dataset in inputs
        ], [
            from_table_meta(dataset, database, namespace, database_info.is_uppercase_names)
            for dataset in outputs
        ]

    def attach_column_lineage(
        self, datasets: list[Dataset], database: str | None, parse_result: SqlMeta
    ) -> None:
        """
        Attaches column lineage facet to the list of datasets.

        Note that currently each dataset has the same column lineage information set.
        This would be a matter of change after OpenLineage SQL Parser improvements.
        """
        if not len(parse_result.column_lineage):
            return
        for dataset in datasets:
            dataset.facets = dataset.facets or {}
            dataset.facets["columnLineage"] = column_lineage_dataset.ColumnLineageDatasetFacet(
                fields={
                    column_lineage.descendant.name: column_lineage_dataset.Fields(
                        inputFields=[
                            column_lineage_dataset.InputField(
                                namespace=dataset.namespace,
                                name=".".join(
                                    filter(
                                        None,
                                        (
                                            column_meta.origin.database or database,
                                            column_meta.origin.schema or self.default_schema,
                                            column_meta.origin.name,
                                        ),
                                    )
                                )
                                if column_meta.origin
                                else "",
                                field=column_meta.name,
                            )
                            for column_meta in column_lineage.lineage
                        ],
                        transformationType="",
                        transformationDescription="",
                    )
                    for column_lineage in parse_result.column_lineage
                }
            )

    def generate_openlineage_metadata_from_sql(
        self,
        sql: list[str] | str,
        hook: BaseHook,
        database_info: DatabaseInfo,
        database: str | None = None,
        sqlalchemy_engine: Engine | None = None,
        use_connection: bool = True,
    ) -> OperatorLineage:
        """
        Parse SQL statement(s) and generate OpenLineage metadata.

        Generated OpenLineage metadata contains:

        * input tables with schemas parsed
        * output tables with schemas parsed
        * run facets
        * job facets.

        :param sql: a SQL statement or list of SQL statement to be parsed
        :param hook: Airflow Hook used to connect to the database
        :param database_info: database specific information
        :param database: when passed it takes precedence over parsed database name
        :param sqlalchemy_engine: when passed, engine's dialect is used to compile SQL queries
        """
        job_facets: dict[str, JobFacet] = {"sql": sql_job.SQLJobFacet(query=self.normalize_sql(sql))}
        parse_result = self.parse(sql=self.split_sql_string(sql))
        if not parse_result:
            return OperatorLineage(job_facets=job_facets)

        run_facets: dict[str, RunFacet] = {}
        if parse_result.errors:
            run_facets["extractionError"] = extraction_error_run.ExtractionErrorRunFacet(
                totalTasks=len(sql) if isinstance(sql, list) else 1,
                failedTasks=len(parse_result.errors),
                errors=[
                    extraction_error_run.Error(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index,
                    )
                    for error in parse_result.errors
                ],
            )

        namespace = self.create_namespace(database_info=database_info)
        if use_connection:
            inputs, outputs = self.parse_table_schemas(
                hook=hook,
                inputs=parse_result.in_tables,
                outputs=parse_result.out_tables,
                namespace=namespace,
                database=database,
                database_info=database_info,
                sqlalchemy_engine=sqlalchemy_engine,
            )
        else:
            inputs, outputs = self.get_metadata_from_parser(
                inputs=parse_result.in_tables,
                outputs=parse_result.out_tables,
                namespace=namespace,
                database=database,
                database_info=database_info,
            )

        self.attach_column_lineage(outputs, database or database_info.database, parse_result)

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            run_facets=run_facets,
            job_facets=job_facets,
        )

    @staticmethod
    def create_namespace(database_info: DatabaseInfo) -> str:
        return (
            f"{database_info.scheme}://{database_info.authority}"
            if database_info.authority
            else database_info.scheme
        )

    @classmethod
    def normalize_sql(cls, sql: list[str] | str) -> str:
        """Make sure to return a semicolon-separated SQL statement."""
        return ";\n".join(stmt.rstrip(" ;\r\n") for stmt in cls.split_sql_string(sql))

    @classmethod
    def split_sql_string(cls, sql: list[str] | str) -> list[str]:
        """
        Split SQL string into list of statements.

        Tries to use `DbApiHook.split_sql_string` if available.
        Otherwise, uses the same logic.
        """
        try:
            from airflow.providers.common.sql.hooks.sql import DbApiHook

            split_statement = DbApiHook.split_sql_string
        except (ImportError, AttributeError):
            # No common.sql Airflow provider available or version is too old.
            def split_statement(sql: str, strip_semicolon: bool = False) -> list[str]:
                splits = sqlparse.split(
                    sql=sqlparse.format(sql, strip_comments=True),
                    strip_semicolon=strip_semicolon,
                )
                return [s for s in splits if s]

        if isinstance(sql, str):
            return split_statement(sql)
        return [obj for stmt in sql for obj in cls.split_sql_string(stmt) if obj != ""]

    def create_information_schema_query(
        self,
        tables: list[DbTableMeta],
        normalize_name: Callable[[str], str],
        is_cross_db: bool,
        information_schema_columns: list[str],
        information_schema_table: str,
        is_uppercase_names: bool,
        use_flat_cross_db_query: bool,
        database: str | None = None,
        sqlalchemy_engine: Engine | None = None,
    ) -> str:
        """Create SELECT statement to query information schema table."""
        tables_hierarchy = self._get_tables_hierarchy(
            tables,
            normalize_name=normalize_name,
            database=database,
            is_cross_db=is_cross_db,
        )
        return create_information_schema_query(
            columns=information_schema_columns,
            information_schema_table_name=information_schema_table,
            tables_hierarchy=tables_hierarchy,
            use_flat_cross_db_query=use_flat_cross_db_query,
            uppercase_names=is_uppercase_names,
            sqlalchemy_engine=sqlalchemy_engine,
        )

    @staticmethod
    def _get_tables_hierarchy(
        tables: list[DbTableMeta],
        normalize_name: Callable[[str], str],
        database: str | None = None,
        is_cross_db: bool = False,
    ) -> TablesHierarchy:
        """
        Create a hierarchy of database -> schema -> table name.

        This helps to create simpler information schema query grouped by
        database and schema.
        :param tables: List of tables.
        :param normalize_name: A method to normalize all names.
        :param is_cross_db: If false, set top (database) level to None
            when creating hierarchy.
        """
        hierarchy: TablesHierarchy = {}
        for table in tables:
            if is_cross_db:
                db = table.database or database
            else:
                db = None
            schemas = hierarchy.setdefault(normalize_name(db) if db else db, {})
            tables = schemas.setdefault(normalize_name(table.schema) if table.schema else None, [])
            tables.append(table.name)
        return hierarchy


def get_openlineage_facets_with_sql(
    hook: DbApiHook, sql: str | list[str], conn_id: str, database: str | None
) -> OperatorLineage | None:
    connection = hook.get_connection(conn_id)
    try:
        database_info = hook.get_openlineage_database_info(connection)
    except AttributeError:
        database_info = None

    if database_info is None:
        log.debug("%s has no database info provided", hook)
        return None

    try:
        sql_parser = SQLParser(
            dialect=hook.get_openlineage_database_dialect(connection),
            default_schema=hook.get_openlineage_default_schema(),
        )
    except AttributeError:
        log.debug("%s failed to get database dialect", hook)
        return None

    try:
        sqlalchemy_engine = hook.get_sqlalchemy_engine()
    except Exception as e:
        log.debug("Failed to get sql alchemy engine: %s", e)
        sqlalchemy_engine = None

    operator_lineage = sql_parser.generate_openlineage_metadata_from_sql(
        sql=sql,
        hook=hook,
        database_info=database_info,
        database=database,
        sqlalchemy_engine=sqlalchemy_engine,
        use_connection=should_use_external_connection(hook),
    )

    return operator_lineage
