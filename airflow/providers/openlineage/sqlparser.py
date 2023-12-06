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

from typing import TYPE_CHECKING, Callable

import sqlparse
from attrs import define
from openlineage.client.facet import (
    BaseFacet,
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    ExtractionError,
    ExtractionErrorRunFacet,
    SqlJobFacet,
)
from openlineage.common.sql import DbTableMeta, SqlMeta, parse

from airflow.providers.openlineage.extractors.base import OperatorLineage
from airflow.providers.openlineage.utils.sql import (
    TablesHierarchy,
    create_information_schema_query,
    get_table_schemas,
)
from airflow.typing_compat import TypedDict

if TYPE_CHECKING:
    from openlineage.client.run import Dataset
    from sqlalchemy.engine import Engine

    from airflow.hooks.base import BaseHook

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
    :param is_information_schema_cross_db: Specifies if information schema contains
        cross-database data.
    :param is_uppercase_names: Specifies if database accepts only uppercase names (e.g. Snowflake).
    :param normalize_name_method: Method to normalize database, schema and table names.
        Defaults to `name.lower()`.
    """

    scheme: str
    authority: str | None = None
    database: str | None = None
    information_schema_columns: list[str] = DEFAULT_INFORMATION_SCHEMA_COLUMNS
    information_schema_table_name: str = DEFAULT_INFORMATION_SCHEMA_TABLE_NAME
    is_information_schema_cross_db: bool = False
    is_uppercase_names: bool = False
    normalize_name_method: Callable[[str], str] = default_normalize_name_method


class SQLParser:
    """Interface for openlineage-sql.

    :param dialect: dialect specific to the database
    :param default_schema: schema applied to each table with no schema parsed
    """

    def __init__(self, dialect: str | None = None, default_schema: str | None = None) -> None:
        self.dialect = dialect
        self.default_schema = default_schema

    def parse(self, sql: list[str] | str) -> SqlMeta | None:
        """Parse a single or a list of SQL statements."""
        return parse(sql=sql, dialect=self.dialect)

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
            dataset.facets["columnLineage"] = ColumnLineageDatasetFacet(
                fields={
                    column_lineage.descendant.name: ColumnLineageDatasetFacetFieldsAdditional(
                        inputFields=[
                            ColumnLineageDatasetFacetFieldsAdditionalInputFields(
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
    ) -> OperatorLineage:
        """Parses SQL statement(s) and generates OpenLineage metadata.

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
        job_facets: dict[str, BaseFacet] = {"sql": SqlJobFacet(query=self.normalize_sql(sql))}
        parse_result = self.parse(self.split_sql_string(sql))
        if not parse_result:
            return OperatorLineage(job_facets=job_facets)

        run_facets: dict[str, BaseFacet] = {}
        if parse_result.errors:
            run_facets["extractionError"] = ExtractionErrorRunFacet(
                totalTasks=len(sql) if isinstance(sql, list) else 1,
                failedTasks=len(parse_result.errors),
                errors=[
                    ExtractionError(
                        errorMessage=error.message,
                        stackTrace=None,
                        task=error.origin_statement,
                        taskNumber=error.index,
                    )
                    for error in parse_result.errors
                ],
            )

        namespace = self.create_namespace(database_info=database_info)
        inputs, outputs = self.parse_table_schemas(
            hook=hook,
            inputs=parse_result.in_tables,
            outputs=parse_result.out_tables,
            namespace=namespace,
            database=database,
            database_info=database_info,
            sqlalchemy_engine=sqlalchemy_engine,
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
        """Makes sure to return a semicolon-separated SQL statements."""
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
            def split_statement(sql: str) -> list[str]:
                splits = sqlparse.split(sqlparse.format(sql, strip_comments=True))
                return [s for s in splits if s]

        if isinstance(sql, str):
            return split_statement(sql)
        return [obj for stmt in sql for obj in cls.split_sql_string(stmt) if obj != ""]

    @classmethod
    def create_information_schema_query(
        cls,
        tables: list[DbTableMeta],
        normalize_name: Callable[[str], str],
        is_cross_db: bool,
        information_schema_columns,
        information_schema_table,
        is_uppercase_names,
        database: str | None = None,
        sqlalchemy_engine: Engine | None = None,
    ) -> str:
        """Creates SELECT statement to query information schema table."""
        tables_hierarchy = cls._get_tables_hierarchy(
            tables,
            normalize_name=normalize_name,
            database=database,
            is_cross_db=is_cross_db,
        )
        return create_information_schema_query(
            columns=information_schema_columns,
            information_schema_table_name=information_schema_table,
            tables_hierarchy=tables_hierarchy,
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
        Creates a hierarchy of database -> schema -> table name.

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
