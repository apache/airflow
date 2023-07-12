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

from unittest import mock
from unittest.mock import MagicMock

import pytest
from openlineage.client.facet import SchemaDatasetFacet, SchemaField, SqlJobFacet
from openlineage.client.run import Dataset
from openlineage.common.sql import DbTableMeta

from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.sqlparser import DatabaseInfo, SQLParser

DB_NAME = "FOOD_DELIVERY"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")

NAMESPACE = "test_namespace"

SCHEMA_FACET = SchemaDatasetFacet(
    fields=[
        SchemaField(name="ID", type="int4"),
        SchemaField(name="AMOUNT_OFF", type="int4"),
        SchemaField(name="CUSTOMER_EMAIL", type="varchar"),
        SchemaField(name="STARTS_ON", type="timestamp"),
        SchemaField(name="ENDS_ON", type="timestamp"),
    ]
)


def normalize_name_lower(name: str) -> str:
    return name.lower()


class TestSQLParser:
    def test_get_tables_hierarchy(self):
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Table1"), DbTableMeta("Table2")], normalize_name_lower
        ) == {None: {None: ["Table1", "Table2"]}}

        # base check with db, no cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")], normalize_name_lower
        ) == {None: {"schema1": ["Table1"], "schema2": ["Table2"]}}

        # same, with cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
            normalize_name_lower,
            is_cross_db=True,
        ) == {"db": {"schema1": ["Table1"], "schema2": ["Table2"]}}

        # explicit db, no cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
            normalize_name_lower,
            database="Db",
        ) == {None: {"schema1": ["Table1", "Table2"]}}

        # explicit db, with cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Schema1.Table1"), DbTableMeta("Schema1.Table2")],
            normalize_name_lower,
            database="Db",
            is_cross_db=True,
        ) == {"db": {"schema1": ["Table1", "Table2"]}}

        # mixed db, with cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Db2.Schema1.Table1"), DbTableMeta("Schema1.Table2")],
            normalize_name_lower,
            database="Db",
            is_cross_db=True,
        ) == {"db": {"schema1": ["Table2"]}, "db2": {"schema1": ["Table1"]}}

    def test_normalize_sql(self):
        assert SQLParser.normalize_sql("select * from asdf") == "select * from asdf"

        assert (
            SQLParser.normalize_sql(["select * from asdf", "insert into asdf values (1,2,3)"])
            == "select * from asdf;\ninsert into asdf values (1,2,3)"
        )

        assert (
            SQLParser.normalize_sql("select * from asdf;insert into asdf values (1,2,3)")
            == "select * from asdf;\ninsert into asdf values (1,2,3)"
        )

        assert (
            SQLParser.normalize_sql(
                """CREATE FUNCTION somefunc() RETURNS integer AS $$
                BEGIN
                    ...
                END;
                $$ LANGUAGE plpgsql```"""
            )
            == """CREATE FUNCTION somefunc() RETURNS integer AS $$
                BEGIN
                    ...
                END;
                $$ LANGUAGE plpgsql```"""
        )

    def test_normalize_sql_with_no_common_sql_provider(self):
        with mock.patch.dict("sys.modules", {"airflow.providers.common.sql.hooks.sql": None}):
            assert (
                SQLParser.normalize_sql("select * from asdf;insert into asdf values (1,2,3)")
                == "select * from asdf;\ninsert into asdf values (1,2,3)"
            )

    def test_parse_table_schemas(self):
        parser = SQLParser()
        db_info = DatabaseInfo(scheme="myscheme")

        hook = MagicMock()

        rows = lambda name: [
            (DB_SCHEMA_NAME, name, "ID", 1, "int4"),
            (DB_SCHEMA_NAME, name, "AMOUNT_OFF", 2, "int4"),
            (DB_SCHEMA_NAME, name, "CUSTOMER_EMAIL", 3, "varchar"),
            (DB_SCHEMA_NAME, name, "STARTS_ON", 4, "timestamp"),
            (DB_SCHEMA_NAME, name, "ENDS_ON", 5, "timestamp"),
        ]

        hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [
            rows("TABLE_IN"),
            rows("TABLE_OUT"),
        ]

        expected = (
            [Dataset(namespace=NAMESPACE, name="PUBLIC.TABLE_IN", facets={"schema": SCHEMA_FACET})],
            [Dataset(namespace=NAMESPACE, name="PUBLIC.TABLE_OUT", facets={"schema": SCHEMA_FACET})],
        )

        assert expected == parser.parse_table_schemas(
            hook=hook,
            namespace=NAMESPACE,
            inputs=[DbTableMeta("TABLE_IN")],
            outputs=[DbTableMeta("TABLE_OUT")],
            database_info=db_info,
        )

    @pytest.mark.parametrize("parser_returns_schema", [True, False])
    @mock.patch("airflow.providers.openlineage.sqlparser.SQLParser.parse")
    def test_generate_openlineage_metadata_from_sql(self, mock_parse, parser_returns_schema):
        parser = SQLParser(default_schema="ANOTHER_SCHEMA")
        db_info = DatabaseInfo(scheme="myscheme", authority="host:port")

        hook = MagicMock()

        rows = lambda schema, table: [
            (schema, table, "ID", 1, "int4"),
            (schema, table, "AMOUNT_OFF", 2, "int4"),
            (schema, table, "CUSTOMER_EMAIL", 3, "varchar"),
            (schema, table, "STARTS_ON", 4, "timestamp"),
            (schema, table, "ENDS_ON", 5, "timestamp"),
        ]

        sql = """CREATE TABLE table_out (
            ID int,
            AMOUNT_OFF int,
            CUSTOMER_EMAIL varchar,
            STARTS_ON timestamp,
            ENDS_ON timestamp
            --irrelevant comment
        )
        ;
        """

        hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [
            rows(DB_SCHEMA_NAME if parser_returns_schema else None, "TABLE_IN"),
            rows(DB_SCHEMA_NAME if parser_returns_schema else None, "TABLE_OUT"),
        ]

        mock_sql_meta = MagicMock()
        if parser_returns_schema:
            mock_sql_meta.in_tables = [DbTableMeta("PUBLIC.TABLE_IN")]
            mock_sql_meta.out_tables = [DbTableMeta("PUBLIC.TABLE_OUT")]
        else:
            mock_sql_meta.in_tables = [DbTableMeta("TABLE_IN")]
            mock_sql_meta.out_tables = [DbTableMeta("TABLE_OUT")]
        mock_sql_meta.errors = []

        mock_parse.return_value = mock_sql_meta

        formatted_sql = """CREATE TABLE table_out (
            ID int,
            AMOUNT_OFF int,
            CUSTOMER_EMAIL varchar,
            STARTS_ON timestamp,
            ENDS_ON timestamp

)"""
        expected_schema = "PUBLIC" if parser_returns_schema else "ANOTHER_SCHEMA"
        expected = OperatorLineage(
            inputs=[
                Dataset(
                    namespace="myscheme://host:port",
                    name=f"{expected_schema}.TABLE_IN",
                    facets={"schema": SCHEMA_FACET},
                )
            ],
            outputs=[
                Dataset(
                    namespace="myscheme://host:port",
                    name=f"{expected_schema}.TABLE_OUT",
                    facets={"schema": SCHEMA_FACET},
                )
            ],
            job_facets={"sql": SqlJobFacet(query=formatted_sql)},
        )

        assert expected == parser.generate_openlineage_metadata_from_sql(
            sql=sql,
            hook=hook,
            database_info=db_info,
        )
