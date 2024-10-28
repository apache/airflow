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
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import column_lineage_dataset, schema_dataset
from openlineage.common.sql import DbTableMeta

from airflow.providers.openlineage.sqlparser import (
    DatabaseInfo,
    GetTableSchemasParams,
    SQLParser,
)

DB_NAME = "FOOD_DELIVERY"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")

NAMESPACE = "test_namespace"


def test_get_table_schemas_params():
    def _inner(x: str) -> str:
        return x

    result = GetTableSchemasParams(
        normalize_name=_inner,
        is_cross_db=False,
        information_schema_columns=["col1", "col2"],
        information_schema_table="table",
        use_flat_cross_db_query=True,
        is_uppercase_names=False,
        database="db",
    )

    assert result["normalize_name"] == _inner
    assert result["is_cross_db"] is False
    assert result["information_schema_columns"] == ["col1", "col2"]
    assert result["information_schema_table"] == "table"
    assert result["use_flat_cross_db_query"] is True
    assert result["is_uppercase_names"] is False
    assert result["database"] == "db"


def test_database_info():
    def _inner(x: str) -> str:
        return x

    result = DatabaseInfo(
        scheme="scheme",
        authority="authority",
        database="database",
        information_schema_columns=["col1", "col2"],
        information_schema_table_name="table",
        use_flat_cross_db_query=True,
        is_information_schema_cross_db=True,
        is_uppercase_names=False,
        normalize_name_method=_inner,
    )

    assert result.scheme == "scheme"
    assert result.authority == "authority"
    assert result.database == "database"
    assert result.information_schema_columns == ["col1", "col2"]
    assert result.information_schema_table_name == "table"
    assert result.use_flat_cross_db_query is True
    assert result.is_information_schema_cross_db is True
    assert result.is_uppercase_names is False
    assert result.is_uppercase_names is False
    assert result.normalize_name_method == _inner


def normalize_name_lower(name: str) -> str:
    return name.lower()


class TestSQLParser:
    def test_get_tables_hierarchy(self):
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Table1"), DbTableMeta("Table2")], normalize_name_lower
        ) == {None: {None: ["Table1", "Table2"]}}

        # base check with db, no cross db
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Db.Schema1.Table1"), DbTableMeta("Db.Schema2.Table2")],
            normalize_name_lower,
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

        # cross db, no db & schema parsed
        assert SQLParser._get_tables_hierarchy(
            [DbTableMeta("Table1"), DbTableMeta("Table2")],
            normalize_name_lower,
            database="Db",
            is_cross_db=True,
        ) == {"db": {None: ["Table1", "Table2"]}}

    def test_normalize_sql(self):
        assert SQLParser.normalize_sql("select * from asdf") == "select * from asdf"

        assert (
            SQLParser.normalize_sql(
                ["select * from asdf", "insert into asdf values (1,2,3)"]
            )
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
        with mock.patch.dict(
            "sys.modules", {"airflow.providers.common.sql.hooks.sql": None}
        ):
            assert (
                SQLParser.normalize_sql(
                    "select * from asdf;insert into asdf values (1,2,3)"
                )
                == "select * from asdf;\ninsert into asdf values (1,2,3)"
            )

    def test_parse_table_schemas(self):
        parser = SQLParser()
        db_info = DatabaseInfo(scheme="myscheme")

        hook = MagicMock()

        def rows(name):
            return [
                (DB_SCHEMA_NAME, name, "ID", 1, "int4"),
                (DB_SCHEMA_NAME, name, "AMOUNT_OFF", 2, "int4"),
                (DB_SCHEMA_NAME, name, "CUSTOMER_EMAIL", 3, "varchar"),
                (DB_SCHEMA_NAME, name, "STARTS_ON", 4, "timestamp"),
                (DB_SCHEMA_NAME, name, "ENDS_ON", 5, "timestamp"),
            ]

        hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [
            rows("top_delivery_times"),
            rows("popular_orders_day_of_week"),
        ]

        expected_schema_facet = schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(name="ID", type="int4"),
                schema_dataset.SchemaDatasetFacetFields(name="AMOUNT_OFF", type="int4"),
                schema_dataset.SchemaDatasetFacetFields(
                    name="CUSTOMER_EMAIL", type="varchar"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="STARTS_ON", type="timestamp"
                ),
                schema_dataset.SchemaDatasetFacetFields(name="ENDS_ON", type="timestamp"),
            ]
        )

        expected = (
            [
                Dataset(
                    namespace=NAMESPACE,
                    name="PUBLIC.top_delivery_times",
                    facets={"schema": expected_schema_facet},
                )
            ],
            [
                Dataset(
                    namespace=NAMESPACE,
                    name="PUBLIC.popular_orders_day_of_week",
                    facets={"schema": expected_schema_facet},
                )
            ],
        )

        assert expected == parser.parse_table_schemas(
            hook=hook,
            namespace=NAMESPACE,
            inputs=[DbTableMeta("top_delivery_times")],
            outputs=[DbTableMeta("popular_orders_day_of_week")],
            database_info=db_info,
        )

    @pytest.mark.parametrize("parser_returns_schema", [True, False])
    @mock.patch("airflow.providers.openlineage.sqlparser.SQLParser.parse")
    def test_generate_openlineage_metadata_from_sql(
        self, mock_parse, parser_returns_schema
    ):
        parser = SQLParser(default_schema="ANOTHER_SCHEMA")
        db_info = DatabaseInfo(scheme="myscheme", authority="host:port")

        hook = MagicMock()

        returned_schema = DB_SCHEMA_NAME if parser_returns_schema else None
        returned_rows = [
            [
                (returned_schema, "top_delivery_times", "order_id", 1, "int4"),
                (
                    returned_schema,
                    "top_delivery_times",
                    "order_placed_on",
                    2,
                    "timestamp",
                ),
                (returned_schema, "top_delivery_times", "customer_email", 3, "varchar"),
            ],
            [
                (
                    returned_schema,
                    "popular_orders_day_of_week",
                    "order_day_of_week",
                    1,
                    "varchar",
                ),
                (
                    returned_schema,
                    "popular_orders_day_of_week",
                    "order_placed_on",
                    2,
                    "timestamp",
                ),
                (
                    returned_schema,
                    "popular_orders_day_of_week",
                    "orders_placed",
                    3,
                    "int4",
                ),
            ],
        ]

        sql = """INSERT INTO popular_orders_day_of_week (order_day_of_week)
        SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week
        FROM top_delivery_times --irrelevant comment
        );"""

        hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = (
            returned_rows
        )

        mock_sql_meta = MagicMock()
        if parser_returns_schema:
            mock_sql_meta.in_tables = [DbTableMeta("PUBLIC.top_delivery_times")]
            mock_sql_meta.out_tables = [DbTableMeta("PUBLIC.popular_orders_day_of_week")]
        else:
            mock_sql_meta.in_tables = [DbTableMeta("top_delivery_times")]
            mock_sql_meta.out_tables = [DbTableMeta("popular_orders_day_of_week")]
        mock_column_lineage = MagicMock()
        mock_column_lineage.descendant.name = "order_day_of_week"
        mock_lineage = MagicMock()
        mock_lineage.name = "order_placed_on"
        mock_lineage.origin.name = "top_delivery_times"
        mock_lineage.origin.database = None
        mock_lineage.origin.schema = "PUBLIC" if parser_returns_schema else None
        mock_column_lineage.lineage = [mock_lineage]

        mock_sql_meta.column_lineage = [mock_column_lineage]
        mock_sql_meta.errors = []

        mock_parse.return_value = mock_sql_meta

        formatted_sql = """INSERT INTO popular_orders_day_of_week (order_day_of_week)
        SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week
        FROM top_delivery_times
        )"""
        expected_schema = "PUBLIC" if parser_returns_schema else "ANOTHER_SCHEMA"
        metadata = parser.generate_openlineage_metadata_from_sql(
            sql=sql,
            hook=hook,
            database_info=db_info,
        )

        assert metadata.inputs == [
            Dataset(
                namespace="myscheme://host:port",
                name=f"{expected_schema}.top_delivery_times",
                facets={
                    "schema": schema_dataset.SchemaDatasetFacet(
                        fields=[
                            schema_dataset.SchemaDatasetFacetFields(
                                name="order_id", type="int4"
                            ),
                            schema_dataset.SchemaDatasetFacetFields(
                                name="order_placed_on", type="timestamp"
                            ),
                            schema_dataset.SchemaDatasetFacetFields(
                                name="customer_email", type="varchar"
                            ),
                        ]
                    )
                },
            )
        ]
        assert len(metadata.outputs) == 1
        assert metadata.outputs[0].namespace == "myscheme://host:port"
        assert metadata.outputs[0].name == f"{expected_schema}.popular_orders_day_of_week"
        assert metadata.outputs[0].facets["schema"] == schema_dataset.SchemaDatasetFacet(
            fields=[
                schema_dataset.SchemaDatasetFacetFields(
                    name="order_day_of_week", type="varchar"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="order_placed_on", type="timestamp"
                ),
                schema_dataset.SchemaDatasetFacetFields(
                    name="orders_placed", type="int4"
                ),
            ]
        )
        assert metadata.outputs[0].facets[
            "columnLineage"
        ] == column_lineage_dataset.ColumnLineageDatasetFacet(
            fields={
                "order_day_of_week": column_lineage_dataset.Fields(
                    inputFields=[
                        column_lineage_dataset.InputField(
                            namespace="myscheme://host:port",
                            name=f"{expected_schema}.top_delivery_times",
                            field="order_placed_on",
                        )
                    ],
                    transformationDescription="",
                    transformationType="",
                )
            }
        )
        assert metadata.job_facets["sql"].query.replace(" ", "") == formatted_sql.replace(
            " ", ""
        )
