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

from unittest.mock import MagicMock

import pytest
from openlineage.client import set_producer
from openlineage.client.event_v2 import Dataset
from openlineage.client.facet_v2 import schema_dataset
from openlineage.common.sql import DbTableMeta
from sqlalchemy import Column, MetaData, Table

from airflow.providers.openlineage import __version__ as OPENLINEAGE_PROVIDER_VERSION
from airflow.providers.openlineage.utils.sql import (
    create_filter_clauses,
    create_information_schema_query,
    get_table_schemas,
)

_PRODUCER = f"https://github.com/apache/airflow/tree/providers-openlineage/{OPENLINEAGE_PROVIDER_VERSION}"
set_producer(_PRODUCER)

DB_NAME = "FOOD_DELIVERY"
DB_SCHEMA_NAME = "PUBLIC"
DB_TABLE_NAME = DbTableMeta("DISCOUNTS")

SCHEMA_FACET = schema_dataset.SchemaDatasetFacet(
    fields=[
        schema_dataset.SchemaDatasetFacetFields(name="ID", type="int4"),
        schema_dataset.SchemaDatasetFacetFields(name="AMOUNT_OFF", type="int4"),
        schema_dataset.SchemaDatasetFacetFields(name="CUSTOMER_EMAIL", type="varchar"),
        schema_dataset.SchemaDatasetFacetFields(name="STARTS_ON", type="timestamp"),
        schema_dataset.SchemaDatasetFacetFields(name="ENDS_ON", type="timestamp"),
    ]
)


def test_get_table_schemas():
    hook = MagicMock()
    # (2) Mock calls to database
    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, rows]

    table_schemas = get_table_schemas(
        hook=hook,
        namespace="bigquery",
        database=DB_NAME,
        schema=DB_SCHEMA_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset(
                namespace="bigquery",
                name="FOOD_DELIVERY.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            )
        ],
        [
            Dataset(
                namespace="bigquery",
                name="FOOD_DELIVERY.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            )
        ],
    )


def test_get_table_schemas_with_mixed_databases():
    hook = MagicMock()
    ANOTHER_DB_NAME = "ANOTHER_DB"

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp", DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", ANOTHER_DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", ANOTHER_DB_NAME),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "CUSTOMER_EMAIL",
            3,
            "varchar",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "STARTS_ON",
            4,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "ENDS_ON",
            5,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    table_schemas = get_table_schemas(
        hook=hook,
        namespace="bigquery",
        database=DB_NAME,
        schema=DB_SCHEMA_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset(
                namespace="bigquery",
                name="FOOD_DELIVERY.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
            Dataset(
                namespace="bigquery",
                name="ANOTHER_DB.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
        ],
        [],
    )


def test_get_table_schemas_with_mixed_schemas():
    hook = MagicMock()
    ANOTHER_DB_SCHEMA_NAME = "ANOTHER_DB_SCHEMA"

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "CUSTOMER_EMAIL", 3, "varchar"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "STARTS_ON", 4, "timestamp"),
        (ANOTHER_DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ENDS_ON", 5, "timestamp"),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    table_schemas = get_table_schemas(
        hook=hook,
        namespace="bigquery",
        database=DB_NAME,
        schema=DB_SCHEMA_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset(
                namespace="bigquery",
                name="FOOD_DELIVERY.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
            Dataset(
                namespace="bigquery",
                name="FOOD_DELIVERY.ANOTHER_DB_SCHEMA.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
        ],
        [],
    )


def test_get_table_schemas_with_other_database():
    hook = MagicMock()
    ANOTHER_DB_NAME = "ANOTHER_DB"

    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "ID", 1, "int4", ANOTHER_DB_NAME),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, "AMOUNT_OFF", 2, "int4", ANOTHER_DB_NAME),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "CUSTOMER_EMAIL",
            3,
            "varchar",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "STARTS_ON",
            4,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
        (
            DB_SCHEMA_NAME,
            DB_TABLE_NAME.name,
            "ENDS_ON",
            5,
            "timestamp",
            ANOTHER_DB_NAME,
        ),
    ]

    hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, rows]

    table_schemas = get_table_schemas(
        hook=hook,
        namespace="bigquery",
        database=DB_NAME,
        schema=DB_SCHEMA_NAME,
        in_query="fake_sql",
        out_query="another_fake_sql",
    )

    assert table_schemas == (
        [
            Dataset(
                namespace="bigquery",
                name="ANOTHER_DB.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
        ],
        [
            Dataset(
                namespace="bigquery",
                name="ANOTHER_DB.PUBLIC.DISCOUNTS",
                facets={"schema": SCHEMA_FACET},
            ),
        ],
    )


@pytest.mark.parametrize(
    "schema_mapping, expected",
    [
        pytest.param(
            {None: {None: ["C1", "C2"]}},
            "information_schema.columns.table_name IN ('C1', 'C2')",
        ),
        pytest.param(
            {None: {"Schema1": ["Table1"], "Schema2": ["Table2"]}},
            "information_schema.columns.table_schema = 'Schema1' AND "
            "information_schema.columns.table_name IN ('Table1') OR "
            "information_schema.columns.table_schema = 'Schema2' AND "
            "information_schema.columns.table_name IN ('Table2')",
        ),
        pytest.param(
            {None: {"Schema1": ["Table1", "Table2"]}},
            "information_schema.columns.table_schema = 'Schema1' AND "
            "information_schema.columns.table_name IN ('Table1', 'Table2')",
        ),
        pytest.param(
            {"Database1": {"Schema1": ["Table1", "Table2"]}},
            "information_schema.columns.table_database = 'Database1' "
            "AND information_schema.columns.table_schema = 'Schema1' "
            "AND information_schema.columns.table_name IN ('Table1', 'Table2')",
        ),
        pytest.param(
            {
                "Database1": {
                    "Schema1": ["Table1", "Table2"],
                    "Schema2": ["Table3", "Table4"],
                }
            },
            "information_schema.columns.table_database = 'Database1' "
            "AND (information_schema.columns.table_schema = 'Schema1' "
            "AND information_schema.columns.table_name IN ('Table1', 'Table2') "
            "OR information_schema.columns.table_schema = 'Schema2' "
            "AND information_schema.columns.table_name IN ('Table3', 'Table4'))",
        ),
        pytest.param(
            {
                "Database1": {"Schema1": ["Table1", "Table2"]},
                "Database2": {"Schema2": ["Table3", "Table4"]},
            },
            "information_schema.columns.table_database = 'Database1' "
            "AND information_schema.columns.table_schema = 'Schema1' "
            "AND information_schema.columns.table_name IN ('Table1', 'Table2') OR "
            "information_schema.columns.table_database = 'Database2' "
            "AND information_schema.columns.table_schema = 'Schema2' "
            "AND information_schema.columns.table_name IN ('Table3', 'Table4')",
        ),
    ],
)
def test_create_filter_clauses(schema_mapping, expected):
    information_table = Table(
        "columns",
        MetaData(),
        *[
            Column(name)
            for name in [
                "table_schema",
                "table_name",
                "column_name",
                "ordinal_position",
                "udt_name",
                "table_database",
            ]
        ],
        schema="information_schema",
    )
    clauses = create_filter_clauses(schema_mapping, information_table)
    assert str(clauses.compile(compile_kwargs={"literal_binds": True})) == expected


def test_create_create_information_schema_query():
    columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "udt_name",
    ]
    assert (
        create_information_schema_query(
            columns=columns,
            information_schema_table_name="information_schema.columns",
            tables_hierarchy={None: {"schema1": ["table1"]}},
        )
        == "SELECT information_schema.columns.table_schema, "
        "information_schema.columns.table_name, information_schema.columns.column_name, "
        "information_schema.columns.ordinal_position, information_schema.columns.udt_name \n"
        "FROM information_schema.columns \n"
        "WHERE information_schema.columns.table_schema = 'schema1' "
        "AND information_schema.columns.table_name IN ('table1')"
    )


def test_create_create_information_schema_query_cross_db():
    columns = [
        "table_schema",
        "table_name",
        "column_name",
        "ordinal_position",
        "data_type",
    ]

    assert (
        create_information_schema_query(
            columns=columns,
            information_schema_table_name="information_schema.columns",
            tables_hierarchy={
                "db": {"schema1": ["table1"]},
                "db2": {"schema1": ["table2"]},
            },
        )
        == "SELECT db.information_schema.columns.table_schema, db.information_schema.columns.table_name, "
        "db.information_schema.columns.column_name, db.information_schema.columns.ordinal_position, "
        "db.information_schema.columns.data_type \n"
        "FROM db.information_schema.columns \n"
        "WHERE db.information_schema.columns.table_schema = 'schema1' "
        "AND db.information_schema.columns.table_name IN ('table1') "
        "UNION ALL "
        "SELECT db2.information_schema.columns.table_schema, db2.information_schema.columns.table_name, "
        "db2.information_schema.columns.column_name, db2.information_schema.columns.ordinal_position, "
        "db2.information_schema.columns.data_type \n"
        "FROM db2.information_schema.columns \n"
        "WHERE db2.information_schema.columns.table_schema = 'schema1' "
        "AND db2.information_schema.columns.table_name IN ('table2')"
    )
