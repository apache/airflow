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

import pytest

from airflow.providers.amazon.aws.hooks.redshift_data import QueryExecutionOutput, RedshiftDataHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.utils.openlineage import (
    get_facets_from_redshift_table,
    get_identity_column_lineage_facet,
)
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    Fields,
    InputField,
)


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.get_records")
def test_get_facets_from_redshift_table_sql_hook(mock_get_records):
    mock_get_records.return_value = [
        ("column1", "varchar", "Column 1 description", "Table description"),
        ("column2", "int", "Column 2 description", "Table description"),
    ]

    mock_hook = RedshiftSQLHook()

    result = get_facets_from_redshift_table(
        redshift_hook=mock_hook, table="my_table", redshift_data_api_kwargs={}
    )

    assert result["documentation"].description == "Table description"
    assert len(result["schema"].fields) == 2
    assert result["schema"].fields[0].name == "column1"
    assert result["schema"].fields[0].type == "varchar"
    assert result["schema"].fields[0].description == "Column 1 description"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.execute_query")
@mock.patch("airflow.providers.amazon.aws.hooks.redshift_data.RedshiftDataHook.conn")
def test_get_facets_from_redshift_table_data_hook(mock_connection, mock_execute_query):
    mock_execute_query.return_value = QueryExecutionOutput(statement_id="statement_id", session_id=None)
    mock_connection.get_statement_result.return_value = {
        "Records": [
            [
                {"stringValue": "column1"},
                {"stringValue": "varchar"},
                {"stringValue": "Column 1 description"},
                {"stringValue": "Table description"},
            ],
            [
                {"stringValue": "column2"},
                {"stringValue": "int"},
                {"stringValue": "Column 2 description"},
                {"stringValue": "Table description"},
            ],
        ]
    }

    mock_hook = RedshiftDataHook()

    result = get_facets_from_redshift_table(
        redshift_hook=mock_hook, table="my_table", redshift_data_api_kwargs={}
    )

    assert result["documentation"].description == "Table description"
    assert len(result["schema"].fields) == 2
    assert result["schema"].fields[0].name == "column1"
    assert result["schema"].fields[0].type == "varchar"
    assert result["schema"].fields[0].description == "Column 1 description"


@mock.patch("airflow.providers.amazon.aws.hooks.redshift_sql.RedshiftSQLHook.get_records")
def test_get_facets_no_records_sql_hook(mock_get_records):
    mock_get_records.return_value = []

    mock_hook = RedshiftSQLHook()

    result = get_facets_from_redshift_table(
        redshift_hook=mock_hook, table="my_table", redshift_data_api_kwargs={}
    )

    assert result["documentation"].description == ""
    assert len(result["schema"].fields) == 0


def test_get_identity_column_lineage_facet_multiple_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = [
        Dataset(namespace="s3://first_bucket", name="dir1"),
        Dataset(namespace="s3://second_bucket", name="dir2"),
    ]
    expected_facet = ColumnLineageDatasetFacet(
        fields={
            "field1": Fields(
                inputFields=[
                    InputField(
                        namespace="s3://first_bucket",
                        name="dir1",
                        field="field1",
                    ),
                    InputField(
                        namespace="s3://second_bucket",
                        name="dir2",
                        field="field1",
                    ),
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
            "field2": Fields(
                inputFields=[
                    InputField(
                        namespace="s3://first_bucket",
                        name="dir1",
                        field="field2",
                    ),
                    InputField(
                        namespace="s3://second_bucket",
                        name="dir2",
                        field="field2",
                    ),
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
        }
    )
    result = get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_field_names():
    field_names = []
    input_datasets = [
        Dataset(namespace="s3://first_bucket", name="dir1"),
        Dataset(namespace="s3://second_bucket", name="dir2"),
    ]
    expected_facet = ColumnLineageDatasetFacet(fields={})
    result = get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = []

    with pytest.raises(
        ValueError, match="When providing `field_names` You must provide at least one `input_dataset`."
    ):
        get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
