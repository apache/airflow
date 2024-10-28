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

import json
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery.table import Table

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    DocumentationDatasetFacet,
    Fields,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.google.cloud.openlineage.utils import (
    get_facets_from_bq_table,
    get_identity_column_lineage_facet,
)

TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_PROJECT_ID = "test-project-id"
TEST_TABLE_API_REPR = {
    "tableReference": {
        "projectId": TEST_PROJECT_ID,
        "datasetId": TEST_DATASET,
        "tableId": TEST_TABLE_ID,
    },
    "description": "Table description.",
    "schema": {
        "fields": [
            {"name": "field1", "type": "STRING", "description": "field1 description"},
            {"name": "field2", "type": "INTEGER"},
        ]
    },
}
TEST_TABLE: Table = Table.from_api_repr(TEST_TABLE_API_REPR)
TEST_EMPTY_TABLE_API_REPR = {
    "tableReference": {
        "projectId": TEST_PROJECT_ID,
        "datasetId": TEST_DATASET,
        "tableId": TEST_TABLE_ID,
    }
}
TEST_EMPTY_TABLE: Table = Table.from_api_repr(TEST_EMPTY_TABLE_API_REPR)


def read_file_json(file):
    with open(file=file) as f:
        return json.loads(f.read())


class TableMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inputs = [
            read_file_json("providers/tests/google/cloud/utils/table_details.json"),
            read_file_json("providers/tests/google/cloud/utils/out_table_details.json"),
        ]

    @property
    def _properties(self):
        return self.inputs.pop()


def test_get_facets_from_bq_table():
    expected_facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields(
                    name="field1", type="STRING", description="field1 description"
                ),
                SchemaDatasetFacetFields(name="field2", type="INTEGER"),
            ]
        ),
        "documentation": DocumentationDatasetFacet(description="Table description."),
    }
    result = get_facets_from_bq_table(TEST_TABLE)
    assert result == expected_facets


def test_get_facets_from_empty_bq_table():
    expected_facets = {
        "schema": SchemaDatasetFacet(fields=[]),
        "documentation": DocumentationDatasetFacet(description=""),
    }
    result = get_facets_from_bq_table(TEST_EMPTY_TABLE)
    assert result == expected_facets


def test_get_identity_column_lineage_facet_multiple_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1"),
        Dataset(namespace="gs://second_bucket", name="dir2"),
    ]
    expected_facet = ColumnLineageDatasetFacet(
        fields={
            "field1": Fields(
                inputFields=[
                    InputField(
                        namespace="gs://first_bucket",
                        name="dir1",
                        field="field1",
                    ),
                    InputField(
                        namespace="gs://second_bucket",
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
                        namespace="gs://first_bucket",
                        name="dir1",
                        field="field2",
                    ),
                    InputField(
                        namespace="gs://second_bucket",
                        name="dir2",
                        field="field2",
                    ),
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
        }
    )
    result = get_identity_column_lineage_facet(
        field_names=field_names, input_datasets=input_datasets
    )
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_field_names():
    field_names = []
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1"),
        Dataset(namespace="gs://second_bucket", name="dir2"),
    ]
    expected_facet = ColumnLineageDatasetFacet(fields={})
    result = get_identity_column_lineage_facet(
        field_names=field_names, input_datasets=input_datasets
    )
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = []

    with pytest.raises(ValueError):
        get_identity_column_lineage_facet(
            field_names=field_names, input_datasets=input_datasets
        )
