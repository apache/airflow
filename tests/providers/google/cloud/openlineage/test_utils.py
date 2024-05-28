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
from openlineage.client.facet import (
    ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields,
    DocumentationDatasetFacet,
    ExternalQueryRunFacet,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SchemaField,
)
from openlineage.client.run import Dataset

from airflow.providers.google.cloud.openlineage.utils import (
    BigQueryJobRunFacet,
    _BigQueryOpenLineageMixin,
    get_facets_from_bq_table,
    get_identity_column_lineage_facet,
)

TEST_DATASET = "test-dataset"
TEST_TABLE_ID = "test-table-id"
TEST_PROJECT_ID = "test-project-id"
TEST_TABLE_API_REPR = {
    "tableReference": {"projectId": TEST_PROJECT_ID, "datasetId": TEST_DATASET, "tableId": TEST_TABLE_ID},
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
    "tableReference": {"projectId": TEST_PROJECT_ID, "datasetId": TEST_DATASET, "tableId": TEST_TABLE_ID}
}
TEST_EMPTY_TABLE: Table = Table.from_api_repr(TEST_EMPTY_TABLE_API_REPR)


def read_file_json(file):
    with open(file=file) as f:
        return json.loads(f.read())


class TableMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inputs = [
            read_file_json("tests/providers/google/cloud/utils/table_details.json"),
            read_file_json("tests/providers/google/cloud/utils/out_table_details.json"),
        ]

    @property
    def _properties(self):
        return self.inputs.pop()


class TestBigQueryOpenLineageMixin:
    def setup_method(self):
        self.job_details = read_file_json("tests/providers/google/cloud/utils/job_details.json")
        self.script_job_details = read_file_json("tests/providers/google/cloud/utils/script_job_details.json")
        hook = MagicMock()
        self.client = MagicMock()

        class BQOperator(_BigQueryOpenLineageMixin):
            sql = ""
            job_id = "job_id"

            @property
            def hook(self):
                return hook

        hook.get_client.return_value = self.client

        self.client.get_table.return_value = TableMock()

        self.operator = BQOperator()

    def test_bq_job_information(self):
        self.client.get_job.return_value._properties = self.job_details

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        self.job_details["configuration"]["query"].pop("query")
        assert lineage.run_facets == {
            "bigQuery_job": BigQueryJobRunFacet(
                cached=False, billedBytes=111149056, properties=json.dumps(self.job_details)
            ),
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=111149056, properties=json.dumps(self.job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            Dataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.test_table",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaField("state", "STRING", "2-digit state code"),
                            SchemaField("gender", "STRING", "Sex (M=male or F=female)"),
                            SchemaField("year", "INTEGER", "4-digit year of birth"),
                            SchemaField("name", "STRING", "Given name of a person at birth"),
                            SchemaField("number", "INTEGER", "Number of occurrences of the name"),
                        ]
                    )
                },
            )
        ]
        assert lineage.outputs == [
            Dataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.output_table",
                facets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=20, size=321, fileCount=None
                    )
                },
            ),
        ]

    def test_bq_script_job_information(self):
        self.client.get_job.side_effect = [
            MagicMock(_properties=self.script_job_details),
            MagicMock(_properties=self.job_details),
        ]
        self.client.list_jobs.return_value = ["child_job_id"]

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        self.script_job_details["configuration"]["query"].pop("query")
        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=120586240, properties=json.dumps(self.script_job_details)
            ),
            "bigQuery_job": BigQueryJobRunFacet(
                cached=False, billedBytes=120586240, properties=json.dumps(self.script_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            Dataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.test_table",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaField("state", "STRING", "2-digit state code"),
                            SchemaField("gender", "STRING", "Sex (M=male or F=female)"),
                            SchemaField("year", "INTEGER", "4-digit year of birth"),
                            SchemaField("name", "STRING", "Given name of a person at birth"),
                            SchemaField("number", "INTEGER", "Number of occurrences of the name"),
                        ]
                    )
                },
            )
        ]
        assert lineage.outputs == [
            Dataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.output_table",
                facets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=20, size=321, fileCount=None
                    )
                },
            ),
        ]

    def test_deduplicate_outputs(self):
        outputs = [
            None,
            Dataset(
                name="d1", namespace="", facets={"outputStatistics": OutputStatisticsOutputDatasetFacet(3, 4)}
            ),
            Dataset(
                name="d1",
                namespace="",
                facets={"outputStatistics": OutputStatisticsOutputDatasetFacet(3, 4), "t1": "t1"},
            ),
            Dataset(
                name="d2",
                namespace="",
                facets={"outputStatistics": OutputStatisticsOutputDatasetFacet(6, 7), "t2": "t2"},
            ),
            Dataset(
                name="d2",
                namespace="",
                facets={"outputStatistics": OutputStatisticsOutputDatasetFacet(60, 70), "t20": "t20"},
            ),
        ]
        result = self.operator._deduplicate_outputs(outputs)
        assert len(result) == 2
        first_result = result[0]
        assert first_result.name == "d1"
        assert first_result.facets == {"t1": "t1"}
        second_result = result[1]
        assert second_result.name == "d2"
        assert second_result.facets == {"t20": "t20"}

    @pytest.mark.parametrize("cache", (None, "false", False, 0))
    def test_get_job_run_facet_no_cache_and_with_bytes(self, cache):
        properties = {
            "statistics": {"query": {"cacheHit": cache, "totalBytesBilled": 10}},
            "configuration": {"query": {"query": "SELECT ..."}},
        }
        result = self.operator._get_bigquery_job_run_facet(properties)
        assert result.cached is False
        assert result.billedBytes == 10
        properties["configuration"]["query"].pop("query")
        assert result.properties == json.dumps(properties)

    @pytest.mark.parametrize("cache", ("true", True))
    def test_get_job_run_facet_with_cache_and_no_bytes(self, cache):
        properties = {
            "statistics": {
                "query": {
                    "cacheHit": cache,
                }
            },
            "configuration": {"query": {"query": "SELECT ..."}},
        }
        result = self.operator._get_bigquery_job_run_facet(properties)
        assert result.cached is True
        assert result.billedBytes is None
        properties["configuration"]["query"].pop("query")
        assert result.properties == json.dumps(properties)

    def test_get_statistics_dataset_facet_no_query_plan(self):
        properties = {
            "statistics": {"query": {"totalBytesBilled": 10}},
            "configuration": {"query": {"query": "SELECT ..."}},
        }
        result = self.operator._get_statistics_dataset_facet(properties)
        assert result is None

    def test_get_statistics_dataset_facet_no_stats(self):
        properties = {
            "statistics": {"query": {"totalBytesBilled": 10, "queryPlan": [{"test": "test"}]}},
            "configuration": {"query": {"query": "SELECT ..."}},
        }
        result = self.operator._get_statistics_dataset_facet(properties)
        assert result is None

    def test_get_statistics_dataset_facet_with_stats(self):
        properties = {
            "statistics": {
                "query": {
                    "totalBytesBilled": 10,
                    "queryPlan": [{"recordsWritten": 123, "shuffleOutputBytes": "321"}],
                }
            },
            "configuration": {"query": {"query": "SELECT ..."}},
        }
        result = self.operator._get_statistics_dataset_facet(properties)
        assert result.rowCount == 123
        assert result.size == 321


def test_get_facets_from_bq_table():
    expected_facets = {
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaField(name="field1", type="STRING", description="field1 description"),
                SchemaField(name="field2", type="INTEGER"),
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
            "field1": ColumnLineageDatasetFacetFieldsAdditional(
                inputFields=[
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace="gs://first_bucket",
                        name="dir1",
                        field="field1",
                    ),
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace="gs://second_bucket",
                        name="dir2",
                        field="field1",
                    ),
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
            "field2": ColumnLineageDatasetFacetFieldsAdditional(
                inputFields=[
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
                        namespace="gs://first_bucket",
                        name="dir1",
                        field="field2",
                    ),
                    ColumnLineageDatasetFacetFieldsAdditionalInputFields(
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
    result = get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_field_names():
    field_names = []
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1"),
        Dataset(namespace="gs://second_bucket", name="dir2"),
    ]
    expected_facet = ColumnLineageDatasetFacet(fields={})
    result = get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
    assert result == expected_facet


def test_get_identity_column_lineage_facet_no_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = []

    with pytest.raises(ValueError):
        get_identity_column_lineage_facet(field_names=field_names, input_datasets=input_datasets)
