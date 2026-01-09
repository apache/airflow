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

import copy
import json
import logging
import os
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import Table

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    DocumentationDatasetFacet,
    ExternalQueryRunFacet,
    Fields,
    InputDataset,
    InputField,
    OutputDataset,
    OutputStatisticsOutputDatasetFacet,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.google.cloud.openlineage.facets import BigQueryJobRunFacet
from airflow.providers.google.cloud.openlineage.mixins import _BigQueryInsertJobOperatorOpenLineageMixin
from airflow.providers.openlineage.sqlparser import SQLParser

QUERY_JOB_PROPERTIES = {
    "configuration": {
        "query": {
            "query": """
            INSERT INTO dest_project.dest_dataset.dest_table
            SELECT a, b, c FROM source_project.source_dataset.source_table
            UNION ALL
            SELECT a, b, c FROM source_table2
            """,
            "defaultDataset": {"datasetId": "default_dataset", "projectId": "default_project"},
        }
    }
}
OUTPUT_DATASET = OutputDataset(
    namespace="bigquery",
    name="dest_project.dest_dataset.dest_table",
    facets={
        "schema": SchemaDatasetFacet(
            fields=[
                SchemaDatasetFacetFields("a", "STRING"),
                SchemaDatasetFacetFields("b", "STRING"),
                SchemaDatasetFacetFields("c", "STRING"),
                SchemaDatasetFacetFields("d", "STRING"),
                SchemaDatasetFacetFields("e", "STRING"),
                SchemaDatasetFacetFields("f", "STRING"),
                SchemaDatasetFacetFields("g", "STRING"),
            ]
        )
    },
)
INPUT_DATASETS = [
    InputDataset(
        namespace="bigquery",
        name="source_project.source_dataset.source_table",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields("a", "STRING"),
                    SchemaDatasetFacetFields("b", "STRING"),
                    SchemaDatasetFacetFields("c", "STRING"),
                    SchemaDatasetFacetFields("x", "STRING"),
                ]
            )
        },
    ),
    InputDataset(
        namespace="bigquery",
        name="default_project.default_dataset.source_table2",
        facets={
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields("a", "STRING"),
                    SchemaDatasetFacetFields("b", "STRING"),
                    SchemaDatasetFacetFields("c", "STRING"),
                    SchemaDatasetFacetFields("y", "STRING"),
                ]
            )
        },
    ),
    InputDataset("bigquery", "some.random.tb"),
]


def read_common_json_file(rel: str):
    with open(os.path.dirname(__file__) + "/../utils/" + rel) as f:
        return json.load(f)


class TestBigQueryOpenLineageMixin:
    def setup_method(self):
        self.copy_job_details = read_common_json_file("copy_job_details.json")
        self.extract_job_details = read_common_json_file("extract_job_details.json")
        self.load_job_details = read_common_json_file("load_job_details.json")
        self.query_job_details = read_common_json_file("query_job_details.json")
        self.script_job_details = read_common_json_file("script_job_details.json")
        hook = MagicMock()
        self.client = MagicMock()

        class BQOperator(_BigQueryInsertJobOperatorOpenLineageMixin):
            sql = ""
            job_id = "job_id"
            project_id = "project_id"
            location = None
            log = logging.getLogger("BQOperator")

            @property
            def hook(self):
                return hook

        hook.get_client.return_value = self.client

        self.operator = BQOperator()
        self.operator._client = self.client

    def test_get_openlineage_facets_on_complete_query_job(self):
        self.client.get_job.return_value._properties = self.query_job_details
        self.client.get_table.side_effect = [
            Table.from_api_repr(read_common_json_file("table_details.json")),
            Table.from_api_repr(read_common_json_file("out_table_details.json")),
        ]

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        self.query_job_details["configuration"]["query"].pop("query")
        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=111149056, properties=json.dumps(self.query_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            InputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.test_table",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("state", "STRING", "2-digit state code"),
                            SchemaDatasetFacetFields("gender", "STRING", "Sex (M=male or F=female)"),
                            SchemaDatasetFacetFields("year", "INTEGER", "4-digit year of birth"),
                            SchemaDatasetFacetFields("name", "STRING", "Given name of a person at birth"),
                            SchemaDatasetFacetFields(
                                "number", "INTEGER", "Number of occurrences of the name"
                            ),
                        ]
                    ),
                    "documentation": DocumentationDatasetFacet(
                        "The table contains the number of applicants for a Social Security card by year of birth and sex."
                    ),
                },
            )
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.output_table",
                outputFacets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=20, size=321, fileCount=None
                    )
                },
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    ),
                },
            ),
        ]

    def test_get_openlineage_facets_on_complete_copy_job(self):
        self.client.get_job.return_value._properties = self.copy_job_details
        self.client.get_table.return_value = Table.from_api_repr(read_common_json_file("table_details.json"))
        expected_facets = {
            "schema": SchemaDatasetFacet(
                fields=[
                    SchemaDatasetFacetFields("state", "STRING", "2-digit state code"),
                    SchemaDatasetFacetFields("gender", "STRING", "Sex (M=male or F=female)"),
                    SchemaDatasetFacetFields("year", "INTEGER", "4-digit year of birth"),
                    SchemaDatasetFacetFields("name", "STRING", "Given name of a person at birth"),
                    SchemaDatasetFacetFields("number", "INTEGER", "Number of occurrences of the name"),
                ]
            ),
            "documentation": DocumentationDatasetFacet(
                "The table contains the number of applicants for a Social Security card by year of birth and sex."
            ),
        }

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=None, properties=json.dumps(self.copy_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            InputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.copy_job_source",
                facets=expected_facets,
            ),
            InputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.copy_job_source2",
                facets=expected_facets,
            ),
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.copy_job_result",
                outputFacets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=20, size=3800, fileCount=None
                    )
                },
                facets={
                    **expected_facets,
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            col: Fields(
                                inputFields=[
                                    InputField(
                                        "bigquery", "airflow-openlineage.new_dataset.copy_job_source", col
                                    ),
                                    InputField(
                                        "bigquery", "airflow-openlineage.new_dataset.copy_job_source2", col
                                    ),
                                ],
                                transformationType="IDENTITY",
                                transformationDescription="identical",
                            )
                            for col in ["state", "gender", "year", "name", "number"]
                        }
                    ),
                },
            ),
        ]

    def test_get_openlineage_facets_on_complete_load_job(self):
        self.client.get_job.return_value._properties = self.load_job_details
        self.client.get_table.return_value = Table.from_api_repr(
            read_common_json_file("out_table_details.json")
        )

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=None, properties=json.dumps(self.load_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            InputDataset(
                namespace="gs://airflow-openlineage",
                name="/",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    )
                },
            ),
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.job_load",
                outputFacets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=10, size=546, fileCount=None
                    )
                },
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    ),
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            "name": Fields(
                                inputFields=[InputField("gs://airflow-openlineage", "/", "name")],
                                transformationType="IDENTITY",
                                transformationDescription="identical",
                            ),
                            "total_people": Fields(
                                inputFields=[InputField("gs://airflow-openlineage", "/", "total_people")],
                                transformationType="IDENTITY",
                                transformationDescription="identical",
                            ),
                        }
                    ),
                },
            ),
        ]

    def test_get_openlineage_facets_on_complete_extract_job(self):
        self.client.get_job.return_value._properties = self.extract_job_details
        self.client.get_table.return_value = Table.from_api_repr(
            read_common_json_file("out_table_details.json")
        )

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=None, properties=json.dumps(self.extract_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            InputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.extract_job_source",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    )
                },
            ),
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="gs://airflow-openlineage",
                name="extract_job_source",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    ),
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            "name": Fields(
                                inputFields=[
                                    InputField(
                                        "bigquery",
                                        "airflow-openlineage.new_dataset.extract_job_source",
                                        "name",
                                    )
                                ],
                                transformationType="IDENTITY",
                                transformationDescription="identical",
                            ),
                            "total_people": Fields(
                                inputFields=[
                                    InputField(
                                        "bigquery",
                                        "airflow-openlineage.new_dataset.extract_job_source",
                                        "total_people",
                                    )
                                ],
                                transformationType="IDENTITY",
                                transformationDescription="identical",
                            ),
                        }
                    ),
                },
            ),
        ]

    def test_get_openlineage_facets_on_complete_script_job(self):
        self.client.get_job.side_effect = [
            MagicMock(_properties=self.script_job_details),
            MagicMock(_properties=self.query_job_details),
        ]
        self.client.get_table.side_effect = [
            Table.from_api_repr(read_common_json_file("table_details.json")),
            Table.from_api_repr(read_common_json_file("out_table_details.json")),
        ]
        self.client.list_jobs.return_value = ["child_job_id"]

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        self.script_job_details["configuration"]["query"].pop("query")
        assert lineage.run_facets == {
            "bigQueryJob": BigQueryJobRunFacet(
                cached=False, billedBytes=120586240, properties=json.dumps(self.script_job_details)
            ),
            "externalQuery": ExternalQueryRunFacet(externalQueryId="job_id", source="bigquery"),
        }
        assert lineage.inputs == [
            InputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.test_table",
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("state", "STRING", "2-digit state code"),
                            SchemaDatasetFacetFields("gender", "STRING", "Sex (M=male or F=female)"),
                            SchemaDatasetFacetFields("year", "INTEGER", "4-digit year of birth"),
                            SchemaDatasetFacetFields("name", "STRING", "Given name of a person at birth"),
                            SchemaDatasetFacetFields(
                                "number", "INTEGER", "Number of occurrences of the name"
                            ),
                        ]
                    ),
                    "documentation": DocumentationDatasetFacet(
                        "The table contains the number of applicants for a Social Security card by year of birth and sex."
                    ),
                },
            )
        ]
        assert lineage.outputs == [
            OutputDataset(
                namespace="bigquery",
                name="airflow-openlineage.new_dataset.output_table",
                outputFacets={
                    "outputStatistics": OutputStatisticsOutputDatasetFacet(
                        rowCount=20, size=321, fileCount=None
                    )
                },
                facets={
                    "schema": SchemaDatasetFacet(
                        fields=[
                            SchemaDatasetFacetFields("name", "STRING"),
                            SchemaDatasetFacetFields("total_people", "INTEGER"),
                        ]
                    ),
                },
            ),
        ]

    def test_deduplicate_outputs(self):
        outputs = [
            None,
            OutputDataset(
                name="d1",
                namespace="",
                outputFacets={"outputStatistics": OutputStatisticsOutputDatasetFacet(3, 4)},
            ),
            OutputDataset(
                name="d1",
                namespace="",
                outputFacets={"outputStatistics": OutputStatisticsOutputDatasetFacet(3, 4)},
                facets={"t1": "t1"},
            ),
            OutputDataset(
                name="d2",
                namespace="",
                outputFacets={"outputStatistics": OutputStatisticsOutputDatasetFacet(6, 7)},
                facets={"t2": "t2"},
            ),
            OutputDataset(
                name="d2",
                namespace="",
                outputFacets={"outputStatistics": OutputStatisticsOutputDatasetFacet(60, 70)},
                facets={"t20": "t20"},
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

    def test_deduplicate_outputs_with_cll(self):
        outputs = [
            None,
            OutputDataset(
                name="a.b.c",
                namespace="bigquery",
                facets={
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            "c": Fields(
                                inputFields=[InputField("bigquery", "a.b.1", "c")],
                                transformationType="",
                                transformationDescription="",
                            ),
                            "d": Fields(
                                inputFields=[InputField("bigquery", "a.b.2", "d")],
                                transformationType="",
                                transformationDescription="",
                            ),
                        }
                    )
                },
            ),
            OutputDataset(
                name="a.b.c",
                namespace="bigquery",
                facets={
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            "c": Fields(
                                inputFields=[InputField("bigquery", "a.b.3", "x")],
                                transformationType="",
                                transformationDescription="",
                            ),
                            "e": Fields(
                                inputFields=[InputField("bigquery", "a.b.1", "e")],
                                transformationType="",
                                transformationDescription="",
                            ),
                        }
                    )
                },
            ),
            OutputDataset(
                name="x.y.z",
                namespace="bigquery",
                facets={
                    "columnLineage": ColumnLineageDatasetFacet(
                        fields={
                            "c": Fields(
                                inputFields=[InputField("bigquery", "a.b.3", "x")],
                                transformationType="",
                                transformationDescription="",
                            )
                        }
                    )
                },
            ),
        ]
        result = self.operator._deduplicate_outputs(outputs)
        assert len(result) == 2
        first_result = result[0]
        assert first_result.name == "a.b.c"
        assert first_result.facets["columnLineage"] == ColumnLineageDatasetFacet(
            fields={
                "c": Fields(
                    inputFields=[InputField("bigquery", "a.b.1", "c"), InputField("bigquery", "a.b.3", "x")],
                    transformationType="",
                    transformationDescription="",
                ),
                "d": Fields(
                    inputFields=[InputField("bigquery", "a.b.2", "d")],
                    transformationType="",
                    transformationDescription="",
                ),
                "e": Fields(
                    inputFields=[InputField("bigquery", "a.b.1", "e")],
                    transformationType="",
                    transformationDescription="",
                ),
            }
        )
        second_result = result[1]
        assert second_result.name == "x.y.z"
        assert second_result.facets["columnLineage"] == ColumnLineageDatasetFacet(
            fields={
                "c": Fields(
                    inputFields=[InputField("bigquery", "a.b.3", "x")],
                    transformationType="",
                    transformationDescription="",
                )
            }
        )

    @patch("airflow.providers.google.cloud.openlineage.mixins.get_facets_from_bq_table")
    def test_get_table_facets_safely(self, mock_get_facets):
        mock_get_facets.return_value = {"some": "facets"}
        result = self.operator._get_table_facets_safely("some_table")
        assert result == {"some": "facets"}

    @patch("airflow.providers.google.cloud.openlineage.mixins.get_facets_from_bq_table")
    def test_get_table_facets_safely_empty_when_error(self, mock_get_facets):
        mock_get_facets.side_effect = ValueError("Some error")
        result = self.operator._get_table_facets_safely("some_table")
        assert result == {}

    @patch("airflow.providers.google.cloud.openlineage.mixins.get_facets_from_bq_table")
    def test_get_dataset(self, mock_get_facets):
        mock_get_facets.return_value = {"some": "facets"}
        table_ref = {"projectId": "p", "datasetId": "d", "tableId": "t"}
        input_result = self.operator._get_dataset(table_ref, "input")
        assert input_result == InputDataset("bigquery", "p.d.t", facets={"some": "facets"})

        output_result = self.operator._get_dataset(table_ref, "output")
        assert output_result == OutputDataset("bigquery", "p.d.t", facets={"some": "facets"})

        with pytest.raises(ValueError, match="Invalid dataset_type. Must be 'input' or 'output'"):
            self.operator._get_dataset(table_ref, "wrong")

    @patch("airflow.providers.google.cloud.openlineage.mixins.get_facets_from_bq_table")
    def test_get_input_dataset(self, mock_get_facets):
        mock_get_facets.return_value = {"some": "facets"}
        table_ref = {"projectId": "p", "datasetId": "d", "tableId": "t"}
        expected_result = self.operator._get_dataset(table_ref, "input")
        result = self.operator._get_input_dataset(table_ref)
        assert result == expected_result

    @patch("airflow.providers.google.cloud.openlineage.mixins.get_facets_from_bq_table")
    def test_get_output_dataset(self, mock_get_facets):
        mock_get_facets.return_value = {"some": "facets"}
        table_ref = {"projectId": "p", "datasetId": "d", "tableId": "t"}
        expected_result = self.operator._get_dataset(table_ref, "output")
        result = self.operator._get_output_dataset(table_ref)
        assert result == expected_result

    @pytest.mark.parametrize("job_type", ("LOAD", "COPY", "EXTRACT"))
    def test_get_bigquery_job_run_facet_non_query_jobs(self, job_type):
        properties = {
            "statistics": {"some": "stats"},
            "configuration": {"jobType": job_type},
        }
        result = self.operator._get_bigquery_job_run_facet(properties)
        assert result.cached is False
        assert result.billedBytes is None
        assert result.properties == json.dumps(properties)

    @pytest.mark.parametrize("cache", (None, "false", False, 0))
    def test_get_bigquery_job_run_facet_query_no_cache_and_with_bytes(self, cache):
        properties = {
            "statistics": {"query": {"cacheHit": cache, "totalBytesBilled": 10}},
            "configuration": {"query": {"query": "SELECT ..."}, "jobType": "QUERY"},
        }
        result = self.operator._get_bigquery_job_run_facet(properties)
        assert result.cached is False
        assert result.billedBytes == 10
        properties["configuration"]["query"].pop("query")
        assert result.properties == json.dumps(properties)

    @pytest.mark.parametrize("cache", ("true", True))
    def test_get_bigquery_job_run_facet_query_with_cache_and_no_bytes(self, cache):
        properties = {
            "statistics": {
                "query": {
                    "cacheHit": cache,
                }
            },
            "configuration": {"query": {"query": "SELECT ..."}, "jobType": "QUERY"},
        }
        result = self.operator._get_bigquery_job_run_facet(properties)
        assert result.cached is True
        assert result.billedBytes is None
        properties["configuration"]["query"].pop("query")
        assert result.properties == json.dumps(properties)

    def test_get_output_statistics_dataset_facet_query_no_query_plan(self):
        properties = {
            "statistics": {"query": {"totalBytesBilled": 10}},
            "configuration": {"query": {"query": "SELECT ..."}, "jobType": "QUERY"},
        }
        result = self.operator._get_output_statistics_dataset_facet(properties)
        assert result is None

    def test_get_output_statistics_dataset_facet_query_no_stats(self):
        properties = {
            "statistics": {"query": {"totalBytesBilled": 10, "queryPlan": [{"test": "test"}]}},
            "configuration": {"query": {"query": "SELECT ..."}, "jobType": "QUERY"},
        }
        result = self.operator._get_output_statistics_dataset_facet(properties)
        assert result is None

    def test_get_output_statistics_dataset_facet_query(self):
        properties = {
            "statistics": {
                "query": {
                    "totalBytesBilled": 10,
                    "queryPlan": [{"recordsWritten": 123, "shuffleOutputBytes": "321"}],
                }
            },
            "configuration": {"query": {"query": "SELECT ..."}, "jobType": "QUERY"},
        }
        result = self.operator._get_output_statistics_dataset_facet(properties)
        assert result.rowCount == 123
        assert result.size == 321

    def test_get_output_statistics_dataset_facet_copy(self):
        properties = {
            "statistics": {
                "copy": {
                    "copiedRows": 123,
                    "copiedLogicalBytes": 321,
                }
            },
            "configuration": {"jobType": "COPY"},
        }
        result = self.operator._get_output_statistics_dataset_facet(properties)
        assert result.rowCount == 123
        assert result.size == 321

    def test_get_output_statistics_dataset_facet_load(self):
        properties = {
            "statistics": {
                "load": {
                    "outputRows": 123,
                    "outputBytes": 321,
                }
            },
            "configuration": {"jobType": "LOAD"},
        }
        result = self.operator._get_output_statistics_dataset_facet(properties)
        assert result.rowCount == 123
        assert result.size == 321

    def test_get_column_level_lineage_facet(self):
        result = self.operator._get_column_level_lineage_facet_for_query_job(
            QUERY_JOB_PROPERTIES, OUTPUT_DATASET, INPUT_DATASETS
        )
        assert result == ColumnLineageDatasetFacet(
            fields={
                col: Fields(
                    inputFields=[
                        InputField("bigquery", "default_project.default_dataset.source_table2", col),
                        InputField("bigquery", "source_project.source_dataset.source_table", col),
                    ],
                    transformationType="",
                    transformationDescription="",
                )
                for col in ("a", "b", "c")
            }
        )

    def test_get_column_level_lineage_facet_early_exit_empty_cll_from_parser(self):
        properties = {"configuration": {"query": {"query": "SELECT 1"}}}
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job(
                properties, OUTPUT_DATASET, INPUT_DATASETS
            )
            is None
        )
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job({}, OUTPUT_DATASET, INPUT_DATASETS)
            is None
        )

    def test_get_column_level_lineage_facet_early_exit_output_table_id_mismatch(self):
        output = copy.deepcopy(OUTPUT_DATASET)
        output.name = "different.name.table"
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job(
                QUERY_JOB_PROPERTIES, output, INPUT_DATASETS
            )
            is None
        )

    def test_get_column_level_lineage_facet_early_exit_output_columns_mismatch(self):
        output = copy.deepcopy(OUTPUT_DATASET)
        output.facets["schema"].fields = [
            SchemaDatasetFacetFields("different_col", "STRING"),
        ]
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job(
                QUERY_JOB_PROPERTIES, output, INPUT_DATASETS
            )
            is None
        )

    def test_get_column_level_lineage_facet_early_exit_wrong_parsed_input_tables(self):
        properties = {
            "configuration": {
                "query": {
                    "query": """
                    INSERT INTO dest_project.dest_dataset.dest_table
                    SELECT a, b, c FROM some.wrong.source_table
                    """,
                }
            }
        }
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job(
                properties, OUTPUT_DATASET, INPUT_DATASETS
            )
            is None
        )

    def test_get_column_level_lineage_facet_early_exit_wrong_parsed_input_columns(self):
        properties = {
            "configuration": {
                "query": {
                    "query": """
                    INSERT INTO dest_project.dest_dataset.dest_table
                    SELECT wrong_col, wrong2, wrong3 FROM source_project.source_dataset.source_table
                    """,
                }
            }
        }
        assert (
            self.operator._get_column_level_lineage_facet_for_query_job(
                properties, OUTPUT_DATASET, INPUT_DATASETS
            )
            is None
        )

    def test_get_qualified_name_from_parse_result(self):
        class _Table:  # Replacement for SQL parser TableMeta
            database = "project"
            schema = "dataset"
            name = "table"

        class _TableNoSchema:  # Replacement for SQL parser TableMeta
            database = None
            schema = "dataset"
            name = "table"

        class _TableNoSchemaNoDb:  # Replacement for SQL parser TableMeta
            database = None
            schema = None
            name = "table"

        result = self.operator._get_qualified_name_from_parse_result(
            table=_Table(),
            default_project="default_project",
            default_dataset="default_dataset",
        )
        assert result == "project.dataset.table"

        result = self.operator._get_qualified_name_from_parse_result(
            table=_TableNoSchema(),
            default_project="default_project",
            default_dataset="default_dataset",
        )
        assert result == "default_project.dataset.table"

        result = self.operator._get_qualified_name_from_parse_result(
            table=_TableNoSchemaNoDb(),
            default_project="default_project",
            default_dataset="default_dataset",
        )
        assert result == "default_project.default_dataset.table"

    def test_extract_default_dataset_and_project(self):
        properties = {"configuration": {"query": {"defaultDataset": {"datasetId": "default_dataset"}}}}
        result = self.operator._extract_default_dataset_and_project(properties, "default_project")
        assert result == ("default_dataset", "default_project")

        properties = {
            "configuration": {
                "query": {"defaultDataset": {"datasetId": "default_dataset", "projectId": "default_project"}}
            }
        }
        result = self.operator._extract_default_dataset_and_project(properties, "another_project")
        assert result == ("default_dataset", "default_project")

        result = self.operator._extract_default_dataset_and_project({}, "default_project")
        assert result == ("", "default_project")

    def test_validate_output_table_id_no_table(self):
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("SELECT 1"))
        assert parse_result.out_tables == []
        assert self.operator._validate_output_table_id(parse_result, None, None, None) is False

    def test_validate_output_table_id_multiple_tables(self):
        query = "INSERT INTO a.b.c VALUES (1); INSERT INTO d.e.f VALUES (2);"
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string(query))
        assert len(parse_result.out_tables) == 2
        assert self.operator._validate_output_table_id(parse_result, None, None, None) is False

    def test_validate_output_table_id_mismatch(self):
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("INSERT INTO a.b.c VALUES (1)"))
        assert len(parse_result.out_tables) == 1
        assert parse_result.out_tables[0].qualified_name == "a.b.c"
        assert (
            self.operator._validate_output_table_id(parse_result, OutputDataset("", "d.e.f"), None, None)
            is False
        )

    def test_validate_output_table_id(self):
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("INSERT INTO a.b.c VALUES (1)"))
        assert len(parse_result.out_tables) == 1
        assert parse_result.out_tables[0].qualified_name == "a.b.c"
        assert (
            self.operator._validate_output_table_id(parse_result, OutputDataset("", "a.b.c"), None, None)
            is True
        )

    def test_validate_output_table_id_query_with_table_name_only(self):
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("INSERT INTO c VALUES (1)"))
        assert len(parse_result.out_tables) == 1
        assert parse_result.out_tables[0].qualified_name == "c"
        assert (
            self.operator._validate_output_table_id(parse_result, OutputDataset("", "a.b.c"), "a", "b")
            is True
        )

    def test_extract_column_names_dataset_without_schema(self):
        assert self.operator._extract_column_names(Dataset("a", "b")) == []

    def test_extract_column_names_dataset_(self):
        ds = Dataset(
            "a",
            "b",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields("col1", "STRING"),
                        SchemaDatasetFacetFields("col2", "STRING"),
                    ]
                )
            },
        )
        assert self.operator._extract_column_names(ds) == ["col1", "col2"]

    def test_validate_output_columns_mismatch(self):
        ds = OutputDataset(
            "a",
            "b",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields("col1", "STRING"),
                        SchemaDatasetFacetFields("col2", "STRING"),
                    ]
                )
            },
        )
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("SELECT a , b FROM c"))
        assert self.operator._validate_output_columns(parse_result, ds) is False

    def test_validate_output_columns(self):
        ds = OutputDataset(
            "a",
            "b",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields("a", "STRING"),
                        SchemaDatasetFacetFields("b", "STRING"),
                    ]
                )
            },
        )
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("SELECT a , b FROM c"))
        assert self.operator._validate_output_columns(parse_result, ds) is True

    def test_extract_parsed_input_tables(self):
        query = "INSERT INTO x SELECT a, b from project1.ds1.tb1; INSERT INTO y SELECT c, d from tb2;"
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string(query))
        assert self.operator._extract_parsed_input_tables(parse_result, "default_project", "default_ds") == {
            "project1.ds1.tb1": ["a", "b"],
            "default_project.default_ds.tb2": ["c", "d"],
        }

    def test_extract_parsed_input_tables_no_cll(self):
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string("SELECT 1"))
        assert self.operator._extract_parsed_input_tables(parse_result, "p", "d") == {}

    def test_validate_input_tables_mismatch(self):
        result = self.operator._validate_input_tables({"a": None, "b": None}, {"a": None, "c": None})
        assert result is False

    def test_validate_input_tables_bq_has_more_tables(self):
        result = self.operator._validate_input_tables({"a": None}, {"a": None, "c": None})
        assert result is True

    def test_validate_input_tables_empty(self):
        result = self.operator._validate_input_tables({}, {"a": None, "c": None})
        assert result is False

    def test_validate_input_columns_mismatch(self):
        result = self.operator._validate_input_columns(
            {"a": ["1", "2"], "b": ["3", "4"]}, {"a": ["1", "2", "3"], "c": ["4", "5"]}
        )
        assert result is False

    def test_validate_input_columns_bq_has_more_cols(self):
        result = self.operator._validate_input_columns(
            {"a": ["1", "2"]}, {"a": ["1", "2", "3"], "c": ["4", "5"]}
        )
        assert result is True

    def test_validate_input_columns_empty(self):
        result = self.operator._validate_input_columns({}, {"a": ["1", "2", "3"], "c": ["4", "5"]})
        assert result is False

    def test_generate_column_lineage_facet(self):
        query = "INSERT INTO b.c SELECT c, d from tb2;"
        parse_result = SQLParser("bigquery").parse(SQLParser.split_sql_string(query))
        result = self.operator._generate_column_lineage_facet(parse_result, "default_project", "default_ds")
        assert result == ColumnLineageDatasetFacet(
            fields={
                "c": Fields(
                    inputFields=[InputField("bigquery", "default_project.default_ds.tb2", "c")],
                    transformationType="",
                    transformationDescription="",
                ),
                "d": Fields(
                    inputFields=[InputField("bigquery", "default_project.default_ds.tb2", "d")],
                    transformationType="",
                    transformationDescription="",
                ),
            }
        )

    def test_project_id_selection(self):
        """
        Check if project_id set via an argument to the operator takes prevalence over project_id set in a
        connection.
        """
        from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

        class TestOperator(GoogleCloudBaseOperator, _BigQueryInsertJobOperatorOpenLineageMixin):
            def __init__(self, project_id: str | None = None, **_):
                self.project_id = project_id
                self.job_id = "foobar"
                self.location = "foobar"
                self.sql = "foobar"

        # First test task where project_id is set explicitly
        test = TestOperator(project_id="project_a")
        test.hook = MagicMock()
        test.hook.project_id = "project_b"
        test._client = MagicMock()

        test.get_openlineage_facets_on_complete(None)
        _, kwargs = test.hook.get_client.call_args
        assert kwargs["project_id"] == "project_a"

        # Then test task where project_id is inherited from the hook
        test = TestOperator()
        test.hook = MagicMock()
        test.hook.project_id = "project_b"
        test._client = MagicMock()

        test.get_openlineage_facets_on_complete(None)
        _, kwargs = test.hook.get_client.call_args
        assert kwargs["project_id"] == "project_b"
