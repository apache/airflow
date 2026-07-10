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
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import Table
from openlineage.client.event_v2 import RunState

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


def make_task_instance():
    logical_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
    dag_run = MagicMock(
        logical_date=logical_date,
        clear_number=0,
        run_after=logical_date,
        conf={},
    )
    ti = MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=-1,
        logical_date=logical_date,
    )
    ti.dag_run = dag_run
    ti.get_template_context.return_value = {
        "dag_run": dag_run,
        "dag": MagicMock(),
        "task": MagicMock(),
        "task_instance": ti,
    }
    return ti


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

    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_get_openlineage_facets_on_complete_script_job(self, mock_emit_query_lineage):
        self.client.get_job.side_effect = [
            MagicMock(_properties=self.script_job_details),
            MagicMock(_properties=self.query_job_details),
        ]
        self.client.get_table.side_effect = [
            Table.from_api_repr(read_common_json_file("table_details.json")),
            Table.from_api_repr(read_common_json_file("out_table_details.json")),
        ]
        self.client.list_jobs.return_value = [MagicMock(job_id="child_job_id")]
        mock_ti = make_task_instance()

        lineage = self.operator.get_openlineage_facets_on_complete(mock_ti)

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
        mock_emit_query_lineage.assert_called_once()
        assert (
            mock_emit_query_lineage.call_args.kwargs["query_id"]
            == self.query_job_details["jobReference"]["jobId"]
        )
        assert mock_emit_query_lineage.call_args.kwargs["query_source_namespace"] == "bigquery"
        assert mock_emit_query_lineage.call_args.kwargs["query_text"] is None
        assert mock_emit_query_lineage.call_args.kwargs["is_successful"] is True
        assert mock_emit_query_lineage.call_args.kwargs["error_message"] is None
        assert mock_emit_query_lineage.call_args.kwargs["task_instance"] is mock_ti
        assert mock_emit_query_lineage.call_args.kwargs["job_name"] == "dag_id.task_id.query.1"
        assert mock_emit_query_lineage.call_args.kwargs["start_time"] == datetime.fromtimestamp(
            self.query_job_details["statistics"]["startTime"] / 1000, tz=timezone.utc
        )
        assert mock_emit_query_lineage.call_args.kwargs["end_time"] == datetime.fromtimestamp(
            self.query_job_details["statistics"]["endTime"] / 1000, tz=timezone.utc
        )

    @patch.object(
        _BigQueryInsertJobOperatorOpenLineageMixin,
        "_get_inputs_and_outputs",
        autospec=True,
    )
    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_script_job_aggregates_parent_datasets_and_emits_child_query_lineage(
        self, mock_emit_query_lineage, mock_get_inputs_and_outputs
    ):
        parent_job_details = copy.deepcopy(self.script_job_details)
        parent_job_details["statistics"]["numChildJobs"] = "2"
        child_job_1_details = {
            "jobReference": {"jobId": "child_job_1"},
            "configuration": {
                "jobType": "QUERY",
                "query": {"query": "CREATE TABLE output_table1 AS SELECT 1 AS id"},
            },
            "statistics": {"query": {"cacheHit": False, "totalBytesBilled": "10"}},
            "status": {"state": "DONE"},
        }
        child_job_2_details = {
            "jobReference": {"jobId": "child_job_2"},
            "configuration": {
                "jobType": "QUERY",
                "query": {"query": "CREATE TABLE output_table2 AS SELECT 2 AS id"},
            },
            "statistics": {"query": {"cacheHit": False, "totalBytesBilled": "20"}},
            "status": {"state": "DONE"},
        }
        input_table1 = InputDataset(namespace="bigquery", name="project.dataset.input_table1")
        output_table1 = OutputDataset(namespace="bigquery", name="project.dataset.output_table1")
        input_table2 = InputDataset(namespace="bigquery", name="project.dataset.input_table2")
        output_table2 = OutputDataset(namespace="bigquery", name="project.dataset.output_table2")
        self.client.get_job.side_effect = [
            MagicMock(_properties=parent_job_details),
            MagicMock(_properties=child_job_1_details),
            MagicMock(_properties=child_job_2_details),
        ]
        self.client.list_jobs.return_value = [
            MagicMock(job_id="child_job_1"),
            MagicMock(job_id="child_job_2"),
        ]
        mock_ti = make_task_instance()

        def get_inputs_and_outputs(_, properties):
            query = properties["configuration"]["query"]["query"]
            if "output_table1" in query:
                return [input_table1], [output_table1]
            return [input_table2], [output_table2]

        mock_get_inputs_and_outputs.side_effect = get_inputs_and_outputs

        lineage = self.operator.get_openlineage_facets_on_complete(mock_ti)

        assert lineage.inputs == [input_table1, input_table2]
        assert lineage.outputs == [output_table1, output_table2]
        assert "bigQueryJob" in lineage.run_facets
        assert "externalQuery" in lineage.run_facets
        assert mock_emit_query_lineage.call_count == 2

        first_call, second_call = mock_emit_query_lineage.call_args_list
        assert first_call.kwargs["query_id"] == "child_job_1"
        assert first_call.kwargs["query_source_namespace"] == "bigquery"
        assert first_call.kwargs["inputs"] == [input_table1]
        assert first_call.kwargs["outputs"] == [output_table1]
        assert first_call.kwargs["task_instance"] is mock_ti
        assert first_call.kwargs["job_name"] == "dag_id.task_id.query.1"

        assert second_call.kwargs["query_id"] == "child_job_2"
        assert second_call.kwargs["query_source_namespace"] == "bigquery"
        assert second_call.kwargs["inputs"] == [input_table2]
        assert second_call.kwargs["outputs"] == [output_table2]
        assert second_call.kwargs["task_instance"] is mock_ti
        assert second_call.kwargs["job_name"] == "dag_id.task_id.query.2"

    @patch.object(
        _BigQueryInsertJobOperatorOpenLineageMixin,
        "_get_inputs_and_outputs",
        autospec=True,
    )
    @patch("airflow.providers.openlineage.api.sql.resolve_task_emission_policy")
    @patch("airflow.providers.openlineage.api.sql.is_openlineage_active", return_value=True)
    @patch("airflow.providers.openlineage.api.sql.emit")
    def test_script_job_builds_child_query_events(
        self,
        mock_emit,
        mock_is_openlineage_active,
        mock_resolve_task_emission_policy,
        mock_get_inputs_and_outputs,
    ):
        parent_job_details = copy.deepcopy(self.script_job_details)
        parent_job_details["statistics"]["numChildJobs"] = "2"
        child_job_1_details = {
            "jobReference": {"jobId": "child_job_1"},
            "configuration": {
                "jobType": "QUERY",
                "query": {"query": "CREATE TABLE output_table1 AS SELECT 1 AS id"},
            },
            "statistics": {
                "startTime": "1600000000000",
                "endTime": "1600000005000",
                "query": {"cacheHit": False, "totalBytesBilled": "10"},
            },
            "status": {"state": "DONE"},
        }
        child_job_2_details = {
            "jobReference": {"jobId": "child_job_2"},
            "configuration": {
                "jobType": "QUERY",
                "query": {"query": "CREATE TABLE output_table2 AS SELECT 2 AS id"},
            },
            "statistics": {
                "startTime": "1600000010000",
                "endTime": "1600000015000",
                "query": {"cacheHit": False, "totalBytesBilled": "20"},
            },
            "status": {"state": "DONE"},
        }
        input_table1 = InputDataset(namespace="bigquery", name="project.dataset.input_table1")
        output_table1 = OutputDataset(namespace="bigquery", name="project.dataset.output_table1")
        input_table2 = InputDataset(namespace="bigquery", name="project.dataset.input_table2")
        output_table2 = OutputDataset(namespace="bigquery", name="project.dataset.output_table2")

        class ChildJob:
            def __init__(self, job_id):
                self.job_id = job_id

        job_details_by_id = {
            "job_id": parent_job_details,
            "child_job_1": child_job_1_details,
            "child_job_2": child_job_2_details,
        }
        self.client.get_job.side_effect = lambda job_id: MagicMock(_properties=job_details_by_id[job_id])
        self.client.list_jobs.return_value = [ChildJob("child_job_2"), ChildJob("child_job_1")]

        def get_inputs_and_outputs(_, properties):
            query = properties["configuration"]["query"]["query"]
            if "output_table1" in query:
                return [input_table1], [output_table1]
            return [input_table2], [output_table2]

        mock_get_inputs_and_outputs.side_effect = get_inputs_and_outputs
        mock_resolve_task_emission_policy.return_value = MagicMock(emit=True)

        lineage = self.operator.get_openlineage_facets_on_complete(make_task_instance())

        # Aggregation follows the sorted (execution-time) order, not the list_jobs order.
        assert lineage.inputs == [input_table1, input_table2]
        assert lineage.outputs == [output_table1, output_table2]
        assert mock_is_openlineage_active.call_count == 2
        assert mock_emit.call_count == 4
        child_1_start, child_1_complete, child_2_start, child_2_complete = [
            call.args[0] for call in mock_emit.call_args_list
        ]

        assert child_1_start.eventType == RunState.START
        assert child_1_complete.eventType == RunState.COMPLETE
        assert child_1_complete.job.name == "dag_id.task_id.query.1"
        assert child_1_complete.run.facets["externalQuery"].externalQueryId == "child_job_1"
        assert child_1_complete.run.facets["externalQuery"].source == "bigquery"
        assert child_1_complete.inputs == [input_table1]
        assert child_1_complete.outputs == [output_table1]
        assert child_1_start.eventTime == "2020-09-13T12:26:40+00:00"
        assert child_1_complete.eventTime == "2020-09-13T12:26:45+00:00"
        assert child_1_complete.run.facets["bigQueryJob"].billedBytes == 10
        assert child_1_complete.job.facets["sql"].query == "CREATE TABLE output_table1 AS SELECT 1 AS id"

        assert child_2_start.eventType == RunState.START
        assert child_2_complete.eventType == RunState.COMPLETE
        assert child_2_complete.job.name == "dag_id.task_id.query.2"
        assert child_2_complete.run.facets["externalQuery"].externalQueryId == "child_job_2"
        assert child_2_complete.inputs == [input_table2]
        assert child_2_complete.outputs == [output_table2]
        assert child_2_start.eventTime == "2020-09-13T12:26:50+00:00"
        assert child_2_complete.eventTime == "2020-09-13T12:26:55+00:00"

    @patch.object(
        _BigQueryInsertJobOperatorOpenLineageMixin,
        "_get_inputs_and_outputs",
        autospec=True,
    )
    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_script_job_continues_after_child_lineage_failure(
        self, mock_emit_query_lineage, mock_get_inputs_and_outputs
    ):
        parent_job_details = copy.deepcopy(self.script_job_details)
        parent_job_details["statistics"]["numChildJobs"] = "2"
        child_job_1_details = {
            "jobReference": {"jobId": "child_job_1"},
            "configuration": {"jobType": "QUERY"},
            "statistics": {"startTime": "1600000000000"},
            "status": {"state": "DONE"},
        }
        child_job_2_details = {
            "jobReference": {"jobId": "child_job_2"},
            "configuration": {"jobType": "QUERY", "query": {"query": "SELECT 2"}},
            "statistics": {"startTime": "1600000010000"},
            "status": {"state": "DONE"},
        }
        input_table2 = InputDataset(namespace="bigquery", name="project.dataset.input_table2")
        output_table2 = OutputDataset(namespace="bigquery", name="project.dataset.output_table2")
        self.client.get_job.side_effect = [
            MagicMock(_properties=parent_job_details),
            MagicMock(_properties=child_job_1_details),
            MagicMock(_properties=child_job_2_details),
        ]
        self.client.list_jobs.return_value = [
            MagicMock(job_id="child_job_1"),
            MagicMock(job_id="child_job_2"),
        ]
        mock_get_inputs_and_outputs.side_effect = [
            RuntimeError("broken child"),
            ([input_table2], [output_table2]),
        ]

        lineage = self.operator.get_openlineage_facets_on_complete(make_task_instance())

        assert lineage.inputs == [input_table2]
        assert lineage.outputs == [output_table2]
        assert "errorMessage" not in lineage.run_facets
        mock_emit_query_lineage.assert_called_once()
        assert mock_emit_query_lineage.call_args.kwargs["query_id"] == "child_job_2"
        assert mock_emit_query_lineage.call_args.kwargs["inputs"] == [input_table2]
        assert mock_emit_query_lineage.call_args.kwargs["outputs"] == [output_table2]
        # The failing child keeps its positional slot so the query.N suffix stays stable across runs.
        assert mock_emit_query_lineage.call_args.kwargs["job_name"] == "dag_id.task_id.query.2"

    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_script_job_without_task_instance_does_not_emit_child_query_events(self, mock_emit_query_lineage):
        self.client.get_job.side_effect = [
            MagicMock(_properties=self.script_job_details),
            MagicMock(_properties=self.query_job_details),
        ]
        self.client.get_table.side_effect = [
            Table.from_api_repr(read_common_json_file("table_details.json")),
            Table.from_api_repr(read_common_json_file("out_table_details.json")),
        ]
        self.client.list_jobs.return_value = [MagicMock(job_id="child_job_id")]

        lineage = self.operator.get_openlineage_facets_on_complete(None)

        # Parent still aggregates child datasets; only the per-child emission is skipped without a TI.
        assert [i.name for i in lineage.inputs] == ["airflow-openlineage.new_dataset.test_table"]
        assert [o.name for o in lineage.outputs] == ["airflow-openlineage.new_dataset.output_table"]
        mock_emit_query_lineage.assert_not_called()
        # Without the None-TI guard, building the child job_name dereferences None and the failure
        # surfaces as an errorMessage facet; its absence proves the guard short-circuited cleanly.
        assert "errorMessage" not in lineage.run_facets

    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_child_query_lineage_without_query_omits_sql_job_facet(self, mock_emit_query_lineage):
        self.operator._emit_child_query_lineage(
            task_instance=make_task_instance(),
            child_index=1,
            child_job_properties={
                "configuration": {"jobType": "QUERY", "query": {}},
                "statistics": {"query": {"cacheHit": False, "totalBytesBilled": "10"}},
            },
            inputs=[],
            outputs=[],
        )

        assert mock_emit_query_lineage.call_args.kwargs["additional_job_facets"] is None

    @patch("airflow.providers.openlineage.api.sql.emit_query_lineage")
    def test_child_query_lineage_marks_failed_child_job(self, mock_emit_query_lineage):
        self.operator._emit_child_query_lineage(
            task_instance=make_task_instance(),
            child_index=1,
            child_job_properties={
                "jobReference": {"jobId": "child_job_id"},
                "configuration": {"jobType": "QUERY", "query": {"query": "SELECT 1"}},
                "status": {"state": "DONE", "errorResult": {"reason": "invalid", "message": "Syntax error"}},
            },
            inputs=[],
            outputs=[],
        )

        assert mock_emit_query_lineage.call_args.kwargs["is_successful"] is False
        assert mock_emit_query_lineage.call_args.kwargs["error_message"] == "Syntax error"

    @patch.dict("sys.modules", {"airflow.providers.openlineage.api.sql": None})
    def test_child_query_lineage_skipped_with_old_openlineage_provider(self):
        self.client.get_job.side_effect = [
            MagicMock(_properties=self.script_job_details),
            MagicMock(_properties=self.query_job_details),
        ]
        self.client.get_table.side_effect = [
            Table.from_api_repr(read_common_json_file("table_details.json")),
            Table.from_api_repr(read_common_json_file("out_table_details.json")),
        ]
        self.client.list_jobs.return_value = [MagicMock(job_id="child_job_id")]

        lineage = self.operator.get_openlineage_facets_on_complete(make_task_instance())

        # Old OpenLineage providers lack emit_query_lineage: the parent event must still
        # aggregate child datasets and must not gain a misleading errorMessage facet.
        assert [i.name for i in lineage.inputs] == ["airflow-openlineage.new_dataset.test_table"]
        assert [o.name for o in lineage.outputs] == ["airflow-openlineage.new_dataset.output_table"]
        assert "errorMessage" not in lineage.run_facets

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("1600000000000", datetime(2020, 9, 13, 12, 26, 40, tzinfo=timezone.utc)),
            (None, None),
            ("not-a-timestamp", None),
        ],
    )
    def test_get_bigquery_job_datetime(self, value, expected):
        assert (
            self.operator._get_bigquery_job_datetime({"statistics": {"startTime": value}}, "startTime")
            == expected
        )

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
