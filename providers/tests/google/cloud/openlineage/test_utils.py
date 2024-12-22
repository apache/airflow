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

import datetime as dt
import json
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import Table

from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    DocumentationDatasetFacet,
    Fields,
    Identifier,
    InputField,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
    SymlinksDatasetFacet,
)
from airflow.providers.google.cloud.openlineage.utils import (
    _extract_supported_job_type_from_dataproc_job,
    _is_openlineage_provider_accessible,
    _replace_dataproc_job_properties,
    extract_ds_name_from_gcs_path,
    get_facets_from_bq_table,
    get_identity_column_lineage_facet,
    inject_openlineage_properties_into_dataproc_job,
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
    "externalDataConfiguration": {
        "sourceFormat": "CSV",
        "sourceUris": ["gs://bucket/path/to/files*", "gs://second_bucket/path/to/other/files*"],
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
                SchemaDatasetFacetFields(name="field1", type="STRING", description="field1 description"),
                SchemaDatasetFacetFields(name="field2", type="INTEGER"),
            ]
        ),
        "documentation": DocumentationDatasetFacet(description="Table description."),
        "symlink": SymlinksDatasetFacet(
            identifiers=[
                Identifier(namespace="gs://bucket", name="path/to", type="file"),
                Identifier(namespace="gs://second_bucket", name="path/to/other", type="file"),
            ]
        ),
    }
    result = get_facets_from_bq_table(TEST_TABLE)
    assert result == expected_facets


def test_get_facets_from_empty_bq_table():
    result = get_facets_from_bq_table(TEST_EMPTY_TABLE)
    assert result == {}


def test_get_identity_column_lineage_facet_source_datasets_schemas_are_subsets():
    field_names = ["field1", "field2", "field3"]
    input_datasets = [
        Dataset(
            namespace="gs://first_bucket",
            name="dir1",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                    ]
                )
            },
        ),
        Dataset(
            namespace="gs://second_bucket",
            name="dir2",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field2", type="STRING"),
                    ]
                )
            },
        ),
    ]
    expected_facet = ColumnLineageDatasetFacet(
        fields={
            "field1": Fields(
                inputFields=[
                    InputField(
                        namespace="gs://first_bucket",
                        name="dir1",
                        field="field1",
                    )
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
            "field2": Fields(
                inputFields=[
                    InputField(
                        namespace="gs://second_bucket",
                        name="dir2",
                        field="field2",
                    ),
                ],
                transformationType="IDENTITY",
                transformationDescription="identical",
            ),
            # field3 is missing here as it's not present in any source dataset
        }
    )
    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {"columnLineage": expected_facet}


def test_get_identity_column_lineage_facet_multiple_input_datasets():
    field_names = ["field1", "field2"]
    schema_facet = SchemaDatasetFacet(
        fields=[
            SchemaDatasetFacetFields(name="field1", type="STRING"),
            SchemaDatasetFacetFields(name="field2", type="STRING"),
        ]
    )
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1", facets={"schema": schema_facet}),
        Dataset(namespace="gs://second_bucket", name="dir2", facets={"schema": schema_facet}),
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
    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {"columnLineage": expected_facet}


def test_get_identity_column_lineage_facet_dest_cols_not_in_input_datasets():
    field_names = ["x", "y"]
    input_datasets = [
        Dataset(
            namespace="gs://first_bucket",
            name="dir1",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field1", type="STRING"),
                    ]
                )
            },
        ),
        Dataset(
            namespace="gs://second_bucket",
            name="dir2",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="field2", type="STRING"),
                    ]
                )
            },
        ),
    ]

    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {}


def test_get_identity_column_lineage_facet_no_schema_in_input_dataset():
    field_names = ["field1", "field2"]
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1"),
    ]
    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {}


def test_get_identity_column_lineage_facet_no_field_names():
    field_names = []
    schema_facet = SchemaDatasetFacet(
        fields=[
            SchemaDatasetFacetFields(name="field1", type="STRING"),
            SchemaDatasetFacetFields(name="field2", type="STRING"),
        ]
    )
    input_datasets = [
        Dataset(namespace="gs://first_bucket", name="dir1", facets={"schema": schema_facet}),
        Dataset(namespace="gs://second_bucket", name="dir2", facets={"schema": schema_facet}),
    ]
    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {}


def test_get_identity_column_lineage_facet_no_input_datasets():
    field_names = ["field1", "field2"]
    input_datasets = []

    result = get_identity_column_lineage_facet(dest_field_names=field_names, input_datasets=input_datasets)
    assert result == {}


@pytest.mark.parametrize(
    "input_path, expected_output",
    [
        ("/path/to/file.txt", "path/to/file.txt"),  # Full file path
        ("file.txt", "file.txt"),  # File path in root directory
        ("/path/to/dir/", "path/to/dir"),  # Directory path
        ("/path/to/dir/*", "path/to/dir"),  # Path with wildcard at the end
        ("/path/to/dir/*.csv", "path/to/dir"),  # Path with wildcard in file name
        ("/path/to/dir/file.*", "path/to/dir"),  # Path with wildcard in file extension
        ("/path/to/*/dir/file.csv", "path/to"),  # Path with wildcard in the middle
        ("/path/to/dir/pre_", "path/to/dir"),  # Path with prefix
        ("/pre", "/"),  # Prefix only
        ("/*", "/"),  # Wildcard after root slash
        ("/", "/"),  # Root path
        ("", "/"),  # Empty path
        (".", "/"),  # Current directory
        ("*", "/"),  # Wildcard only
    ],
)
def test_extract_ds_name_from_gcs_path(input_path, expected_output):
    assert extract_ds_name_from_gcs_path(input_path) == expected_output


@patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
@patch("airflow.providers.openlineage.conf.is_disabled")
def test_is_openlineage_provider_accessible(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = False
    mock_get_listener.return_value = True
    assert _is_openlineage_provider_accessible() is True


@patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
@patch("airflow.providers.openlineage.conf.is_disabled")
def test_is_openlineage_provider_disabled(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = True
    assert _is_openlineage_provider_accessible() is False


@patch("airflow.providers.openlineage.plugins.listener.get_openlineage_listener")
@patch("airflow.providers.openlineage.conf.is_disabled")
def test_is_openlineage_listener_not_found(mock_is_disabled, mock_get_listener):
    mock_is_disabled.return_value = False
    mock_get_listener.return_value = None
    assert _is_openlineage_provider_accessible() is False


@pytest.mark.parametrize(
    "job, expected",
    [
        ({"sparkJob": {}}, "sparkJob"),
        ({"pysparkJob": {}}, "pysparkJob"),
        ({"spark_job": {}}, "spark_job"),
        ({"pyspark_job": {}}, "pyspark_job"),
        ({"unsupportedJob": {}}, None),
        ({}, None),
    ],
)
def test_extract_supported_job_type_from_dataproc_job(job, expected):
    assert _extract_supported_job_type_from_dataproc_job(job) == expected


def test_replace_dataproc_job_properties_injection():
    job_type = "sparkJob"
    original_job = {job_type: {"properties": {"existingProperty": "value"}}}
    new_properties = {"newProperty": "newValue"}

    updated_job = _replace_dataproc_job_properties(original_job, job_type, new_properties)

    assert updated_job[job_type]["properties"] == {"newProperty": "newValue"}
    assert original_job[job_type]["properties"] == {"existingProperty": "value"}


def test_replace_dataproc_job_properties_key_error():
    original_job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    job_type = "nonExistentJobType"
    new_properties = {"newProperty": "newValue"}

    with pytest.raises(KeyError, match=f"Job type '{job_type}' is missing in the job definition."):
        _replace_dataproc_job_properties(original_job, job_type, new_properties)


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_job_provider_not_accessible(mock_is_accessible):
    mock_is_accessible.return_value = False
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, None, True)
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._extract_supported_job_type_from_dataproc_job")
def test_inject_openlineage_properties_into_dataproc_job_unsupported_job_type(
    mock_extract_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_extract_job_type.return_value = None
    job = {"unsupportedJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, None, True)
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._extract_supported_job_type_from_dataproc_job")
def test_inject_openlineage_properties_into_dataproc_job_no_inject_parent_job_info(
    mock_extract_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_extract_job_type.return_value = "sparkJob"
    inject_parent_job_info = False
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, None, inject_parent_job_info)
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_job(mock_is_ol_accessible):
    mock_is_ol_accessible.return_value = True
    context = {
        "ti": MagicMock(
            dag_id="dag_id",
            task_id="task_id",
            try_number=1,
            map_index=1,
            logical_date=dt.datetime(2024, 11, 11),
        )
    }
    expected_properties = {
        "existingProperty": "value",
        "spark.openlineage.parentJobName": "dag_id.task_id",
        "spark.openlineage.parentJobNamespace": "default",
        "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
    }
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, context, True)
    assert result == {"sparkJob": {"properties": expected_properties}}
