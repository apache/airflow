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
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.bigquery.table import Table
from google.cloud.dataproc_v1 import Batch, RuntimeConfig
from openlineage.client.facet_v2 import column_lineage_dataset
from openlineage.client.transport import HttpConfig, HttpTransport

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
    _extract_dataproc_batch_properties,
    _extract_supported_job_type_from_dataproc_job,
    _is_dataproc_batch_of_supported_type,
    _is_openlineage_provider_accessible,
    _replace_dataproc_batch_properties,
    _replace_dataproc_job_properties,
    extract_ds_name_from_gcs_path,
    get_facets_from_bq_table,
    get_identity_column_lineage_facet,
    get_namespace_name_from_source_uris,
    inject_openlineage_properties_into_dataproc_batch,
    inject_openlineage_properties_into_dataproc_job,
    inject_openlineage_properties_into_dataproc_workflow_template,
    merge_column_lineage_facets,
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
EXAMPLE_BATCH = {
    "spark_batch": {
        "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
        "main_class": "org.apache.spark.examples.SparkPi",
    },
    "runtime_config": {"properties": {"existingProperty": "value"}},
}
EXAMPLE_TEMPLATE = {
    "id": "test-workflow",
    "placement": {
        "cluster_selector": {
            "zone": "europe-central2-c",
            "cluster_labels": {"key": "value"},
        }
    },
    "jobs": [
        {
            "step_id": "job_1",
            "pyspark_job": {
                "main_python_file_uri": "gs://bucket1/spark_job.py",
                "properties": {
                    "spark.sql.shuffle.partitions": "1",
                },
            },
        }
    ],
}
EXAMPLE_CONTEXT = {
    "ti": MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=1,
        logical_date=dt.datetime(2024, 11, 11),
        dag_run=MagicMock(
            run_after=dt.datetime(2024, 11, 11), logical_date=dt.datetime(2024, 11, 11), clear_number=0
        ),
    )
}
OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG = {
    "url": "https://some-custom.url",
    "endpoint": "/api/custom",
    "timeout": 123,
    "compression": "gzip",
    "custom_headers": {
        "key1": "val1",
        "key2": "val2",
    },
    "auth": {
        "type": "api_key",
        "apiKey": "secret_123",
    },
}
OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES = {
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "https://some-custom.url",
    "spark.openlineage.transport.endpoint": "/api/custom",
    "spark.openlineage.transport.auth.type": "api_key",
    "spark.openlineage.transport.auth.apiKey": "Bearer secret_123",
    "spark.openlineage.transport.compression": "gzip",
    "spark.openlineage.transport.headers.key1": "val1",
    "spark.openlineage.transport.headers.key2": "val2",
    "spark.openlineage.transport.timeoutInMillis": "123000",
}
OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES = {
    "spark.openlineage.parentJobName": "dag_id.task_id",
    "spark.openlineage.parentJobNamespace": "default",
    "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
    "spark.openlineage.rootParentJobName": "dag_id",
    "spark.openlineage.rootParentJobNamespace": "default",
    "spark.openlineage.rootParentRunId": "01931885-2800-799d-8041-88a263ffa0d8",
}


def read_file_json(file):
    with open(file=file) as f:
        return json.loads(f.read())


class TableMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.inputs = [
            read_file_json("providers/google/tests/unit/google/cloud/utils/table_details.json"),
            read_file_json("providers/google/tests/unit/google/cloud/utils/out_table_details.json"),
        ]

    @property
    def _properties(self):
        return self.inputs.pop()


def test_merge_column_lineage_facets():
    result = merge_column_lineage_facets(
        [
            ColumnLineageDatasetFacet(
                fields={
                    "c": Fields(
                        inputFields=[
                            InputField(
                                "bigquery",
                                "a.b.1",
                                "c",
                                [
                                    column_lineage_dataset.Transformation(
                                        "type", "some_subtype", "desc", False
                                    )
                                ],
                            )
                        ],
                        transformationType="IDENTITY",
                        transformationDescription="IDENTICAL",
                    ),
                    "d": Fields(
                        inputFields=[
                            InputField(
                                "bigquery",
                                "a.b.2",
                                "d",
                                [column_lineage_dataset.Transformation("t", "s", "d", False)],
                            )
                        ],
                        transformationType="",
                        transformationDescription="",
                    ),
                }
            ),
            ColumnLineageDatasetFacet(
                fields={
                    "c": Fields(
                        inputFields=[
                            InputField(
                                "bigquery",
                                "a.b.3",
                                "x",
                                [
                                    column_lineage_dataset.Transformation(
                                        "another_type", "different_subtype", "example", True
                                    )
                                ],
                            ),
                            InputField(
                                "bigquery",
                                "a.b.1",
                                "c",
                                [
                                    column_lineage_dataset.Transformation(
                                        "diff_type", "diff_subtype", "diff", True
                                    )
                                ],
                            ),
                        ],
                        transformationType="",
                        transformationDescription="",
                    ),
                    "e": Fields(
                        inputFields=[InputField("bigquery", "a.b.1", "e")],
                        transformationType="IDENTITY",
                        transformationDescription="IDENTICAL",
                    ),
                }
            ),
            ColumnLineageDatasetFacet(
                fields={
                    "c": Fields(
                        inputFields=[InputField("bigquery", "a.b.3", "x")],
                        transformationType="",
                        transformationDescription="",
                    )
                }
            ),
        ]
    )
    assert result == ColumnLineageDatasetFacet(
        fields={
            "c": Fields(
                inputFields=[
                    InputField(
                        "bigquery",
                        "a.b.1",
                        "c",
                        [
                            column_lineage_dataset.Transformation("type", "some_subtype", "desc", False),
                            column_lineage_dataset.Transformation("diff_type", "diff_subtype", "diff", True),
                        ],
                    ),
                    InputField(
                        "bigquery",
                        "a.b.3",
                        "x",
                        [
                            column_lineage_dataset.Transformation(
                                "another_type", "different_subtype", "example", True
                            )
                        ],
                    ),
                ],
                transformationType="",
                transformationDescription="",
            ),
            "d": Fields(
                inputFields=[
                    InputField(
                        "bigquery",
                        "a.b.2",
                        "d",
                        [column_lineage_dataset.Transformation("t", "s", "d", False)],
                    )
                ],
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
    ("input_path", "expected_output"),
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
    ("job", "expected"),
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

    result = inject_openlineage_properties_into_dataproc_job(job, EXAMPLE_CONTEXT, True, True)
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._extract_supported_job_type_from_dataproc_job")
def test_inject_openlineage_properties_into_dataproc_job_unsupported_job_type(
    mock_extract_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_extract_job_type.return_value = None
    job = {"unsupportedJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, EXAMPLE_CONTEXT, True, True)
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._extract_supported_job_type_from_dataproc_job")
def test_inject_openlineage_properties_into_dataproc_job_no_injection(
    mock_extract_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_extract_job_type.return_value = "sparkJob"
    inject_parent_job_info = False
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(
        job, EXAMPLE_CONTEXT, inject_parent_job_info, False
    )
    assert result == job


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_job_parent_info_only(mock_is_ol_accessible):
    mock_is_ol_accessible.return_value = True
    expected_properties = {
        "existingProperty": "value",
        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
    }
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, EXAMPLE_CONTEXT, True, False)
    assert result == {"sparkJob": {"properties": expected_properties}}


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_job_transport_info_only(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    expected_properties = {
        "existingProperty": "value",
        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
    }
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, EXAMPLE_CONTEXT, False, True)
    assert result == {"sparkJob": {"properties": expected_properties}}


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_job_all_injections(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    expected_properties = {
        "existingProperty": "value",
        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
    }
    job = {"sparkJob": {"properties": {"existingProperty": "value"}}}
    result = inject_openlineage_properties_into_dataproc_job(job, EXAMPLE_CONTEXT, True, True)
    assert result == {"sparkJob": {"properties": expected_properties}}


@pytest.mark.parametrize(
    ("batch", "expected"),
    [
        ({"spark_batch": {}}, True),
        ({"pyspark_batch": {}}, True),
        ({"unsupported_batch": {}}, False),
        ({}, False),
        (Batch(spark_batch={"jar_file_uris": ["uri"]}), True),
        (Batch(pyspark_batch={"main_python_file_uri": "uri"}), True),
        (Batch(pyspark_batch={}), False),
        (Batch(spark_sql_batch={}), False),
        (Batch(), False),
    ],
)
def test_is_dataproc_batch_of_supported_type(batch, expected):
    assert _is_dataproc_batch_of_supported_type(batch) == expected


def test_extract_dataproc_batch_properties_batch_object_with_runtime_object():
    properties = {"key1": "value1", "key2": "value2"}
    mock_runtime_config = RuntimeConfig(properties=properties)
    mock_batch = Batch(runtime_config=mock_runtime_config)
    result = _extract_dataproc_batch_properties(mock_batch)
    assert result == properties


def test_extract_dataproc_batch_properties_batch_object_with_runtime_dict():
    properties = {"key1": "value1", "key2": "value2"}
    mock_batch = Batch(runtime_config={"properties": properties})
    result = _extract_dataproc_batch_properties(mock_batch)
    assert result == {"key1": "value1", "key2": "value2"}


def test_extract_dataproc_batch_properties_batch_object_with_runtime_object_empty():
    mock_batch = Batch(runtime_config=RuntimeConfig())
    result = _extract_dataproc_batch_properties(mock_batch)
    assert result == {}


def test_extract_dataproc_batch_properties_dict_with_runtime_config_object():
    properties = {"key1": "value1", "key2": "value2"}
    mock_runtime_config = RuntimeConfig(properties=properties)
    mock_batch_dict = {"runtime_config": mock_runtime_config}

    result = _extract_dataproc_batch_properties(mock_batch_dict)
    assert result == properties


def test_extract_dataproc_batch_properties_dict_with_properties_dict():
    properties = {"key1": "value1", "key2": "value2"}
    mock_batch_dict = {"runtime_config": {"properties": properties}}
    result = _extract_dataproc_batch_properties(mock_batch_dict)
    assert result == properties


def test_extract_dataproc_batch_properties_empty_runtime_config():
    mock_batch_dict = {"runtime_config": {}}
    result = _extract_dataproc_batch_properties(mock_batch_dict)
    assert result == {}


def test_extract_dataproc_batch_properties_empty_dict():
    assert _extract_dataproc_batch_properties({}) == {}


def test_extract_dataproc_batch_properties_empty_batch():
    assert _extract_dataproc_batch_properties(Batch()) == {}


def test_replace_dataproc_batch_properties_with_batch_object():
    original_batch = Batch(
        spark_batch={
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        runtime_config=RuntimeConfig(properties={"existingProperty": "value"}),
    )
    new_properties = {"newProperty": "newValue"}
    expected_batch = Batch(
        spark_batch={
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        runtime_config=RuntimeConfig(properties={"newProperty": "newValue"}),
    )

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch.runtime_config.properties == {"existingProperty": "value"}
    assert original_batch.spark_batch.main_class == "org.apache.spark.examples.SparkPi"


def test_replace_dataproc_batch_properties_with_batch_object_and_run_time_config_dict():
    original_batch = Batch(
        spark_batch={
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        runtime_config={"properties": {"existingProperty": "value"}},
    )
    new_properties = {"newProperty": "newValue"}
    expected_batch = Batch(
        spark_batch={
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        runtime_config={"properties": {"newProperty": "newValue"}},
    )

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch.runtime_config.properties == {"existingProperty": "value"}
    assert original_batch.spark_batch.main_class == "org.apache.spark.examples.SparkPi"


def test_replace_dataproc_batch_properties_with_empty_batch_object():
    original_batch = Batch()
    new_properties = {"newProperty": "newValue"}
    expected_batch = Batch(runtime_config=RuntimeConfig(properties={"newProperty": "newValue"}))

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch == Batch()


def test_replace_dataproc_batch_properties_with_dict():
    original_batch = {
        "spark_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        "runtime_config": {"properties": {"existingProperty": "value"}},
    }
    new_properties = {"newProperty": "newValue"}
    expected_batch = {
        "spark_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        "runtime_config": {"properties": {"newProperty": "newValue"}},
    }

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch["runtime_config"]["properties"] == {"existingProperty": "value"}
    assert original_batch["spark_batch"]["main_class"] == "org.apache.spark.examples.SparkPi"


def test_replace_dataproc_batch_properties_with_dict_and_run_time_config_object():
    original_batch = {
        "spark_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        "runtime_config": RuntimeConfig(properties={"existingProperty": "value"}),
    }
    new_properties = {"newProperty": "newValue"}
    expected_batch = {
        "spark_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        "runtime_config": RuntimeConfig(properties={"newProperty": "newValue"}),
    }

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch["runtime_config"].properties == {"existingProperty": "value"}
    assert original_batch["spark_batch"]["main_class"] == "org.apache.spark.examples.SparkPi"


def test_replace_dataproc_batch_properties_with_empty_dict():
    original_batch = {}
    new_properties = {"newProperty": "newValue"}
    expected_batch = {"runtime_config": {"properties": {"newProperty": "newValue"}}}

    updated_batch = _replace_dataproc_batch_properties(original_batch, new_properties)

    assert updated_batch == expected_batch
    assert original_batch == {}


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_batch_provider_not_accessible(mock_is_accessible):
    mock_is_accessible.return_value = False
    result = inject_openlineage_properties_into_dataproc_batch(EXAMPLE_BATCH, None, True, True)
    assert result == EXAMPLE_BATCH


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._is_dataproc_batch_of_supported_type")
def test_inject_openlineage_properties_into_dataproc_batch_unsupported_batch_type(
    mock_valid_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_valid_job_type.return_value = False
    batch = {
        "unsupported_batch": {
            "jar_file_uris": ["file:///usr/lib/spark/examples/jars/spark-examples.jar"],
            "main_class": "org.apache.spark.examples.SparkPi",
        },
        "runtime_config": {"properties": {"existingProperty": "value"}},
    }
    result = inject_openlineage_properties_into_dataproc_batch(batch, None, True, True)
    assert result == batch


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._is_dataproc_batch_of_supported_type")
def test_inject_openlineage_properties_into_dataproc_batch_no_injection(
    mock_valid_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_valid_job_type.return_value = True
    result = inject_openlineage_properties_into_dataproc_batch(EXAMPLE_BATCH, None, False, False)
    assert result == EXAMPLE_BATCH


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_batch_parent_info_only(mock_is_ol_accessible):
    mock_is_ol_accessible.return_value = True
    expected_properties = {
        "existingProperty": "value",
        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
    }
    expected_batch = {
        **EXAMPLE_BATCH,
        "runtime_config": {"properties": expected_properties},
    }
    result = inject_openlineage_properties_into_dataproc_batch(EXAMPLE_BATCH, EXAMPLE_CONTEXT, True, False)
    assert result == expected_batch


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_batch_transport_info_only(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    expected_properties = {"existingProperty": "value", **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES}
    expected_batch = {
        **EXAMPLE_BATCH,
        "runtime_config": {"properties": expected_properties},
    }
    result = inject_openlineage_properties_into_dataproc_batch(EXAMPLE_BATCH, EXAMPLE_CONTEXT, False, True)
    assert result == expected_batch


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_batch_all_injections(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    expected_properties = {
        "existingProperty": "value",
        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
    }
    expected_batch = {
        **EXAMPLE_BATCH,
        "runtime_config": {"properties": expected_properties},
    }
    result = inject_openlineage_properties_into_dataproc_batch(EXAMPLE_BATCH, EXAMPLE_CONTEXT, True, True)
    assert result == expected_batch


@pytest.mark.parametrize(
    ("input_uris", "expected_output"),
    [
        (["gs://bucket/blob"], {("gs://bucket", "/")}),
        (["gs://bucket/blob/*"], {("gs://bucket", "blob")}),
        (
            [
                "https://googleapis.com/bigtable/projects/project/instances/instance/appProfiles/profile/tables/table",
                "https://googleapis.com/bigtable/projects/project/instances/instance/tables/table",
            ],
            {("bigtable://project/instance", "table"), ("bigtable://project/instance", "table")},
        ),
        (
            [
                "gs://bucket/blob",
                "https://googleapis.com/bigtable/projects/project/instances/instance/tables/table",
                "invalid_uri",
            ],
            {("gs://bucket", "/"), ("bigtable://project/instance", "table")},
        ),
        ([], set()),
        (["invalid_uri"], set()),
    ],
)
def test_get_namespace_name_from_source_uris(input_uris, expected_output):
    assert get_namespace_name_from_source_uris(input_uris) == expected_output


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_workflow_template_provider_not_accessible(
    mock_is_accessible,
):
    mock_is_accessible.return_value = False
    template = {"workflow": "template"}  # It does not matter what the dict is, we should return it unmodified
    result = inject_openlineage_properties_into_dataproc_workflow_template(template, None, True, True)
    assert result == template


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
@patch("airflow.providers.google.cloud.openlineage.utils._extract_supported_job_type_from_dataproc_job")
def test_inject_openlineage_properties_into_dataproc_workflow_template_no_injection(
    mock_extract_job_type, mock_is_accessible
):
    mock_is_accessible.return_value = True
    mock_extract_job_type.return_value = "sparkJob"
    template = {"workflow": "template"}  # It does not matter what the dict is, we should return it unmodified
    result = inject_openlineage_properties_into_dataproc_workflow_template(template, None, False, False)
    assert result == template


@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_workflow_template_parent_info_only(
    mock_is_ol_accessible,
):
    mock_is_ol_accessible.return_value = True
    template = {
        **EXAMPLE_TEMPLATE,
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.parentJobNamespace": "test",
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
        ],
    }
    expected_template = {
        **EXAMPLE_TEMPLATE,
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {  # Injected properties
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.parentJobName": "dag_id.task_id",
                        "spark.openlineage.parentJobNamespace": "default",
                        "spark.openlineage.parentRunId": "01931885-2800-7be7-aa8d-aaa15c337267",
                        "spark.openlineage.rootParentJobName": "dag_id",
                        "spark.openlineage.rootParentJobNamespace": "default",
                        "spark.openlineage.rootParentRunId": "01931885-2800-799d-8041-88a263ffa0d8",
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {  # Not modified because it's already present
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.parentJobNamespace": "test",
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {  # Not modified because it's unsupported job type
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
        ],
    }
    result = inject_openlineage_properties_into_dataproc_workflow_template(
        template, EXAMPLE_CONTEXT, True, False
    )
    assert result == expected_template


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_workflow_template_transport_info_only(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    template = {
        **EXAMPLE_TEMPLATE,
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.transport.type": "console",
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
        ],
    }
    expected_template = {
        **EXAMPLE_TEMPLATE,
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {  # Injected properties
                        "spark.sql.shuffle.partitions": "1",
                        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {  # Not modified because it's already present
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.transport.type": "console",
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {  # Not modified because it's unsupported job type
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
        ],
    }
    result = inject_openlineage_properties_into_dataproc_workflow_template(
        template, EXAMPLE_CONTEXT, False, True
    )
    assert result == expected_template


@patch("airflow.providers.openlineage.utils.spark.get_openlineage_listener")
@patch("airflow.providers.google.cloud.openlineage.utils._is_openlineage_provider_accessible")
def test_inject_openlineage_properties_into_dataproc_workflow_template_all_injections(
    mock_is_ol_accessible, mock_ol_listener
):
    mock_is_ol_accessible.return_value = True
    fake_listener = mock.MagicMock()
    mock_ol_listener.return_value = fake_listener
    fake_listener.adapter.get_or_create_openlineage_client.return_value.transport = HttpTransport(
        HttpConfig.from_dict(OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_CONFIG)
    )
    template = {
        **EXAMPLE_TEMPLATE,
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.transport.type": "console",
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
            {
                "step_id": "job_4",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.parentJobNamespace": "test",
                    },
                },
            },
        ],
    }
    expected_template = {
        "id": "test-workflow",
        "placement": {
            "cluster_selector": {
                "zone": "europe-central2-c",
                "cluster_labels": {"key": "value"},
            }
        },
        "jobs": [
            {
                "step_id": "job_1",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket1/spark_job.py",
                    "properties": {  # Injected properties
                        "spark.sql.shuffle.partitions": "1",
                        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                    },
                },
            },
            {
                "step_id": "job_2",
                "pyspark_job": {  # Only parent info injected
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.transport.type": "console",
                        **OPENLINEAGE_PARENT_JOB_EXAMPLE_SPARK_PROPERTIES,
                    },
                },
            },
            {
                "step_id": "job_3",
                "hive_job": {  # Not modified because it's unsupported job type
                    "main_python_file_uri": "gs://bucket3/hive_job.py",
                    "properties": {
                        "spark.sql.shuffle.partitions": "1",
                    },
                },
            },
            {
                "step_id": "job_4",
                "pyspark_job": {
                    "main_python_file_uri": "gs://bucket2/spark_job.py",
                    "properties": {  # Only transport info injected
                        "spark.sql.shuffle.partitions": "1",
                        "spark.openlineage.parentJobNamespace": "test",
                        **OPENLINEAGE_HTTP_TRANSPORT_EXAMPLE_SPARK_PROPERTIES,
                    },
                },
            },
        ],
    }
    result = inject_openlineage_properties_into_dataproc_workflow_template(
        template, EXAMPLE_CONTEXT, True, True
    )
    assert result == expected_template
