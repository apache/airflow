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

from datetime import datetime
from unittest import mock

import pytest

from airflow.providers.amazon.aws.hooks.redshift_data import QueryExecutionOutput, RedshiftDataHook
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from airflow.providers.amazon.aws.utils.openlineage import (
    get_facets_from_redshift_table,
    get_identity_column_lineage_facet,
    inject_parent_job_information_into_glue_script_args,
)
from airflow.providers.common.compat.openlineage.facet import (
    ColumnLineageDatasetFacet,
    Dataset,
    Fields,
    InputField,
)

EXAMPLE_CONTEXT = {
    "ti": mock.MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=-1,
        logical_date=datetime(2024, 11, 11),
        dag_run=mock.MagicMock(logical_date=datetime(2024, 11, 11), clear_number=0),
    )
}


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


class TestInjectParentJobInformationIntoGlueScriptArgs:
    """Tests for injecting OpenLineage parent job info via --conf Spark properties."""

    @mock.patch(
        "airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information",
        return_value=None,
    )
    def test_skips_when_openlineage_not_available(self, mock_get_parent_info):
        """Returns unchanged script_args when OpenLineage is not available."""
        result = inject_parent_job_information_into_glue_script_args({}, EXAMPLE_CONTEXT)
        assert result == {}

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_injects_into_empty_script_args(self, mock_get_parent_info):
        """Injects --conf with Spark OpenLineage properties into empty script_args."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag_id.task_id",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag_id",
            root_parent_run_id="uuid-456",
        )

        result = inject_parent_job_information_into_glue_script_args({}, EXAMPLE_CONTEXT)

        assert "--conf" in result
        conf = result["--conf"]
        # Verify AWS Glue --conf format: "prop1=val1 --conf prop2=val2 --conf ..."
        assert "spark.openlineage.parentJobNamespace=default" in conf
        assert "spark.openlineage.parentJobName=dag_id.task_id" in conf
        assert "spark.openlineage.parentRunId=uuid-123" in conf
        assert "spark.openlineage.rootParentJobNamespace=default" in conf
        assert "spark.openlineage.rootParentJobName=dag_id" in conf
        assert "spark.openlineage.rootParentRunId=uuid-456" in conf
        # Verify the format uses " --conf " as separator between properties
        assert " --conf " in conf

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_appends_to_existing_conf(self, mock_get_parent_info):
        """Appends OpenLineage properties to existing --conf value with --conf prefix."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        existing = {"--conf": "spark.executor.memory=4g --conf spark.driver.memory=2g"}
        result = inject_parent_job_information_into_glue_script_args(existing, EXAMPLE_CONTEXT)

        conf = result["--conf"]
        # Original conf preserved at the beginning
        assert conf.startswith("spark.executor.memory=4g --conf spark.driver.memory=2g")
        # OpenLineage properties added with --conf prefix
        assert " --conf spark.openlineage.parentJobName=dag.task" in conf

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_skips_when_ol_properties_already_in_conf(self, mock_get_parent_info):
        """Skips injection if user already set spark.openlineage.parent* properties."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        # User already set their own parent job info
        existing = {"--conf": "spark.openlineage.parentJobName=custom_job"}
        result = inject_parent_job_information_into_glue_script_args(existing, EXAMPLE_CONTEXT)

        # Should return unchanged - user's values preserved
        assert result == existing

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_preserves_other_script_args(self, mock_get_parent_info):
        """Preserves other script_args while adding --conf."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        existing = {"--input": "s3://bucket/input", "--output": "s3://bucket/output"}
        result = inject_parent_job_information_into_glue_script_args(existing, EXAMPLE_CONTEXT)

        assert result["--input"] == "s3://bucket/input"
        assert result["--output"] == "s3://bucket/output"
        assert "--conf" in result

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_does_not_mutate_original_script_args(self, mock_get_parent_info):
        """Ensures original script_args dict is not mutated."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        original = {"--input": "s3://bucket/input"}
        original_copy = dict(original)
        result = inject_parent_job_information_into_glue_script_args(original, EXAMPLE_CONTEXT)

        # Original unchanged
        assert original == original_copy
        # Result is different object
        assert result is not original
        assert "--conf" in result
        assert "--conf" not in original
