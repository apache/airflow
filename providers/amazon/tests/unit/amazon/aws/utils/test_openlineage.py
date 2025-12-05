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


# --- Glue OpenLineage tests ---

from datetime import datetime
from unittest.mock import MagicMock

from airflow.providers.amazon.aws.utils.openlineage import (
    _format_glue_customer_env_vars,
    _is_parent_job_info_present_in_glue_env_vars,
    _parse_glue_customer_env_vars,
    inject_parent_job_information_into_glue_script_args,
)

EXAMPLE_CONTEXT = {
    "ti": MagicMock(
        dag_id="dag_id",
        task_id="task_id",
        try_number=1,
        map_index=-1,
        logical_date=datetime(2024, 11, 11),
        dag_run=MagicMock(logical_date=datetime(2024, 11, 11), clear_number=0),
    )
}


class TestParseGlueCustomerEnvVars:
    def test_empty_string(self):
        assert _parse_glue_customer_env_vars("") == {}

    def test_none(self):
        assert _parse_glue_customer_env_vars(None) == {}

    def test_simple_single(self):
        result = _parse_glue_customer_env_vars("KEY1=VAL1")
        assert result == {"KEY1": "VAL1"}

    def test_simple_multiple(self):
        result = _parse_glue_customer_env_vars("KEY1=VAL1,KEY2=VAL2")
        assert result == {"KEY1": "VAL1", "KEY2": "VAL2"}

    def test_quoted_value_with_comma(self):
        result = _parse_glue_customer_env_vars('KEY1=VAL1,KEY2="val with, comma"')
        assert result == {"KEY1": "VAL1", "KEY2": "val with, comma"}

    def test_quoted_value_with_space(self):
        result = _parse_glue_customer_env_vars('KEY1="value with spaces"')
        assert result == {"KEY1": "value with spaces"}

    def test_value_with_equals(self):
        result = _parse_glue_customer_env_vars("KEY1=val=ue")
        assert result == {"KEY1": "val=ue"}


class TestFormatGlueCustomerEnvVars:
    def test_empty(self):
        result = _format_glue_customer_env_vars({})
        assert result == ""

    def test_simple_single(self):
        result = _format_glue_customer_env_vars({"KEY1": "VAL1"})
        assert result == "KEY1=VAL1"

    def test_simple_multiple(self):
        result = _format_glue_customer_env_vars({"KEY1": "VAL1", "KEY2": "VAL2"})
        assert "KEY1=VAL1" in result
        assert "KEY2=VAL2" in result

    def test_value_with_comma_gets_quoted(self):
        result = _format_glue_customer_env_vars({"KEY": "val,ue"})
        assert result == 'KEY="val,ue"'

    def test_value_with_space_gets_quoted(self):
        result = _format_glue_customer_env_vars({"KEY": "val ue"})
        assert result == 'KEY="val ue"'

    def test_roundtrip(self):
        original = {"KEY1": "simple", "KEY2": "has, comma", "KEY3": "has space"}
        formatted = _format_glue_customer_env_vars(original)
        parsed = _parse_glue_customer_env_vars(formatted)
        assert parsed == original


class TestIsParentJobInfoPresentInGlueEnvVars:
    def test_empty_returns_false(self):
        assert _is_parent_job_info_present_in_glue_env_vars({}) is False

    def test_unrelated_vars_return_false(self):
        script_args = {"--customer-driver-env-vars": "MY_VAR=value"}
        assert _is_parent_job_info_present_in_glue_env_vars(script_args) is False

    def test_parent_var_returns_true(self):
        script_args = {"--customer-driver-env-vars": "OPENLINEAGE_PARENT_JOB_NAME=test"}
        assert _is_parent_job_info_present_in_glue_env_vars(script_args) is True

    def test_root_parent_var_returns_true(self):
        script_args = {"--customer-driver-env-vars": "OPENLINEAGE_ROOT_PARENT_RUN_ID=123"}
        assert _is_parent_job_info_present_in_glue_env_vars(script_args) is True

    def test_executor_env_vars_also_checked(self):
        script_args = {"--customer-executor-env-vars": "OPENLINEAGE_PARENT_RUN_ID=123"}
        assert _is_parent_job_info_present_in_glue_env_vars(script_args) is True

    def test_no_script_args_key(self):
        script_args = {"--other-arg": "value"}
        assert _is_parent_job_info_present_in_glue_env_vars(script_args) is False


class TestInjectParentJobInformationIntoGlueScriptArgs:
    @mock.patch(
        "airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information",
        return_value=None,
    )
    def test_skips_when_openlineage_not_available(self, mock_get_parent_info):
        result = inject_parent_job_information_into_glue_script_args({}, EXAMPLE_CONTEXT)
        assert result == {}

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_injects_into_empty_script_args(self, mock_get_parent_info):
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

        assert "--customer-driver-env-vars" in result
        env_vars = _parse_glue_customer_env_vars(result["--customer-driver-env-vars"])
        assert env_vars["OPENLINEAGE_PARENT_JOB_NAMESPACE"] == "default"
        assert env_vars["OPENLINEAGE_PARENT_JOB_NAME"] == "dag_id.task_id"
        assert env_vars["OPENLINEAGE_PARENT_RUN_ID"] == "uuid-123"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_JOB_NAMESPACE"] == "default"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_JOB_NAME"] == "dag_id"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_RUN_ID"] == "uuid-456"

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_merges_with_existing_env_vars(self, mock_get_parent_info):
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        existing = {"--customer-driver-env-vars": "EXISTING_VAR=value"}
        result = inject_parent_job_information_into_glue_script_args(existing, EXAMPLE_CONTEXT)

        env_vars = _parse_glue_customer_env_vars(result["--customer-driver-env-vars"])
        assert "EXISTING_VAR" in env_vars
        assert env_vars["EXISTING_VAR"] == "value"
        assert "OPENLINEAGE_PARENT_JOB_NAME" in env_vars

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_preserves_other_script_args(self, mock_get_parent_info):
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
        assert "--customer-driver-env-vars" in result

    @mock.patch("airflow.providers.amazon.aws.utils.openlineage.get_parent_job_information")
    def test_preserves_existing_customer_driver_env_vars(self, mock_get_parent_info):
        """Key test: verify that existing user-defined env vars are preserved when injecting OL vars."""
        from airflow.providers.openlineage.utils.spark import ParentJobInformation

        mock_get_parent_info.return_value = ParentJobInformation(
            parent_job_namespace="default",
            parent_job_name="dag.task",
            parent_run_id="uuid-123",
            root_parent_job_namespace="default",
            root_parent_job_name="dag",
            root_parent_run_id="uuid-456",
        )

        # User has set multiple custom env vars including ones with special characters
        existing = {
            "--customer-driver-env-vars": 'MY_VAR=value1,ANOTHER_VAR=value2,COMPLEX_VAR="value with, comma"',
            "--other-arg": "other_value",
        }
        result = inject_parent_job_information_into_glue_script_args(existing, EXAMPLE_CONTEXT)

        # Parse the resulting env vars
        env_vars = _parse_glue_customer_env_vars(result["--customer-driver-env-vars"])

        # Verify ALL original user env vars are preserved
        assert env_vars["MY_VAR"] == "value1"
        assert env_vars["ANOTHER_VAR"] == "value2"
        assert env_vars["COMPLEX_VAR"] == "value with, comma"

        # Verify OpenLineage env vars were added
        assert env_vars["OPENLINEAGE_PARENT_JOB_NAMESPACE"] == "default"
        assert env_vars["OPENLINEAGE_PARENT_JOB_NAME"] == "dag.task"
        assert env_vars["OPENLINEAGE_PARENT_RUN_ID"] == "uuid-123"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_JOB_NAMESPACE"] == "default"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_JOB_NAME"] == "dag"
        assert env_vars["OPENLINEAGE_ROOT_PARENT_RUN_ID"] == "uuid-456"

        # Verify other script args are unchanged
        assert result["--other-arg"] == "other_value"

        # Total count: 3 user vars + 6 OL vars = 9 vars
        assert len(env_vars) == 9
