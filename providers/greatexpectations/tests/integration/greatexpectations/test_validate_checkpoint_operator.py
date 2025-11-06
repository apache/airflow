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

from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
import pytest

from airflow.providers.greatexpectations.common.errors import GXValidationFailed
from airflow.providers.greatexpectations.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)

if TYPE_CHECKING:
    from great_expectations.data_context import AbstractDataContext, FileDataContext

from tests.integration.greatexpectations.conftest import is_valid_gx_cloud_url, rand_name


class TestValidateCheckpointOperator:
    """Test cases for GXValidateCheckpointOperator with different context types."""

    COL_NAME = "my_column"

    @pytest.fixture
    def data_frame(self) -> pd.DataFrame:
        """Sample data frame for test cases"""
        return pd.DataFrame({self.COL_NAME: [1, 2, 3, 4, 5]})

    @pytest.fixture
    def configure_checkpoint(self) -> Callable[[AbstractDataContext], gx.Checkpoint]:
        """Configure an arbitrary checkpoint to a given context.

        This will pass for the data_frame fixture.
        """

        def _configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            batch_definition = (
                context.data_sources.add_pandas(name=rand_name())
                .add_dataframe_asset(rand_name())
                .add_batch_definition_whole_dataframe(rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column=self.COL_NAME,
                            min_value=0,
                            max_value=100,
                        )
                    ],
                )
            )
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=batch_definition,
                    suite=suite,
                )
            )
            checkpoint = context.checkpoints.add(
                gx.Checkpoint(name=rand_name(), validation_definitions=[validation_definition])
            )

            return checkpoint

        return _configure_checkpoint

    @pytest.fixture
    def configure_checkpoint_with_cleanup(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        ensure_checkpoint_cleanup: Callable[[str], None],
        ensure_validation_definition_cleanup: Callable[[str], None],
        ensure_suite_cleanup: Callable[[str], None],
        ensure_data_source_cleanup: Callable[[str], None],
    ) -> Callable[[AbstractDataContext], gx.Checkpoint]:
        """Configure an arbitrary checkpoint to a given cloud context.

        The models will be cleaned up when the test ends
        This will pass for the data_frame fixture.
        """

        def _configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            # get the checkpoint
            checkpoint = configure_checkpoint(context)

            # ensure cleanup after the test ends
            ensure_checkpoint_cleanup(checkpoint.name)
            for vd in checkpoint.validation_definitions:
                ensure_validation_definition_cleanup(vd.name)
                ensure_suite_cleanup(vd.suite.name)
                ensure_data_source_cleanup(vd.data.data_asset.datasource.name)

            # return the checkpoint for use in test
            return checkpoint

        return _configure_checkpoint

    def test_with_cloud_context(
        self,
        configure_checkpoint_with_cleanup: Callable[[AbstractDataContext], gx.Checkpoint],
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with cloud contexts."""

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            context_type="cloud",
            task_id="cloud_context",
            configure_checkpoint=configure_checkpoint_with_cleanup,
            batch_parameters={"dataframe": data_frame},
        )

        mock_ti = Mock()
        validate_cloud_checkpoint.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True
        # make sure we have something that looks like a valid result url
        assert is_valid_gx_cloud_url(pushed_result["validation_results"][0]["result_url"])

    def test_with_file_context(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        tmp_path: Path,
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with file contexts."""

        def configure_context() -> FileDataContext:
            return gx.get_context(mode="file", project_root_dir=tmp_path)

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            configure_file_data_context=configure_context,
            task_id="file_context",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": data_frame},
        )

        mock_ti = Mock()
        validate_cloud_checkpoint.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_with_ephemeral_context(
        self,
        configure_checkpoint: Callable[[AbstractDataContext], gx.Checkpoint],
        data_frame: pd.DataFrame,
    ) -> None:
        """Ensure GXValidateCheckpointOperator works with ephemeral contexts."""

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="ephemeral_context",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": data_frame},
        )

        mock_ti = Mock()
        validate_cloud_checkpoint.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_postgres_data_source(
        self,
        table_name: str,
        load_postgres_data: Callable[[list[dict]], None],
        postgres_connection_string: str,
    ) -> None:
        load_postgres_data(
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ]
        )

        def configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            bd = (
                context.data_sources.add_postgres(
                    name=rand_name(),
                    connection_string=postgres_connection_string,
                )
                .add_table_asset(name=rand_name(), table_name=table_name)
                .add_batch_definition_whole_table(name=rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        ),
                        gxe.ExpectTableRowCountToEqual(value=2),
                    ],
                )
            )
            vd = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=bd,
                    suite=suite,
                )
            )
            return context.checkpoints.add(gx.Checkpoint(name=rand_name(), validation_definitions=[vd]))

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="postgres_data_source",
            configure_checkpoint=configure_checkpoint,
        )

        mock_ti = Mock()
        validate_checkpoint.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_filesystem_data_source(
        self,
        load_csv_data: Callable[[Path, list[dict]], None],
        tmp_path: Path,
    ) -> None:
        file_name = "data.csv"
        data_location = tmp_path / file_name
        load_csv_data(
            data_location,
            [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 31},
            ],
        )

        def configure_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            bd = (
                context.data_sources.add_pandas_filesystem(
                    name=rand_name(),
                    base_directory=tmp_path,
                )
                .add_csv_asset(
                    name=rand_name(),
                )
                .add_batch_definition_path(
                    name=rand_name(),
                    path=file_name,
                )
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column="age",
                            min_value=0,
                            max_value=100,
                        ),
                        gxe.ExpectTableRowCountToEqual(value=2),
                    ],
                )
            )
            vd = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=bd,
                    suite=suite,
                )
            )
            return context.checkpoints.add(gx.Checkpoint(name=rand_name(), validation_definitions=[vd]))

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="filesystem_data_source",
            configure_checkpoint=configure_checkpoint,
        )

        mock_ti = Mock()
        validate_checkpoint.execute(context={"ti": mock_ti})

        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"] is True

    def test_validation_failure_raises_exception(self) -> None:
        """Test that validation failure raises GXValidationFailed exception."""
        # Create data that will fail validation
        failing_data_frame = pd.DataFrame({self.COL_NAME: [200, 300, 400]})  # values outside expected range

        def configure_failing_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            batch_definition = (
                context.data_sources.add_pandas(name=rand_name())
                .add_dataframe_asset(rand_name())
                .add_batch_definition_whole_dataframe(rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column=self.COL_NAME,
                            min_value=0,
                            max_value=100,  # values in data are > 100, so this will fail
                        )
                    ],
                )
            )
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=batch_definition,
                    suite=suite,
                )
            )
            checkpoint = context.checkpoints.add(
                gx.Checkpoint(name=rand_name(), validation_definitions=[validation_definition])
            )
            return checkpoint

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="validation_failure_test",
            configure_checkpoint=configure_failing_checkpoint,
            batch_parameters={"dataframe": failing_data_frame},
        )

        mock_ti = Mock()
        with pytest.raises(GXValidationFailed):
            validate_checkpoint.execute(context={"ti": mock_ti})

    def test_validation_failure_xcom_contains_result(self) -> None:
        """Test that when validation fails and exception is raised, xcom still contains the failed result."""
        # Create data that will fail validation
        failing_data_frame = pd.DataFrame({self.COL_NAME: [200, 300, 400]})  # values outside expected range

        def configure_failing_checkpoint(context: AbstractDataContext) -> gx.Checkpoint:
            batch_definition = (
                context.data_sources.add_pandas(name=rand_name())
                .add_dataframe_asset(rand_name())
                .add_batch_definition_whole_dataframe(rand_name())
            )
            suite = context.suites.add(
                gx.ExpectationSuite(
                    name=rand_name(),
                    expectations=[
                        gxe.ExpectColumnValuesToBeBetween(
                            column=self.COL_NAME,
                            min_value=0,
                            max_value=100,  # values in data are > 100, so this will fail
                        )
                    ],
                )
            )
            validation_definition = context.validation_definitions.add(
                gx.ValidationDefinition(
                    name=rand_name(),
                    data=batch_definition,
                    suite=suite,
                )
            )
            checkpoint = context.checkpoints.add(
                gx.Checkpoint(name=rand_name(), validation_definitions=[validation_definition])
            )
            return checkpoint

        validate_checkpoint = GXValidateCheckpointOperator(
            context_type="ephemeral",
            task_id="validation_failure_xcom_test",
            configure_checkpoint=configure_failing_checkpoint,
            batch_parameters={"dataframe": failing_data_frame},
        )

        mock_ti = Mock()
        with pytest.raises(GXValidationFailed):
            validate_checkpoint.execute(context={"ti": mock_ti})

        # Verify that xcom_push was called with the failed validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False
