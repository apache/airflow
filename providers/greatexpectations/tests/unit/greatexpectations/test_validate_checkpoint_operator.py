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
from collections.abc import Generator
from typing import TYPE_CHECKING, Literal
from unittest.mock import Mock

import pandas as pd
import pytest
from great_expectations import Checkpoint, ExpectationSuite, ValidationDefinition
from great_expectations.data_context import AbstractDataContext
from great_expectations.expectations import ExpectColumnValuesToBeInSet

if TYPE_CHECKING:
    from great_expectations.data_context import FileDataContext
    from pytest_mock import MockerFixture

    from airflow.utils.context import Context

from airflow.providers.greatexpectations.common.constants import USER_AGENT_STR
from airflow.providers.greatexpectations.exceptions import GXValidationFailed
from airflow.providers.greatexpectations.operators.validate_checkpoint import (
    GXValidateCheckpointOperator,
)


class TestValidateCheckpointOperator:
    def test_validate_dataframe(self) -> None:
        # arrange
        column_name = "col_A"

        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            # setup data source, asset, batch definition
            batch_definition = (
                context.data_sources.add_pandas(name="test datasource")
                .add_dataframe_asset("test asset")
                .add_batch_definition_whole_dataframe("test batch def")
            )
            # setup expectation suite
            expectation_suite = context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
                        ),
                    ],
                )
            )
            # setup validation definition
            validation_definition = context.validation_definitions.add(
                ValidationDefinition(
                    name="test validation definition",
                    data=batch_definition,
                    suite=expectation_suite,
                )
            )
            # setup checkpoint
            checkpoint = context.checkpoints.add(
                Checkpoint(
                    name="test checkpoint",
                    validation_definitions=[validation_definition],
                )
            )
            return checkpoint

        df = pd.DataFrame({column_name: ["a", "b", "c"]})

        validate_cloud_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters={"dataframe": df},
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act
        validate_cloud_checkpoint.execute(context=context)

        # assert
        # Get the result from xcom_push call
        pushed_result = mock_ti.xcom_push.call_args[1]["value"]
        assert pushed_result["success"]
        json.dumps(pushed_result)  # result must be json serializable

    def test_context_type_ephemeral(self, mocker: MockerFixture) -> None:
        """Expect that param context_type creates an EphemeralDataContext."""
        # arrange
        context_type: Literal["ephemeral"] = "ephemeral"
        configure_checkpoint = Mock()
        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type, user_agent_str=USER_AGENT_STR)

    def test_context_type_cloud(self, mocker: MockerFixture) -> None:
        """Expect that param context_type creates a CloudDataContext."""
        # arrange
        context_type: Literal["cloud"] = "cloud"
        configure_checkpoint = Mock()
        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
        )

        # act
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]
        validate_checkpoint.execute(context=context)

        # assert
        mock_gx.get_context.assert_called_once_with(mode=context_type, user_agent_str=USER_AGENT_STR)

    def test_context_type_filesystem(self, mocker: MockerFixture) -> None:
        """Expect that param context_type defers creation of data context to user."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()
        configure_file_data_context = Mock()
        mock_gx = Mock()
        mocker.patch.dict("sys.modules", {"great_expectations": mock_gx})
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        mock_gx.get_context.assert_not_called()

    def test_user_configured_context_is_passed_to_configure_checkpoint(self) -> None:
        """Expect that if a user configures a file context, it gets passed to the configure_checkpoint function."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()
        configure_file_data_context = Mock()
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type=context_type,
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        configure_checkpoint.assert_called_once_with(configure_file_data_context.return_value)

    def test_context_type_filesystem_requires_configure_file_data_context(self) -> None:
        """Expect that param context_type requires the configure_file_data_context parameter."""
        # arrange
        context_type: Literal["file"] = "file"
        configure_checkpoint = Mock()

        # act/assert
        with pytest.raises(ValueError, match="configure_file_data_context"):
            GXValidateCheckpointOperator(
                task_id="validate_checkpoint_success",
                configure_checkpoint=configure_checkpoint,
                context_type=context_type,
                configure_file_data_context=None,  # must be defined
            )

    def test_batch_parameters(self) -> None:
        """Expect that param batch_parameters is passed to Checkpoint.run. This
        also confirms that we run the Checkpoint returned by configure_checkpoint."""
        # arrange
        mock_checkpoint = Mock()
        configure_checkpoint = Mock()
        configure_checkpoint.return_value = mock_checkpoint
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }
        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint",
            configure_checkpoint=configure_checkpoint,
            batch_parameters=batch_parameters,
            context_type="ephemeral",
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        mock_checkpoint.run.assert_called_once_with(batch_parameters=batch_parameters)

    def test_configure_file_data_context_with_without_generator(self) -> None:
        """Expect that configure_file_data_context can just return a DataContext"""
        # arrange
        gx_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> FileDataContext:
            setup()
            return gx_context

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        setup.assert_called_once()
        gx_context.set_user_agent_str.assert_called_once_with(USER_AGENT_STR)
        configure_checkpoint.assert_called_once_with(gx_context)
        teardown.assert_not_called()

    def test_configure_file_data_context_with_generator(self) -> None:
        """Expect that configure_file_data_context can return a generator that yields a DataContext."""
        # arrange
        gx_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> Generator[FileDataContext, None, None]:
            setup()
            yield gx_context
            teardown()

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        validate_checkpoint.execute(context=context)

        # assert
        setup.assert_called_once()
        gx_context.set_user_agent_str.assert_called_once_with(USER_AGENT_STR)
        configure_checkpoint.assert_called_once_with(gx_context)
        teardown.assert_called_once()

    def test_configure_file_data_context_with_generator_no_yield(self) -> None:
        """Expect that configure_file_data_context errors if it does not yield a DataContext."""
        # arrange
        gx_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> Generator[FileDataContext, None, None]:
            setup()
            if False:
                # Force this to be a generator for the test
                yield gx_context
            teardown()

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        with pytest.raises(RuntimeError, match="did not yield"):
            validate_checkpoint.execute(context=context)

        # assert
        setup.assert_called_once()
        configure_checkpoint.assert_not_called()
        teardown.assert_called_once()

    def test_configure_file_data_context_with_generator_multiple_yields(self) -> None:
        """Expect that configure_file_data_context errors if it yields multiple times."""
        # arrange
        gx_context = Mock(spec=AbstractDataContext)
        setup = Mock()
        teardown = Mock()
        configure_checkpoint = Mock()

        def configure_file_data_context() -> Generator[FileDataContext, None, None]:
            setup()
            yield gx_context
            yield gx_context
            teardown()

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_success",
            configure_checkpoint=configure_checkpoint,
            context_type="file",
            configure_file_data_context=configure_file_data_context,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act
        with pytest.raises(RuntimeError, match="yielded more than once"):
            validate_checkpoint.execute(context=context)

        # assert
        setup.assert_called_once()
        teardown.assert_not_called()

    def test_validation_failure_raises_exception(self) -> None:
        """Expect that when validation fails, GXValidationFailed exception is raised."""

        # arrange
        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            # setup data source, asset, batch definition
            data_source = context.data_sources.add_pandas(name="test datasource")
            data_asset = data_source.add_dataframe_asset(name="test asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(name="test batch def")

            # setup expectation suite
            column_name = "col_A"
            suite = context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=[
                                "a",
                                "b",
                                "c",
                            ],  # different values to cause failure
                        ),
                    ],
                )
            )

            # setup validation definition
            validation_definition = context.validation_definitions.add(
                ValidationDefinition(
                    name="test validation definition",
                    data=batch_definition,
                    suite=suite,
                )
            )

            # setup checkpoint
            checkpoint = context.checkpoints.add(
                Checkpoint(
                    name="test checkpoint",
                    validation_definitions=[validation_definition],
                )
            )
            return checkpoint

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["x", "y", "z"]})  # values NOT in the expected set
        batch_parameters = {"dataframe": df}

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_failure",
            configure_checkpoint=configure_checkpoint,
            batch_parameters=batch_parameters,
        )
        context: Context = {"ti": Mock()}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_checkpoint.execute(context=context)

    def test_validation_failure_xcom_contains_result(self) -> None:
        """Expect that when validation fails and exception is raised, xcom still contains the result."""

        # arrange
        def configure_checkpoint(context: AbstractDataContext) -> Checkpoint:
            # setup data source, asset, batch definition
            data_source = context.data_sources.add_pandas(name="test datasource")
            data_asset = data_source.add_dataframe_asset(name="test asset")
            batch_definition = data_asset.add_batch_definition_whole_dataframe(name="test batch def")

            # setup expectation suite
            column_name = "col_A"
            suite = context.suites.add(
                ExpectationSuite(
                    name="test suite",
                    expectations=[
                        ExpectColumnValuesToBeInSet(
                            column=column_name,
                            value_set=[
                                "a",
                                "b",
                                "c",
                            ],  # different values to cause failure
                        ),
                    ],
                )
            )

            # setup validation definition
            validation_definition = context.validation_definitions.add(
                ValidationDefinition(
                    name="test validation definition",
                    data=batch_definition,
                    suite=suite,
                )
            )

            # setup checkpoint
            checkpoint = context.checkpoints.add(
                Checkpoint(
                    name="test checkpoint",
                    validation_definitions=[validation_definition],
                )
            )
            return checkpoint

        column_name = "col_A"
        df = pd.DataFrame({column_name: ["x", "y", "z"]})  # values NOT in the expected set
        batch_parameters = {"dataframe": df}

        validate_checkpoint = GXValidateCheckpointOperator(
            task_id="validate_checkpoint_failure",
            configure_checkpoint=configure_checkpoint,
            batch_parameters=batch_parameters,
        )
        mock_ti = Mock()
        context: Context = {"ti": mock_ti}  # type: ignore[typeddict-item]

        # act & assert
        with pytest.raises(GXValidationFailed):
            validate_checkpoint.execute(context=context)

        # Verify that xcom_push was called with the validation result
        mock_ti.xcom_push.assert_called_once()
        call_args = mock_ti.xcom_push.call_args
        assert call_args[1]["key"] == "return_value"
        result = call_args[1]["value"]
        assert result["success"] is False
