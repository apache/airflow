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
from typing import Literal
from unittest.mock import Mock

import great_expectations.expectations as gxe
import pytest

from airflow.providers.greatexpectations.common.gx_context_actions import (
    run_validation_definition,
)

pytestmark = pytest.mark.unit


class TestRunValidationDefinition:
    def test_validation_is_added_or_updated(self, mock_gx: Mock) -> None:
        # arrange
        task_id = "test_run_validation_definition"
        mock_context = Mock()
        validation_definition_factory = mock_context.validation_definitions
        validation_definition = mock_gx.ValidationDefinition.return_value
        batch_definition = Mock()
        expect = Mock()

        # act
        run_validation_definition(
            task_id=task_id,
            expect=expect,
            batch_definition=batch_definition,
            batch_parameters={},
            result_format=None,
            gx_context=mock_context,
        )

        # assert
        mock_gx.ValidationDefinition.assert_called_once_with(
            name=task_id, suite=expect, data=batch_definition
        )
        validation_definition_factory.add_or_update.assert_called_once_with(
            validation=validation_definition
        )

    @pytest.mark.parametrize(
        "result_format", ["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"]
    )
    def test_validation_is_run_with_result_format(
        self,
        mock_gx: Mock,
        result_format: Literal["BOOLEAN_ONLY", "BASIC", "SUMMARY", "COMPLETE"],
    ) -> None:
        # arrange
        task_id = "test_run_validation_definition"
        mock_context = Mock()
        validation_definition_factory = mock_context.validation_definitions
        validation_definition = validation_definition_factory.add_or_update.return_value
        batch_definition = Mock()
        expect = Mock()
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }
        # act
        run_validation_definition(
            task_id=task_id,
            expect=expect,
            batch_definition=batch_definition,
            batch_parameters=batch_parameters,
            result_format=result_format,
            gx_context=mock_context,
        )

        # assert
        validation_definition.run.assert_called_once_with(
            batch_parameters=batch_parameters, result_format=result_format
        )

    def test_null_result_format_is_not_passed_through(self, mock_gx: Mock) -> None:
        # arrange
        task_id = "test_run_validation_definition"
        mock_context = Mock()
        validation_definition_factory = mock_context.validation_definitions
        validation_definition = validation_definition_factory.add_or_update.return_value
        batch_definition = Mock()
        expect = Mock()
        batch_parameters = {
            "year": "2024",
            "month": "01",
            "day": "01",
        }
        # act
        run_validation_definition(
            task_id=task_id,
            expect=expect,
            batch_definition=batch_definition,
            batch_parameters=batch_parameters,
            result_format=None,
            gx_context=mock_context,
        )

        # assert
        validation_definition.run.assert_called_once_with(
            batch_parameters=batch_parameters
        )

    def test_expectation_is_transformed_to_suite(self, mock_gx: Mock) -> None:
        # arrange
        task_id = "test_run_validation_definition"
        mock_context = Mock()
        batch_definition = Mock()
        expect = gxe.ExpectColumnValuesToBeInSet(
            column="col A",
            value_set=["a", "b", "c", "d", "e"],  # type: ignore[arg-type]
        )
        expected_suite = mock_gx.ExpectationSuite.return_value

        # act
        run_validation_definition(
            task_id=task_id,
            expect=expect,
            batch_definition=batch_definition,
            batch_parameters={},
            result_format=None,
            gx_context=mock_context,
        )

        # assert
        mock_gx.ValidationDefinition.assert_called_once_with(
            name=task_id, suite=expected_suite, data=batch_definition
        )
        mock_gx.ExpectationSuite.assert_called_once_with(
            name=task_id, expectations=[expect]
        )

    def test_result(self, mock_gx: Mock) -> None:
        # arrange
        task_id = "test_run_validation_definition"
        mock_context = Mock()
        batch_definition = Mock()
        expect = Mock()
        validation_definition_factory = mock_context.validation_definitions
        validation_definition = validation_definition_factory.add_or_update.return_value

        # act
        result = run_validation_definition(
            task_id=task_id,
            expect=expect,
            batch_definition=batch_definition,
            batch_parameters={},
            result_format=None,
            gx_context=mock_context,
        )

        # assert
        assert result is validation_definition.run.return_value 