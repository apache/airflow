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

import pytest

from airflow.providers.greatexpectations.exceptions import (
    GXValidationFailed,
    extract_validation_failure_context,
)


class TestExtractValidationFailureContext:
    """Test the extract_validation_failure_context function."""

    @pytest.fixture
    def checkpoint_result_data(self):
        """Sample CheckpointResult data structure."""
        return {
            "success": False,
            "statistics": {
                "evaluated_validations": 1,
                "success_percent": 0.0,
                "successful_validations": 0,
                "unsuccessful_validations": 1,
            },
            "validation_results": [
                {
                    "success": False,
                    "statistics": {
                        "evaluated_expectations": 6,
                        "successful_expectations": 1,
                        "unsuccessful_expectations": 5,
                        "success_percent": 16.666666666666664,
                    },
                    "expectations": [
                        {
                            "expectation_type": "expect_column_values_to_not_be_null",
                            "success": True,
                            "kwargs": {
                                "batch_id": "test ds-test asset",
                                "column": "name",
                            },
                        },
                        {
                            "expectation_type": "expect_column_values_to_be_between",
                            "success": False,
                            "kwargs": {
                                "batch_id": "test ds-test asset",
                                "column": "name",
                                "min_value": 1.0,
                                "max_value": 2.0,
                            },
                        },
                        {
                            "expectation_type": "expect_column_max_to_be_between",
                            "success": False,
                            "kwargs": {
                                "batch_id": "test ds-test asset",
                                "column": "name",
                                "min_value": 1.0,
                                "max_value": 2.0,
                            },
                        },
                        {
                            "expectation_type": "expect_column_min_to_be_between",
                            "success": False,
                            "kwargs": {
                                "batch_id": "test ds-test asset",
                                "column": "name",
                                "min_value": 3.0,
                                "max_value": 4.0,
                            },
                        },
                        {
                            "expectation_type": "expect_column_values_to_be_in_set",
                            "success": False,
                            "kwargs": {
                                "batch_id": "test ds-test asset",
                                "column": "name",
                                "value_set": [6, 7, 8],
                            },
                        },
                        {
                            "expectation_type": "expect_table_row_count_to_equal",
                            "success": False,
                            "kwargs": {"batch_id": "test ds-test asset", "value": 10},
                        },
                    ],
                }
            ],
        }

    @pytest.fixture
    def expectation_suite_result_data(self):
        """Sample ExpectationSuiteValidationResult data structure."""
        return {
            "success": False,
            "statistics": {
                "evaluated_expectations": 6,
                "successful_expectations": 1,
                "unsuccessful_expectations": 5,
                "success_percent": 16.666666666666664,
            },
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": True,
                    "kwargs": {"batch_id": "test ds-test asset", "column": "name"},
                },
                {
                    "expectation_type": "expect_column_values_to_be_between",
                    "success": False,
                    "kwargs": {
                        "batch_id": "test ds-test asset",
                        "column": "name",
                        "min_value": 1.0,
                        "max_value": 2.0,
                    },
                },
                {
                    "expectation_type": "expect_column_max_to_be_between",
                    "success": False,
                    "kwargs": {
                        "batch_id": "test ds-test asset",
                        "column": "name",
                        "min_value": 1.0,
                        "max_value": 2.0,
                    },
                },
                {
                    "expectation_type": "expect_column_min_to_be_between",
                    "success": False,
                    "kwargs": {
                        "batch_id": "test ds-test asset",
                        "column": "name",
                        "min_value": 3.0,
                        "max_value": 4.0,
                    },
                },
                {
                    "expectation_type": "expect_column_values_to_be_in_set",
                    "success": False,
                    "kwargs": {
                        "batch_id": "test ds-test asset",
                        "column": "name",
                        "value_set": [6, 7, 8],
                    },
                },
                {
                    "expectation_type": "expect_table_row_count_to_equal",
                    "success": False,
                    "kwargs": {"batch_id": "test ds-test asset", "value": 10},
                },
            ],
        }

    def test_extract_context_checkpoint_result(self, checkpoint_result_data):
        """Test context extraction from CheckpointResult format."""
        context = extract_validation_failure_context(checkpoint_result_data, "test_task")

        assert context["xcom_location"] == "Task 'test_task' -> XCom key 'return_value'"
        assert context["statistics"] == {
            "evaluated_validations": 1,
            "success_percent": 0.0,
            "successful_validations": 0,
            "unsuccessful_validations": 1,
        }
        # Should have 5 failed expectation types, sorted alphabetically
        expected_failed_types = [
            "expect_column_max_to_be_between",
            "expect_column_min_to_be_between",
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
            "expect_table_row_count_to_equal",
        ]
        assert context["failed_expectation_types"] == expected_failed_types

    def test_extract_context_expectation_suite_result(self, expectation_suite_result_data):
        """Test context extraction from ExpectationSuiteValidationResult format."""
        context = extract_validation_failure_context(expectation_suite_result_data, "test_task")

        assert context["xcom_location"] == "Task 'test_task' -> XCom key 'return_value'"
        assert context["statistics"] == {
            "evaluated_expectations": 6,
            "successful_expectations": 1,
            "unsuccessful_expectations": 5,
            "success_percent": 16.666666666666664,
        }
        # Should have 5 failed expectation types, sorted alphabetically
        expected_failed_types = [
            "expect_column_max_to_be_between",
            "expect_column_min_to_be_between",
            "expect_column_values_to_be_between",
            "expect_column_values_to_be_in_set",
            "expect_table_row_count_to_equal",
        ]
        assert context["failed_expectation_types"] == expected_failed_types

    def test_extract_context_max_10_expectation_types(self):
        """Test that only first 10 unique failed expectation types are returned."""
        expectations = []
        for i in range(15):
            expectations.append(
                {
                    "expectation_type": f"expect_type_{i}",
                    "success": False,
                }
            )

        result_data = {"success": False, "expectations": expectations}
        context = extract_validation_failure_context(result_data, "test_task")

        assert len(context["failed_expectation_types"]) == 10
        # Should be sorted alphabetically - first 10 in lexicographic order
        all_types = [f"expect_type_{i}" for i in range(15)]
        expected_types = sorted(all_types)[:10]
        assert context["failed_expectation_types"] == expected_types

    def test_extract_context_duplicate_expectation_types(self):
        """Test that duplicate expectation types are only included once."""
        expectations = [
            {"expectation_type": "expect_column_to_exist", "success": False},
            {"expectation_type": "expect_column_to_exist", "success": False},
            {"expectation_type": "expect_table_to_exist", "success": False},
            {"expectation_type": "expect_table_to_exist", "success": False},
        ]

        result_data = {"success": False, "expectations": expectations}
        context = extract_validation_failure_context(result_data, "test_task")

        assert len(context["failed_expectation_types"]) == 2
        assert set(context["failed_expectation_types"]) == {
            "expect_column_to_exist",
            "expect_table_to_exist",
        }


class TestGXValidationFailed:
    """Test the GXValidationFailed exception class."""

    @pytest.fixture
    def sample_result_dict(self):
        """Sample validation result dictionary."""
        return {
            "success": False,
            "statistics": {
                "evaluated_expectations": 3,
                "successful_expectations": 1,
                "unsuccessful_expectations": 2,
                "success_percent": 33.33,
            },
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "success": True},
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "success": False,
                },
                {
                    "expectation_type": "expect_column_values_to_be_unique",
                    "success": False,
                },
            ],
        }

    def test_exception_with_full_context(self, sample_result_dict):
        """Test exception creation with full validation context."""
        exc = GXValidationFailed(sample_result_dict, "test_task")

        assert exc.xcom_location == "Task 'test_task' -> XCom key 'return_value'"
        assert exc.statistics == {
            "evaluated_expectations": 3,
            "successful_expectations": 1,
            "unsuccessful_expectations": 2,
            "success_percent": 33.33,
        }
        assert set(exc.failed_expectation_types) == {
            "expect_column_values_to_not_be_null",
            "expect_column_values_to_be_unique",
        }

        # Test error message content
        error_msg = str(exc)
        assert "Great Expectations data validation failed." in error_msg
        assert "Task 'test_task' -> XCom key 'return_value'" in error_msg
        assert "evaluated_expectations: 3" in error_msg
