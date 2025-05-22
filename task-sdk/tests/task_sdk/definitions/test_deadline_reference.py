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

import logging
from unittest import mock

import pytest

from airflow.sdk.definitions.deadline_reference import (
    DagRunLogicalDateDeadline,
    DagRunQueuedAtDeadline,
    DeadlineReference,
    FixedDatetimeDeadline,
)
from task_sdk.definitions.test_dag import DEFAULT_DATE

DAG_ID = "dag_id_1"


class TestDeadlineReference:
    def test_deadline_reference_creation(self):
        """Test that DeadlineReference provides consistent interface and types."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)
        assert isinstance(fixed_reference, FixedDatetimeDeadline)
        assert fixed_reference._datetime == DEFAULT_DATE

        logical_date_reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        assert isinstance(logical_date_reference, DagRunLogicalDateDeadline)

        queued_reference = DeadlineReference.DAGRUN_QUEUED_AT
        assert isinstance(queued_reference, DagRunQueuedAtDeadline)

    @pytest.mark.parametrize(
        "reference",
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
        ],
    )
    def test_deadline_evaluate_with(self, reference):
        """Test that all deadline types evaluate correctly with their required conditions."""
        conditions = {"dag_id": DAG_ID}

        if reference.requires_conditions:
            with mock.patch.object(reference, "evaluate_with") as mock_evaluate:
                mock_evaluate.return_value = DEFAULT_DATE

                result = reference.evaluate_with(**conditions)

                mock_evaluate.assert_called_once_with(**conditions)
        else:
            result = reference.evaluate_with()

        assert result == DEFAULT_DATE

    @pytest.mark.parametrize(
        "reference",
        [
            pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
            pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
            pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
        ],
    )
    def test_deadline_evaluate_behavior(self, reference):
        """
        Test evaluate() behavior for all deadline types.

        Verifies:
        1. Fixed deadlines delegate evaluate() to evaluate_with()
        2. Calculated deadlines raise appropriate error for evaluate()
        3. Fixed deadlines return correct value
        """
        if reference.requires_conditions:
            with pytest.raises(AttributeError, match="requires additional conditions"):
                reference.evaluate()
        else:
            # Verify that evaluate() calls evaluate_with().
            with mock.patch.object(reference, "evaluate_with") as mock_evaluate_with:
                mock_evaluate_with.return_value = DEFAULT_DATE
                result = reference.evaluate()
                mock_evaluate_with.assert_called_once_with()
                assert result == DEFAULT_DATE

            # Test actual evaluation.
            assert reference.evaluate() == reference.evaluate_with() == DEFAULT_DATE

    def test_fixed_deadline_ignores_unexpected_kwargs(self, caplog):
        """Test that fixed deadlines ignore unexpected kwargs in evaluate_with and log appropriately."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)

        with caplog.at_level(logging.DEBUG):
            # Should not raise an error and should return the same value
            with_kwargs = fixed_reference.evaluate_with(unexpected="kwargs")
            without_kwargs = fixed_reference.evaluate()

            assert with_kwargs == without_kwargs == DEFAULT_DATE
            assert "Fixed Datetime Deadlines do not accept conditions, ignoring kwargs" in caplog.text
