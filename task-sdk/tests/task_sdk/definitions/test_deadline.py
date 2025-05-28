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
from task_sdk.definitions.test_dag import DEFAULT_DATE

from airflow.models.deadline import ReferenceModels
from airflow.sdk.definitions.deadline import DeadlineReference

DAG_ID = "dag_id_1"

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
]


class TestDeadlineReference:
    @pytest.mark.parametrize("reference", REFERENCE_TYPES)
    def test_deadline_evaluate_with(self, reference):
        """Test that all deadline types evaluate correctly with their required conditions."""
        conditions = {
            "dag_id": DAG_ID,
            "unexpected": "param",  # Add an unexpected parameter.
            "extra": "kwarg",  # Add another unexpected parameter.
        }

        with mock.patch.object(reference, "_evaluate_with") as mock_evaluate:
            mock_evaluate.return_value = DEFAULT_DATE

            if reference.required_kwargs:
                result = reference.evaluate_with(**conditions)
            else:
                result = reference.evaluate_with()

            # Verify only expected kwargs are passed through.
            expected_kwargs = {k: conditions[k] for k in reference.required_kwargs if k in conditions}
            mock_evaluate.assert_called_once_with(**expected_kwargs)
            assert result == DEFAULT_DATE

    @pytest.mark.parametrize("reference", REFERENCE_TYPES)
    def test_deadline_missing_required_kwargs(self, reference):
        """Test that deadlines raise appropriate errors for missing required parameters."""
        if reference.required_kwargs:
            with pytest.raises(ValueError) as e:
                reference.evaluate_with()
            expected_error = f"{reference.__class__.__name__} is missing required parameters: dag_id"
            assert expected_error in str(e)
        else:
            # Let the lack of an exception here effectively assert that no exception is raised.
            reference.evaluate_with()

    def test_deadline_reference_creation(self):
        """Test that DeadlineReference provides consistent interface and types."""
        fixed_reference = DeadlineReference.FIXED_DATETIME(DEFAULT_DATE)
        assert isinstance(fixed_reference, ReferenceModels.FixedDatetimeDeadline)
        assert fixed_reference._datetime == DEFAULT_DATE

        logical_date_reference = DeadlineReference.DAGRUN_LOGICAL_DATE
        assert isinstance(logical_date_reference, ReferenceModels.DagRunLogicalDateDeadline)

        queued_reference = DeadlineReference.DAGRUN_QUEUED_AT
        assert isinstance(queued_reference, ReferenceModels.DagRunQueuedAtDeadline)
