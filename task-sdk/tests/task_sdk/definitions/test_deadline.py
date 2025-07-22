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

from datetime import datetime, timedelta
from unittest import mock

import pytest

from airflow.models.deadline import ReferenceModels
from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference

UNIMPORTABLE_DOT_PATH = "valid.but.nonexistent.path"

DAG_ID = "dag_id_1"
RUN_ID = 1
DEFAULT_DATE = datetime(2025, 6, 26)

TEST_CALLBACK_PATH = f"{__name__}.test_callback_for_deadline"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}

REFERENCE_TYPES = [
    pytest.param(DeadlineReference.DAGRUN_LOGICAL_DATE, id="logical_date"),
    pytest.param(DeadlineReference.DAGRUN_QUEUED_AT, id="queued_at"),
    pytest.param(DeadlineReference.FIXED_DATETIME(DEFAULT_DATE), id="fixed_deadline"),
]


def test_callback_for_deadline():
    """Used in a number of tests to confirm that Deadlines and DeadlineAlerts function correctly."""
    pass


class TestDeadlineAlert:
    @pytest.mark.parametrize(
        "callback_value, expected_path",
        [
            pytest.param(test_callback_for_deadline, TEST_CALLBACK_PATH, id="valid_callable"),
            pytest.param(TEST_CALLBACK_PATH, TEST_CALLBACK_PATH, id="valid_path_string"),
            pytest.param(lambda x: x, None, id="lambda_function"),
            pytest.param(TEST_CALLBACK_PATH + "  ", TEST_CALLBACK_PATH, id="path_with_whitespace"),
            pytest.param(UNIMPORTABLE_DOT_PATH, UNIMPORTABLE_DOT_PATH, id="valid_format_not_importable"),
        ],
    )
    def test_get_callback_path_happy_cases(self, callback_value, expected_path):
        path = DeadlineAlert.get_callback_path(callback_value)
        if expected_path is None:
            assert path.endswith("<lambda>")
        else:
            assert path == expected_path

    @pytest.mark.parametrize(
        "callback_value, error_type",
        [
            pytest.param(42, ImportError, id="not_a_string"),
            pytest.param("", ImportError, id="empty_string"),
            pytest.param("os.path", AttributeError, id="non_callable_module"),
        ],
    )
    def test_get_callback_path_error_cases(self, callback_value, error_type):
        expected_message = ""
        if error_type is ImportError:
            expected_message = "doesn't look like a valid dot path."
        elif error_type is AttributeError:
            expected_message = "is not callable."

        with pytest.raises(error_type, match=expected_message):
            DeadlineAlert.get_callback_path(callback_value)

    @pytest.mark.parametrize(
        "test_alert, should_equal",
        [
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=TEST_CALLBACK_PATH,
                    callback_kwargs=TEST_CALLBACK_KWARGS,
                ),
                True,
                id="same_alert",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
                    interval=timedelta(hours=1),
                    callback=TEST_CALLBACK_PATH,
                    callback_kwargs=TEST_CALLBACK_KWARGS,
                ),
                False,
                id="different_reference",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=2),
                    callback=TEST_CALLBACK_PATH,
                    callback_kwargs=TEST_CALLBACK_KWARGS,
                ),
                False,
                id="different_interval",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback="other.callback",
                    callback_kwargs=TEST_CALLBACK_KWARGS,
                ),
                False,
                id="different_callback",
            ),
            pytest.param(
                DeadlineAlert(
                    reference=DeadlineReference.DAGRUN_QUEUED_AT,
                    interval=timedelta(hours=1),
                    callback=TEST_CALLBACK_PATH,
                    callback_kwargs={"arg2": "value2"},
                ),
                False,
                id="different_kwargs",
            ),
            pytest.param("not a DeadlineAlert", False, id="non_deadline_alert"),
        ],
    )
    def test_deadline_alert_equality(self, test_alert, should_equal):
        base_alert = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=timedelta(hours=1),
            callback=TEST_CALLBACK_PATH,
            callback_kwargs=TEST_CALLBACK_KWARGS,
        )

        assert (base_alert == test_alert) == should_equal

    def test_deadline_alert_hash(self):
        std_interval = timedelta(hours=1)
        std_callback = TEST_CALLBACK_PATH
        std_kwargs = TEST_CALLBACK_KWARGS

        alert1 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=std_callback,
            callback_kwargs=std_kwargs,
        )
        alert2 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=std_callback,
            callback_kwargs=std_kwargs,
        )

        assert hash(alert1) == hash(alert1)
        assert hash(alert1) == hash(alert2)

    def test_deadline_alert_in_set(self):
        std_interval = timedelta(hours=1)
        std_callback = TEST_CALLBACK_PATH
        std_kwargs = TEST_CALLBACK_KWARGS

        alert1 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=std_callback,
            callback_kwargs=std_kwargs,
        )
        alert2 = DeadlineAlert(
            reference=DeadlineReference.DAGRUN_QUEUED_AT,
            interval=std_interval,
            callback=std_callback,
            callback_kwargs=std_kwargs,
        )

        alert_set = {alert1, alert2}
        assert len(alert_set) == 1


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
