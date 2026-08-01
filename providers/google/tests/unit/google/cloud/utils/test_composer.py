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

from datetime import datetime, timedelta, timezone

import pytest

from airflow.providers.google.cloud.utils.composer import (
    check_dag_run_states_in_window,
    composer_dag_run_date_field,
    is_in_execution_window,
    normalize_to_utc,
    parse_composer_airflow_datetime,
)

WINDOW_START = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
WINDOW_END = datetime(2024, 1, 2, 0, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize(
    ("composer_airflow_version", "expected"),
    [
        (2, "execution_date"),
        (3, "logical_date"),
        (4, "logical_date"),
    ],
)
def test_composer_dag_run_date_field(composer_airflow_version, expected):
    assert composer_dag_run_date_field(composer_airflow_version) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(
            datetime(2024, 1, 1, 12, 0),
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            id="naive-treated-as-utc",
        ),
        pytest.param(
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone(timedelta(hours=2))),
            datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            id="aware-converted-to-utc",
        ),
    ],
)
def test_normalize_to_utc(value, expected):
    assert normalize_to_utc(value) == expected


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, None, id="none"),
        pytest.param("", None, id="empty-string"),
        pytest.param(
            "2024-01-01T12:00:00+02:00",
            datetime(2024, 1, 1, 10, 0, tzinfo=timezone.utc),
            id="offset-string",
        ),
        pytest.param(
            "2024-01-01T12:00:00",
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            id="naive-string-treated-as-utc",
        ),
        pytest.param(
            datetime(2024, 1, 1, 12, 0),
            datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc),
            id="datetime-passthrough-normalized",
        ),
    ],
)
def test_parse_composer_airflow_datetime(value, expected):
    assert parse_composer_airflow_datetime(value) == expected


@pytest.mark.parametrize(
    ("run_date", "expected"),
    [
        pytest.param(WINDOW_START, True, id="start-inclusive"),
        pytest.param(WINDOW_END, False, id="end-exclusive"),
        pytest.param(datetime(2024, 1, 1, 12, 0, tzinfo=timezone.utc), True, id="inside"),
        pytest.param(datetime(2023, 12, 31, 23, 59, tzinfo=timezone.utc), False, id="before"),
        pytest.param(datetime(2024, 1, 1, 12, 0), True, id="naive-inside-treated-as-utc"),
        pytest.param(
            datetime(2024, 1, 1, 14, 0, tzinfo=timezone(timedelta(hours=2))),
            True,
            id="aware-non-utc-inside",
        ),
    ],
)
def test_is_in_execution_window(run_date, expected):
    assert is_in_execution_window(run_date, WINDOW_START, WINDOW_END) is expected


def _dag_run(date_value, state="success", date_field="logical_date"):
    return {date_field: date_value, "state": state}


class TestCheckDagRunStatesInWindow:
    def _check(self, dag_runs, *, allowed_states=("success",), composer_airflow_version=3):
        return check_dag_run_states_in_window(
            dag_runs,
            start_date=WINDOW_START,
            end_date=WINDOW_END,
            allowed_states=allowed_states,
            composer_airflow_version=composer_airflow_version,
        )

    def test_no_runs_yields_false(self):
        assert self._check([]) is False

    def test_only_out_of_window_runs_yields_false(self):
        runs = [_dag_run("2023-12-01T00:00:00+00:00"), _dag_run("2024-02-01T00:00:00+00:00")]
        assert self._check(runs) is False

    def test_in_window_allowed_run_yields_true(self):
        assert self._check([_dag_run("2024-01-01T06:00:00+00:00")]) is True

    def test_in_window_disallowed_run_yields_false(self):
        runs = [
            _dag_run("2024-01-01T06:00:00+00:00"),
            _dag_run("2024-01-01T07:00:00+00:00", state="failed"),
        ]
        assert self._check(runs) is False

    def test_null_date_runs_are_skipped(self):
        runs = [_dag_run(None), _dag_run("2024-01-01T06:00:00+00:00")]
        assert self._check(runs) is True

    def test_only_null_date_runs_yields_false(self):
        assert self._check([_dag_run(None)]) is False

    @pytest.mark.parametrize("allowed_states", [None, []], ids=["none", "empty-list"])
    def test_missing_allowed_states_defaults_to_success(self, allowed_states):
        success = [_dag_run("2024-01-01T06:00:00+00:00", state="success")]
        failed = [_dag_run("2024-01-01T06:00:00+00:00", state="failed")]
        assert self._check(success, allowed_states=allowed_states) is True
        assert self._check(failed, allowed_states=allowed_states) is False

    def test_airflow_2_reads_execution_date_field(self):
        runs = [_dag_run("2024-01-01T06:00:00+00:00", date_field="execution_date")]
        assert self._check(runs, composer_airflow_version=2) is True
        # The same payload under version 3 has no logical_date -> skipped.
        assert self._check(runs, composer_airflow_version=3) is False
