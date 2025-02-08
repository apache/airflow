#
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
from typing import Callable
from unittest import mock

import pytest
import structlog
from pytest_unordered import unordered

from airflow.exceptions import AirflowSkipException
from airflow.sdk.api.datamodels._generated import TerminalTIState
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.execution_time.comms import GetXCom, XComResult

log = structlog.get_logger()

RunTI = Callable[[DAG, str, int], TerminalTIState]


def test_xcom_map(run_ti: RunTI, mock_supervisor_comms):
    results = set()
    with DAG("test") as dag:

        @dag.task
        def push():
            return ["a", "b", "c"]

        @dag.task
        def pull(value):
            results.add(value)

        pull.expand_kwargs(push().map(lambda v: {"value": v * 2}))

    # The function passed to "map" is *NOT* a task.
    assert set(dag.task_dict) == {"push", "pull"}

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')

    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TerminalTIState.SUCCESS

    assert results == {"aa", "bb", "cc"}


def test_xcom_map_transform_to_none(run_ti: RunTI, mock_supervisor_comms):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return v

        pull.expand(value=push().map(c_to_none))

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')

    # Run "pull". This should automatically convert "c" to None.
    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TerminalTIState.SUCCESS

    assert results == {"a", "b", None}


def test_xcom_convert_to_kwargs_fails_task(run_ti: RunTI, mock_supervisor_comms, captured_logs):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        def c_to_none(v):
            if v == "c":
                return None
            return {"value": v}

        pull.expand_kwargs(push().map(c_to_none))

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')

    # The first two "pull" tis should succeed.
    for map_index in range(2):
        assert run_ti(dag, "pull", map_index) == TerminalTIState.SUCCESS

    # Clear captured logs from the above
    captured_logs[:] = []

    # But the third one fails because the map() result cannot be used as kwargs.
    assert run_ti(dag, "pull", 2) == TerminalTIState.FAILED

    assert captured_logs == unordered(
        [
            {
                "event": "Task failed with exception",
                "level": "error",
                "timestamp": mock.ANY,
                "exception": [
                    {
                        "exc_type": "ValueError",
                        "exc_value": "expand_kwargs() expects a list[dict], not list[None]",
                        "frames": mock.ANY,
                        "is_cause": False,
                        "syntax_error": None,
                    }
                ],
            },
        ]
    )


def test_xcom_map_error_fails_task(mock_supervisor_comms, run_ti, captured_logs):
    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            print(value)

        def does_not_work_with_c(v):
            if v == "c":
                raise RuntimeError("nope")
            return {"value": v * 2}

        pull.expand_kwargs(push().map(does_not_work_with_c))

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')
    # The third one (for "c") will fail.
    assert run_ti(dag, "pull", 2) == TerminalTIState.FAILED

    assert captured_logs == unordered(
        [
            {
                "event": "Task failed with exception",
                "level": "error",
                "timestamp": mock.ANY,
                "exception": [
                    {
                        "exc_type": "RuntimeError",
                        "exc_value": "nope",
                        "frames": mock.ANY,
                        "is_cause": False,
                        "syntax_error": None,
                    }
                ],
            },
        ]
    )


def test_xcom_map_nest(mock_supervisor_comms, run_ti):
    results = set()

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def pull(value):
            results.add(value)

        converted = push().map(lambda v: v * 2).map(lambda v: {"value": v})
        pull.expand_kwargs(converted)

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')

    # Now "pull" should apply the mapping functions in order.
    for map_index in range(3):
        assert run_ti(dag, "pull", map_index) == TerminalTIState.SUCCESS
    assert results == {"aa", "bb", "cc"}


def test_xcom_map_zip_nest(mock_supervisor_comms, run_ti):
    results = set()

    with DAG("test") as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c", "d"]

        @dag.task
        def push_numbers():
            return [1, 2, 3, 4]

        @dag.task
        def pull(value):
            results.add(value)

        doubled = push_numbers().map(lambda v: v * 2)
        combined = doubled.zip(push_letters())

        def convert_zipped(zipped):
            letter, number = zipped
            return letter * number

        pull.expand(value=combined.map(convert_zipped))

    def xcom_get():
        # TODO: Tidy this after #45927 is reopened and fixed properly
        last_request = mock_supervisor_comms.send_request.mock_calls[-1].kwargs["msg"]
        if not isinstance(last_request, GetXCom):
            return mock.DEFAULT
        if last_request.task_id == "push_letters":
            value = json.dumps(push_letters.function())
            return XComResult(key="return_value", value=value)
        if last_request.task_id == "push_numbers":
            value = json.dumps(push_numbers.function())
            return XComResult(key="return_value", value=value)
        return mock.DEFAULT

    mock_supervisor_comms.get_message.side_effect = xcom_get

    # Run "pull".
    for map_index in range(4):
        assert run_ti(dag, "pull", map_index) == TerminalTIState.SUCCESS

    assert results == {"aa", "bbbb", "cccccc", "dddddddd"}


def test_xcom_map_raise_to_skip(run_ti, mock_supervisor_comms):
    result = []

    with DAG("test") as dag:

        @dag.task()
        def push():
            return ["a", "b", "c"]

        @dag.task()
        def forward(value):
            result.append(value)

        def skip_c(v):
            if v == "c":
                raise AirflowSkipException()
            return {"value": v}

        forward.expand_kwargs(push().map(skip_c))

    # Mock xcom result from push task
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value='["a", "b", "c"]')

    # Run "forward". This should automatically skip "c".
    states = [run_ti(dag, "forward", map_index) for map_index in range(3)]

    assert states == [TerminalTIState.SUCCESS, TerminalTIState.SUCCESS, TerminalTIState.SKIPPED]

    assert result == ["a", "b"]


def test_xcom_concat(run_ti, mock_supervisor_comms):
    from airflow.sdk.definitions.xcom_arg import _ConcatResult

    agg_results = set()
    all_results = None

    with DAG("test") as dag:

        @dag.task
        def push_letters():
            return ["a", "b", "c"]

        @dag.task
        def push_numbers():
            return [1, 2]

        @dag.task
        def pull_one(value):
            agg_results.add(value)

        @dag.task
        def pull_all(value):
            assert isinstance(value, _ConcatResult)
            assert value[0] == "a"
            assert value[1] == "b"
            assert value[2] == "c"
            assert value[3] == 1
            assert value[4] == 2
            with pytest.raises(IndexError):
                value[5]
            assert value[-5] == "a"
            assert value[-4] == "b"
            assert value[-3] == "c"
            assert value[-2] == 1
            assert value[-1] == 2
            with pytest.raises(IndexError):
                value[-6]
            nonlocal all_results
            all_results = list(value)

        pushed_values = push_letters().concat(push_numbers())

        pull_one.expand(value=pushed_values)
        pull_all(pushed_values)

    def xcom_get():
        # TODO: Tidy this after #45927 is reopened and fixed properly
        last_request = mock_supervisor_comms.send_request.mock_calls[-1].kwargs["msg"]
        if not isinstance(last_request, GetXCom):
            return mock.DEFAULT
        if last_request.task_id == "push_letters":
            value = json.dumps(push_letters.function())
            return XComResult(key="return_value", value=value)
        if last_request.task_id == "push_numbers":
            value = json.dumps(push_numbers.function())
            return XComResult(key="return_value", value=value)
        return mock.DEFAULT

    mock_supervisor_comms.get_message.side_effect = xcom_get

    # Run "pull_one" and "pull_all".
    assert run_ti(dag, "pull_all", None) == TerminalTIState.SUCCESS
    assert all_results == ["a", "b", "c", 1, 2]

    states = [run_ti(dag, "pull_one", map_index) for map_index in range(5)]
    assert states == [TerminalTIState.SUCCESS] * 5
    assert agg_results == {"a", "b", "c", 1, 2}
