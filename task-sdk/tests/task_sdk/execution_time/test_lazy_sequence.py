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

from unittest.mock import ANY, Mock, call

import pytest

from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetXComCount,
    GetXComSequenceItem,
    XComCountResponse,
    XComResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence


@pytest.fixture
def mock_operator():
    return Mock(spec=["dag_id", "task_id"], dag_id="dag", task_id="task")


@pytest.fixture
def mock_xcom_arg(mock_operator):
    return Mock(spec=["operator", "key"], operator=mock_operator, key="return_value")


@pytest.fixture
def mock_ti():
    return Mock(spec=["run_id"], run_id="run")


@pytest.fixture
def lazy_sequence(mock_xcom_arg, mock_ti):
    return LazyXComSequence(mock_xcom_arg, mock_ti)


def test_len(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.get_message.return_value = XComCountResponse(len=3)
    assert len(lazy_sequence) == 3
    assert mock_supervisor_comms.send_request.mock_calls == [
        call(log=ANY, msg=GetXComCount(key="return_value", dag_id="dag", task_id="task", run_id="run")),
    ]


def test_iter(mock_supervisor_comms, lazy_sequence):
    it = iter(lazy_sequence)

    mock_supervisor_comms.get_message.side_effect = [
        XComResult(key="return_value", value="f"),
        ErrorResponse(error=ErrorType.XCOM_NOT_FOUND, detail={"oops": "sorry!"}),
    ]
    assert list(it) == ["f"]
    assert mock_supervisor_comms.send_request.mock_calls == [
        call(
            log=ANY,
            msg=GetXComSequenceItem(
                key="return_value",
                dag_id="dag",
                task_id="task",
                run_id="run",
                offset=0,
            ),
        ),
        call(
            log=ANY,
            msg=GetXComSequenceItem(
                key="return_value",
                dag_id="dag",
                task_id="task",
                run_id="run",
                offset=1,
            ),
        ),
    ]


def test_getitem_index(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value="f")
    assert lazy_sequence[4] == "f"
    assert mock_supervisor_comms.send_request.mock_calls == [
        call(
            log=ANY,
            msg=GetXComSequenceItem(
                key="return_value",
                dag_id="dag",
                task_id="task",
                run_id="run",
                offset=4,
            ),
        ),
    ]


def test_getitem_indexerror(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.get_message.return_value = ErrorResponse(
        error=ErrorType.XCOM_NOT_FOUND,
        detail={"oops": "sorry!"},
    )
    with pytest.raises(IndexError) as ctx:
        lazy_sequence[4]
    assert ctx.value.args == (4,)
    assert mock_supervisor_comms.send_request.mock_calls == [
        call(
            log=ANY,
            msg=GetXComSequenceItem(
                key="return_value",
                dag_id="dag",
                task_id="task",
                run_id="run",
                offset=4,
            ),
        ),
    ]
