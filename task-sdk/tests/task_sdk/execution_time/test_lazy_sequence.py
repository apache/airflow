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

import airflow
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetXComCount,
    GetXComSequenceItem,
    XComCountResponse,
    XComResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.xcom import resolve_xcom_backend

from tests_common.test_utils.config import conf_vars


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


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom):
        return f"Made with CustomXCom: {xcom.value}"


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


@conf_vars({("core", "xcom_backend"): "task_sdk.execution_time.test_lazy_sequence.CustomXCom"})
def test_getitem_calls_correct_deserialise(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.get_message.return_value = XComResult(key="return_value", value="some-value")

    xcom = resolve_xcom_backend()
    assert xcom.__name__ == "CustomXCom"
    airflow.sdk.execution_time.xcom.XCom = xcom

    assert lazy_sequence[4] == "Made with CustomXCom: some-value"
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
