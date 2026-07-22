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

from unittest.mock import Mock, call

import pytest

import airflow
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import (
    ErrorResponse,
    GetXComByKeys,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    XComCountResponse,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence, XComIterable
from airflow.sdk.execution_time.xcom import resolve_xcom_backend

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def mock_operator():
    return Mock(spec=["dag_id", "task_id"], dag_id="dag", task_id="task")


@pytest.fixture
def mock_xcom_arg(mock_operator):
    return Mock(spec=["operator", "key"], operator=mock_operator, key=BaseXCom.XCOM_RETURN_KEY)


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
    mock_supervisor_comms.send.return_value = XComCountResponse(len=3)
    assert len(lazy_sequence) == 3
    mock_supervisor_comms.send.assert_called_once_with(
        msg=GetXComCount(key=BaseXCom.XCOM_RETURN_KEY, dag_id="dag", task_id="task", run_id="run"),
    )


def test_iter(mock_supervisor_comms, lazy_sequence):
    it = iter(lazy_sequence)

    mock_supervisor_comms.send.side_effect = [
        XComSequenceIndexResult(root="f"),
        ErrorResponse(error=ErrorType.XCOM_NOT_FOUND, detail={"oops": "sorry!"}),
    ]
    assert list(it) == ["f"]
    mock_supervisor_comms.send.assert_has_calls(
        [
            call(
                msg=GetXComSequenceItem(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="dag",
                    task_id="task",
                    run_id="run",
                    offset=0,
                ),
            ),
            call(
                msg=GetXComSequenceItem(
                    key=BaseXCom.XCOM_RETURN_KEY,
                    dag_id="dag",
                    task_id="task",
                    run_id="run",
                    offset=1,
                ),
            ),
        ]
    )


def test_getitem_index(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="f")
    assert lazy_sequence[4] == "f"
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


@conf_vars({("core", "xcom_backend"): "task_sdk.execution_time.test_lazy_sequence.CustomXCom"})
def test_getitem_calls_correct_deserialise(monkeypatch, mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="some-value")

    xcom = resolve_xcom_backend()
    assert xcom.__name__ == "CustomXCom"
    monkeypatch.setattr(airflow.sdk.execution_time.xcom, "XCom", xcom)

    assert lazy_sequence[4] == "Made with CustomXCom: some-value"
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


def test_getitem_indexerror(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = ErrorResponse(
        error=ErrorType.XCOM_NOT_FOUND,
        detail={"oops": "sorry!"},
    )
    with pytest.raises(IndexError) as ctx:
        lazy_sequence[4]
    assert ctx.value.args == (4,)
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceItem(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            offset=4,
        ),
    )


def test_getitem_slice(mock_supervisor_comms, lazy_sequence):
    mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=[6, 4, 1])
    assert lazy_sequence[:5] == [6, 4, 1]
    mock_supervisor_comms.send.assert_called_once_with(
        GetXComSequenceSlice(
            key=BaseXCom.XCOM_RETURN_KEY,
            dag_id="dag",
            task_id="task",
            run_id="run",
            start=None,
            stop=5,
            step=None,
        ),
    )


class TestXComIterable:
    @pytest.fixture
    def iterable(self):
        return XComIterable(task_id="task", dag_id="dag", run_id="run", length=3)

    def test_iter_sends_single_batch_request(self, mock_supervisor_comms, iterable):
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "b", "c"])
        assert list(iterable) == ["a", "b", "c"]
        mock_supervisor_comms.send.assert_called_once_with(
            GetXComByKeys(
                keys=["return_value_0", "return_value_1", "return_value_2"],
                dag_id="dag",
                run_id="run",
                task_id="task",
                map_index=-1,
            )
        )

    def test_iter_empty_length_skips_supervisor_call(self, mock_supervisor_comms):
        iterable = XComIterable(task_id="task", dag_id="dag", run_id="run", length=0)
        assert list(iterable) == []
        mock_supervisor_comms.send.assert_not_called()

    def test_iter_is_lazy(self, mock_supervisor_comms, iterable):
        """Deserialization happens on demand - breaking early skips remaining items."""
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "b", "c"])
        it = iter(iterable)
        assert next(it) == "a"
        # Only one send was made for the whole batch, but only one item consumed.
        mock_supervisor_comms.send.assert_called_once()

    def test_iter_uses_map_index(self, mock_supervisor_comms):
        iterable = XComIterable(task_id="task", dag_id="dag", run_id="run", length=2, map_index=5)
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["x", "y"])
        list(iterable)
        mock_supervisor_comms.send.assert_called_once_with(
            GetXComByKeys(
                keys=["return_value_0", "return_value_1"],
                dag_id="dag",
                run_id="run",
                task_id="task",
                map_index=5,
            )
        )

    def test_iter_unexpected_response_raises(self, mock_supervisor_comms, iterable):
        mock_supervisor_comms.send.return_value = XComSequenceIndexResult(root="oops")
        with pytest.raises(TypeError, match="Got unexpected response to GetXComByKeys"):
            list(iterable)

    def test_getitem_integer(self, mock_supervisor_comms, iterable):
        """Single-index access still uses XCom.get_one (one-item fetch)."""
        from unittest.mock import patch

        with patch("airflow.sdk.execution_time.lazy_sequence.XCom") as mock_xcom:
            mock_xcom.get_one.return_value = "val"
            assert iterable[1] == "val"
            mock_xcom.get_one.assert_called_once_with(
                key="return_value_1",
                dag_id="dag",
                task_id="task",
                run_id="run",
                map_index=None,
            )
        mock_supervisor_comms.send.assert_not_called()

    def test_getitem_integer_out_of_range(self, mock_supervisor_comms, iterable):
        with pytest.raises(IndexError):
            iterable[10]
        mock_supervisor_comms.send.assert_not_called()

    def test_getitem_slice_sends_batch(self, mock_supervisor_comms, iterable):
        mock_supervisor_comms.send.return_value = XComSequenceSliceResult(root=["a", "c"])
        assert iterable[::2] == ["a", "c"]
        mock_supervisor_comms.send.assert_called_once_with(
            GetXComByKeys(
                keys=["return_value_0", "return_value_2"],
                dag_id="dag",
                run_id="run",
                task_id="task",
                map_index=-1,
            )
        )

    def test_getitem_empty_slice_skips_supervisor_call(self, mock_supervisor_comms, iterable):
        assert iterable[5:2] == []
        mock_supervisor_comms.send.assert_not_called()

    def test_len(self, iterable):
        assert len(iterable) == 3
