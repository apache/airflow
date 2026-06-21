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

from airflow.api_fastapi.common.partition_helpers import _extract_partitioned_timetable
from airflow.exceptions import DeserializationError


def _make_serdag(exc: Exception):
    """
    Return a plain SerializedDagModel stand-in whose ``.dag`` property raises ``exc``.

    Plain class (not ``MagicMock``) because ``MagicMock.__getattr__`` swallows
    ``AttributeError`` from descriptors and falls back to auto-attribute creation,
    which masks the ``except AttributeError`` branch we want to exercise.
    """

    class _SerDag:
        dag_id = "bad-dag"

        @property
        def dag(self):
            raise exc

    return _SerDag()


@pytest.mark.parametrize(
    "exc",
    [
        DeserializationError("corrupted"),
        TypeError("mis-paired RollupMapper upstream/window"),
    ],
    ids=["DeserializationError", "TypeError"],
)
def test_extract_partitioned_timetable_deserialization_failure_logs_and_returns_none(exc):
    """
    Failures from the timetable deserialization path (``DeserializationError``)
    and from ``RollupMapper.__init__``'s eager pairing validation (``TypeError``)
    must produce a warning log and return ``None`` rather than propagating —
    protects the read-only UI from 500s when a serialized Dag is corrupted or
    misconfigured.
    """
    serdag = _make_serdag(exc)

    with mock.patch("airflow.api_fastapi.common.partition_helpers.log") as mock_log:
        result = _extract_partitioned_timetable(serdag)

    assert result is None
    assert mock_log.warning.mock_calls == [
        mock.call("Failed to deserialize timetable for Dag", dag_id="bad-dag", exc_info=True)
    ]


@pytest.mark.parametrize(
    "exc",
    [
        KeyError("missing key"),
        AttributeError("no attr"),
        ImportError("no module"),
        ValueError("bad value"),
        RuntimeError("unexpected"),
    ],
    ids=["KeyError", "AttributeError", "ImportError", "ValueError", "RuntimeError"],
)
def test_extract_partitioned_timetable_refactor_signal_exceptions_propagate(exc):
    """
    Exceptions outside the narrow ``(DeserializationError, TypeError)`` set must
    propagate so refactor bugs (renamed attribute, missing dict key, broken
    import path) and runtime errors surface to the caller instead of silently
    downgrading the route to non-rollup.
    """
    serdag = _make_serdag(exc)

    with mock.patch("airflow.api_fastapi.common.partition_helpers.log") as mock_log:
        with pytest.raises(type(exc)):
            _extract_partitioned_timetable(serdag)

    mock_log.warning.assert_not_called()
