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

import argparse
from unittest.mock import Mock, sentinel

import httpx
import pytest

from airflowctl.api.client import ServerResponseError
from airflowctl.ctl.utils.dag_run import (
    _get_dag_run_by_logical_date,
    _get_dag_run_by_run_id,
    _validate_selectors,
    resolve_dag_run,
    resolve_dag_run_id,
)


def make_args(**kwargs) -> argparse.Namespace:
    defaults = {"run_id": None, "logical_date": None}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def test_requires_one_selector(capsys):
    with pytest.raises(SystemExit) as exc:
        _validate_selectors(make_args())

    assert exc.value.code == 1
    assert "Provide either" in capsys.readouterr().out


def test_rejects_both_selectors(capsys):
    with pytest.raises(SystemExit) as exc:
        _validate_selectors(make_args(run_id="run_1", logical_date="2026-01-01T00:00:00Z"))

    assert exc.value.code == 1
    assert "Provide either" in capsys.readouterr().out


def test_accepts_run_id_only():
    _validate_selectors(make_args(run_id="run_1"))


def test_accepts_logical_date_only():
    _validate_selectors(make_args(logical_date="2026-01-01T00:00:00Z"))


def server_error(status_code: int) -> ServerResponseError:
    request = httpx.Request("GET", "http://localhost/api/v2/dags/d/dagRuns/r")
    response = httpx.Response(status_code, request=request)
    return ServerResponseError(f"error {status_code}", request=request, response=response)


def test_get_by_run_id_forwards_response_and_flags():
    client = Mock()
    client.dag_runs.get.return_value = sentinel.dag_run

    result = _get_dag_run_by_run_id(client, "my_dag", "run_1")

    assert result is sentinel.dag_run
    client.dag_runs.get.assert_called_once_with(
        dag_id="my_dag",
        dag_run_id="run_1",
        suppress_error_log=True,
    )


def test_get_by_run_id_404_exits(capsys):
    client = Mock()
    client.dag_runs.get.side_effect = server_error(404)

    with pytest.raises(SystemExit) as exc:
        _get_dag_run_by_run_id(client, "my_dag", "run_1")

    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().out


def test_get_by_run_id_other_error_propagates():
    client = Mock()
    client.dag_runs.get.side_effect = server_error(500)

    with pytest.raises(ServerResponseError):
        _get_dag_run_by_run_id(client, "my_dag", "run_1")


def test_get_by_logical_date_queries_exact_timestamp():
    client = Mock()
    client.dag_runs.list.return_value = Mock(dag_runs=[sentinel.dag_run])

    result = _get_dag_run_by_logical_date(client, "my_dag", "2026-01-01T00:00:00Z")

    assert result is sentinel.dag_run
    kwargs = client.dag_runs.list.call_args.kwargs
    assert kwargs["logical_date_gte"] == kwargs["logical_date_lte"]
    assert kwargs["logical_date_gte"].isoformat() == "2026-01-01T00:00:00+00:00"
    assert kwargs["order_by"] == "-id"
    assert kwargs["limit"] == 1


@pytest.mark.parametrize(
    "value",
    [
        pytest.param("2026-01-01T00:00:00", id="naive"),
        pytest.param("not-a-date", id="invalid"),
    ],
)
def test_get_by_logical_date_rejects_bad_input(value, capsys):
    client = Mock()

    with pytest.raises(SystemExit) as exc:
        _get_dag_run_by_logical_date(client, "my_dag", value)

    assert exc.value.code == 1
    client.dag_runs.list.assert_not_called()


def test_get_by_logical_date_not_found_exits(capsys):
    client = Mock()
    client.dag_runs.list.return_value = Mock(dag_runs=[])

    with pytest.raises(SystemExit) as exc:
        _get_dag_run_by_logical_date(client, "my_dag", "2026-01-01T00:00:00Z")

    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().out


def test_resolve_dag_run_id_skips_api_for_run_id():
    client = Mock()

    result = resolve_dag_run_id(client, make_args(run_id="run_1"))

    assert result == "run_1"
    client.dag_runs.get.assert_not_called()
    client.dag_runs.list.assert_not_called()


def test_get_by_logical_date_404_is_treated_as_not_found(capsys):
    client = Mock()
    client.dag_runs.list.side_effect = server_error(404)

    with pytest.raises(SystemExit) as exc:
        _get_dag_run_by_logical_date(client, "my_dag", "2026-01-01T00:00:00Z")

    assert exc.value.code == 1
    assert "not found" in capsys.readouterr().out


def test_get_by_logical_date_other_error_propagates():
    client = Mock()
    client.dag_runs.list.side_effect = server_error(500)

    with pytest.raises(ServerResponseError):
        _get_dag_run_by_logical_date(client, "my_dag", "2026-01-01T00:00:00Z")


def test_resolve_dag_run_by_run_id():
    client = Mock()
    client.dag_runs.get.return_value = sentinel.dag_run

    result = resolve_dag_run(client, make_args(dag_id="my_dag", run_id="run_1"))

    assert result is sentinel.dag_run
    client.dag_runs.get.assert_called_once_with(
        dag_id="my_dag",
        dag_run_id="run_1",
        suppress_error_log=True,
    )


def test_resolve_dag_run_by_logical_date():
    client = Mock()
    client.dag_runs.list.return_value = Mock(dag_runs=[sentinel.dag_run])

    result = resolve_dag_run(client, make_args(dag_id="my_dag", logical_date="2026-01-01T00:00:00Z"))

    assert result is sentinel.dag_run


def test_resolve_dag_run_id_by_logical_date():
    client = Mock()
    run = Mock(dag_run_id="run_42")
    client.dag_runs.list.return_value = Mock(dag_runs=[run])

    result = resolve_dag_run_id(client, make_args(dag_id="my_dag", logical_date="2026-01-01T00:00:00Z"))

    assert result == "run_42"
