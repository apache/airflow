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

import importlib
from unittest import mock

import pytest

_MODULE = "airflow.providers.common.compat.openlineage.utils.sql"


def test_import():
    from airflow.providers.common.compat.openlineage.utils.sql import get_openlineage_facets_with_sql

    assert get_openlineage_facets_with_sql is not None


@pytest.fixture
def fallback_get_openlineage_facets_with_sql(monkeypatch):
    """Load the module as if the OpenLineage provider was too old to ship ``get_openlineage_facets_with_sql``."""
    from airflow.providers.openlineage import sqlparser

    monkeypatch.delattr(sqlparser, "get_openlineage_facets_with_sql")
    module = importlib.reload(importlib.import_module(_MODULE))
    yield module.get_openlineage_facets_with_sql
    monkeypatch.undo()
    importlib.reload(module)


@mock.patch(f"{_MODULE}.log", autospec=True)
def test_fallback_hook_without_database_info_method_returns_none(
    mock_log, fallback_get_openlineage_facets_with_sql
):
    hook = mock.MagicMock(spec=["get_connection"])

    assert fallback_get_openlineage_facets_with_sql(hook, "SELECT 1", "conn", None) is None
    mock_log.warning.assert_not_called()


@mock.patch(f"{_MODULE}.log", autospec=True)
def test_fallback_error_from_database_info_is_logged_as_warning(
    mock_log, fallback_get_openlineage_facets_with_sql
):
    hook = mock.MagicMock(spec=["get_connection", "get_openlineage_database_info"])
    hook.get_openlineage_database_info.side_effect = AttributeError("broken import")

    assert fallback_get_openlineage_facets_with_sql(hook, "SELECT 1", "conn", None) is None
    mock_log.warning.assert_called_once()
    assert mock_log.warning.call_args.args[1] == "conn"
    assert str(mock_log.warning.call_args.args[2]) == "broken import"


@mock.patch(f"{_MODULE}.log", autospec=True)
def test_fallback_error_from_dialect_is_logged_as_warning(mock_log, fallback_get_openlineage_facets_with_sql):
    hook = mock.MagicMock(
        spec=[
            "get_connection",
            "get_openlineage_database_info",
            "get_openlineage_database_dialect",
            "get_openlineage_default_schema",
        ]
    )
    hook.get_openlineage_database_dialect.side_effect = AttributeError("broken import")

    assert fallback_get_openlineage_facets_with_sql(hook, "SELECT 1", "conn", None) is None
    mock_log.warning.assert_called_once()
    assert mock_log.warning.call_args.args[1] == "conn"
    assert str(mock_log.warning.call_args.args[2]) == "broken import"
