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
"""No-DB-mode harness for the DAG processor (AIP-92).

The goal is a DAG processor that runs with zero metadata-DB access (all DB I/O
forwarded over the API). `forbid_db_access` makes any metadata-DB connection
raise, so tests can assert a code path is DB-free. Each phase that moves a DB
touchpoint behind the API flips one of the strict-xfail gates below to passing.
"""

from __future__ import annotations

import contextlib
from datetime import datetime, timezone
from unittest import mock

import pytest

from airflow.dag_processing.manager import DagFileProcessorManager

from tests_common.test_utils.config import conf_vars

MGR = "airflow.dag_processing.manager"
API_URL = "http://localhost:8080/dag-processing"

# These tests patch ``settings.engine`` to forbid connections, so they need a real engine to
# patch. In the Non-DB test environment ``settings.engine`` is None, so run them as DB tests.
pytestmark = pytest.mark.db_test


@contextlib.contextmanager
def forbid_db_access():
    """Raise ``AssertionError`` if any metadata-DB connection is opened in the block."""
    from airflow import settings

    def _forbid(*args, **kwargs):
        raise AssertionError("Metadata DB access is forbidden in no-DB mode")

    with (
        mock.patch.object(settings.engine, "connect", side_effect=_forbid),
        mock.patch.object(settings.engine, "raw_connection", side_effect=_forbid),
    ):
        yield


@conf_vars({("core", "load_examples"): "False"})
def test_forbid_db_access_blocks_real_queries():
    """The guard itself works: a real query under it raises."""
    from sqlalchemy import text

    from airflow.utils.session import create_session

    with forbid_db_access(), pytest.raises(AssertionError, match="forbidden"):
        with create_session() as session:
            session.execute(text("SELECT 1")).all()


@conf_vars({("core", "load_examples"): "False", ("core", "dag_processing_api_server_url"): API_URL})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_bundle_state_is_db_free_when_api_configured(mock_client_cls):
    """Reading and updating bundle state go through the API, opening no DB connection."""
    client = mock_client_cls.return_value
    client.get_bundle_state.return_value = None
    manager = DagFileProcessorManager(max_runs=1)
    with forbid_db_access():
        assert manager.get_bundle_state("any-bundle") is None
        manager.update_bundle_state("any-bundle", last_refreshed=datetime.now(timezone.utc), version="v1")
    client.get_bundle_state.assert_called_once_with("any-bundle")
    client.update_bundle_state.assert_called_once()


@conf_vars({("core", "load_examples"): "False", ("core", "dag_processing_api_server_url"): API_URL})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_sync_bundles_is_db_free_when_api_configured(mock_client_cls):
    """Startup bundle sync goes through the API, opening no DB connection."""
    manager = DagFileProcessorManager(max_runs=1)
    with forbid_db_access():
        manager.sync_bundles()
    mock_client_cls.return_value.sync_bundles.assert_called_once_with()


@conf_vars({("core", "load_examples"): "False", ("core", "dag_processing_api_server_url"): API_URL})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_stale_sweep_is_db_free_when_api_configured(mock_client_cls):
    """Time-based stale-dag deactivation and warning purge go through the API, no DB."""
    manager = DagFileProcessorManager(max_runs=1)
    with forbid_db_access():
        manager.deactivate_stale_dags(last_parsed={})
        manager.purge_inactive_dag_warnings()
    client = mock_client_cls.return_value
    client.deactivate_stale_dags.assert_called_once()
    client.purge_inactive_dag_warnings.assert_called_once_with()


@conf_vars({("core", "load_examples"): "False", ("core", "dag_processing_api_server_url"): API_URL})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_priority_claim_is_db_free_when_api_configured(mock_client_cls):
    """Claiming priority parse requests goes through the API, opening no DB connection."""
    mock_client_cls.return_value.claim_priority_files.return_value = []
    manager = DagFileProcessorManager(max_runs=1)
    with forbid_db_access():
        assert manager.claim_priority_files() == []
    mock_client_cls.return_value.claim_priority_files.assert_called_once()


@conf_vars({("core", "load_examples"): "False", ("core", "dag_processing_api_server_url"): API_URL})
@mock.patch(f"{MGR}.DagProcessingApiClient")
def test_callback_claim_is_db_free_when_api_configured(mock_client_cls):
    """Fetching callbacks goes through the API, opening no DB connection."""
    mock_client_cls.return_value.fetch_callbacks.return_value = []
    manager = DagFileProcessorManager(max_runs=1)
    with forbid_db_access():
        assert manager.fetch_callbacks() == []
    mock_client_cls.return_value.fetch_callbacks.assert_called_once()
