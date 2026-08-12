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

import pytest

from airflow.models.callback import CallbackFetchMethod, CallbackState, ExecutorCallback
from airflow.sdk.definitions.callback import SyncCallback

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = pytest.mark.db_test


def sync_callback():
    """Empty (sync) callable used for the version-gating tests."""
    pass


def _make_queued_callback(session, dag_id="test_dag"):
    callback = ExecutorCallback(
        callback_def=SyncCallback(sync_callback, kwargs={}),
        fetch_method=CallbackFetchMethod.IMPORT_PATH,
    )
    callback.data["dag_id"] = dag_id
    callback.state = CallbackState.QUEUED
    session.add(callback)
    session.commit()
    return callback


@pytest.fixture
def old_ver_client(client):
    """Client configured to use the API version before the callback run endpoint was added."""
    client.headers["Airflow-API-Version"] = "2026-06-30"
    return client


class TestRunCallbackEndpointVersioning:
    """The callbacks/{callback_id}/run endpoint didn't exist before the 2026-08-01 API version."""

    @pytest.fixture(autouse=True)
    def setup_teardown(self):
        clear_db_callbacks()
        yield
        clear_db_callbacks()

    def test_old_version_returns_404(self, old_ver_client, session):
        """PATCH /callbacks/{callback_id}/run should not exist in older API versions."""
        callback = _make_queued_callback(session)

        response = old_ver_client.patch(f"/execution/callbacks/{callback.id}/run")

        assert response.status_code == 404

    def test_head_version_works(self, client, session):
        """PATCH /callbacks/{callback_id}/run should work in the current API version."""
        callback = _make_queued_callback(session)

        response = client.patch(f"/execution/callbacks/{callback.id}/run")

        assert response.status_code == 204
