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

from airflow.models.callback import Callback, CallbackFetchMethod, ExecutorCallback
from airflow.sdk.definitions.callback import SyncCallback
from airflow.utils.state import CallbackState

pytestmark = pytest.mark.db_test


def _make_callback(session, state: CallbackState) -> Callback:
    cb = ExecutorCallback(SyncCallback("os.getcwd"), fetch_method=CallbackFetchMethod.IMPORT_PATH)
    cb.state = state
    session.add(cb)
    session.commit()
    return cb


class TestRunCallback:
    def test_claim_transitions_queued_to_running(self, client, session):
        """A QUEUED callback is claimed and moved to RUNNING; the response reports the new state."""
        cb = _make_callback(session, CallbackState.QUEUED)

        response = client.post(f"/execution/callbacks/{cb.id}/run")

        assert response.status_code == 200
        assert response.json() == {"id": str(cb.id), "state": "running"}

        session.expire_all()
        assert session.get(Callback, cb.id).state == CallbackState.RUNNING

    def test_returns_404_for_unknown_callback(self, client):
        response = client.post("/execution/callbacks/00000000-0000-0000-0000-000000000000/run")
        assert response.status_code == 404

    def test_returns_422_for_invalid_uuid(self, client):
        response = client.post("/execution/callbacks/not-a-uuid/run")
        assert response.status_code == 422

    @pytest.mark.parametrize("state", [CallbackState.RUNNING, CallbackState.SUCCESS, CallbackState.FAILED])
    def test_a_callback_can_only_be_claimed_once(self, client, session, state):
        """A callback already RUNNING or terminal cannot be claimed again — replay/redelivery is refused."""
        cb = _make_callback(session, state)

        response = client.post(f"/execution/callbacks/{cb.id}/run")

        assert response.status_code == 409
