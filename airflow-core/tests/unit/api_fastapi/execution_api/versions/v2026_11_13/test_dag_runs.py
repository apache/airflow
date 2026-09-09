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
from cadwyn import generate_versioned_models

from airflow.api_fastapi.execution_api.datamodels.dagrun import ClearDagRunPayload
from airflow.api_fastapi.execution_api.versions import bundle
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import clear_db_runs

# The version that introduces failed-only clear and the last one that predates it.
CURRENT_VERSION = "2026-11-13"
PREVIOUS_VERSION = "2026-10-30"


class TestClearDagRunPayloadVersioning:
    """The ``only_failed`` field is versioned additively: present in the new version, absent before it."""

    def test_field_present_in_current_version(self):
        """@AC-FR004-01 The new version's ClearDagRunPayload exposes the additive only_failed field."""
        versioned = generate_versioned_models(bundle)[CURRENT_VERSION][ClearDagRunPayload]
        assert "only_failed" in versioned.model_fields

    def test_field_negotiated_away_in_previous_version(self):
        """@AC-FR004-02 The previous version negotiates only_failed away (field absent ⇒ whole-run)."""
        versioned = generate_versioned_models(bundle)[PREVIOUS_VERSION][ClearDagRunPayload]
        assert "only_failed" not in versioned.model_fields


@pytest.mark.db_test
class TestDagRunClearVersionNegotiation:
    def setup_method(self):
        clear_db_runs()

    def teardown_method(self):
        clear_db_runs()

    @pytest.fixture
    def old_ver_client(self, client):
        """Client pinned to the version that predates the ``only_failed`` clear field."""
        client.headers["Airflow-API-Version"] = PREVIOUS_VERSION
        return client

    def test_old_version_no_body_clears_whole_run(self, old_ver_client, session, dag_maker):
        """@AC-FR004-01 An old-version client sending no body clears the whole run (only_failed defaults False)."""
        from airflow.providers.standard.operators.empty import EmptyOperator

        dag_id = "test_old_clear_no_body"
        run_id = "test_run_id"
        with dag_maker(dag_id=dag_id, session=session, serialized=True):
            EmptyOperator(task_id="test_task")
        dag_maker.create_dagrun(run_id=run_id, state=DagRunState.SUCCESS)
        session.commit()

        with mock.patch.object(SerializedDAG, "clear", autospec=True) as mock_clear:
            response = old_ver_client.post(f"/execution/dag-runs/{dag_id}/{run_id}/clear")

        assert response.status_code == 204
        mock_clear.assert_called_once()
        assert mock_clear.call_args.kwargs["only_failed"] is False
