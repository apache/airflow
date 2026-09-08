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

from airflow.api_fastapi.app import get_auth_manager
from airflow.api_fastapi.auth.managers.models.resource_details import DagDetails
from airflow.models.dagbundle import DagBundleModel

from tests_common.test_utils.api_fastapi import _check_last_log
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_logs

pytestmark = pytest.mark.db_test


class TestRefreshDagBundle:
    def setup_method(self):
        clear_db_dag_bundles()
        clear_db_logs()

    def teardown_method(self):
        clear_db_dag_bundles()
        clear_db_logs()

    @pytest.fixture
    def auth_manager(self, test_client):
        auth_manager = get_auth_manager()
        with mock.patch.object(auth_manager, "is_authorized_dag"):
            yield auth_manager

    def test_global_bundle_refresh_uses_dag_permission(self, auth_manager, session, test_client):
        session.add(DagBundleModel(name="global-bundle"))
        session.commit()
        auth_manager.is_authorized_dag.return_value = True

        first_response = test_client.post("/dagBundles/global-bundle/refresh")
        second_response = test_client.post("/dagBundles/global-bundle/refresh")

        assert first_response.status_code == 202
        assert first_response.json() == {
            "bundle_name": "global-bundle",
            "refresh_generation": 1,
        }
        assert second_response.status_code == 202
        assert second_response.json()["refresh_generation"] == 2
        auth_manager.is_authorized_dag.assert_called_with(
            method="PUT",
            details=DagDetails(id=None, team_name=None),
            user=mock.ANY,
        )
        session.expire_all()
        assert session.get(DagBundleModel, "global-bundle").refresh_generation == 2
        _check_last_log(session, dag_id=None, event="refresh_dag_bundle", logical_date=None)

    def test_team_bundle_refresh_uses_team_permission(self, auth_manager, session, test_client, testing_team):
        bundle = DagBundleModel(name="team-bundle")
        bundle.teams.append(testing_team)
        session.add(bundle)
        session.commit()
        auth_manager.is_authorized_dag.return_value = True

        response = test_client.post("/dagBundles/team-bundle/refresh")

        assert response.status_code == 202
        assert response.json()["refresh_generation"] == 1
        auth_manager.is_authorized_dag.assert_called_once_with(
            method="PUT",
            details=DagDetails(id=None, team_name=testing_team.name),
            user=mock.ANY,
        )

    def test_team_bundle_refresh_forbidden(self, auth_manager, session, test_client, testing_team):
        bundle = DagBundleModel(name="team-bundle")
        bundle.teams.append(testing_team)
        session.add(bundle)
        session.commit()
        auth_manager.is_authorized_dag.return_value = False

        response = test_client.post("/dagBundles/team-bundle/refresh")

        assert response.status_code == 403
        session.expire_all()
        assert session.get(DagBundleModel, "team-bundle").refresh_generation == 0

    @pytest.mark.parametrize("active", [True, False])
    def test_unknown_or_inactive_bundle_returns_not_found(self, active, auth_manager, session, test_client):
        bundle_name = "missing-bundle"
        if not active:
            bundle_name = "inactive-bundle"
            bundle = DagBundleModel(name=bundle_name)
            bundle.active = False
            session.add(bundle)
            session.commit()

        response = test_client.post(f"/dagBundles/{bundle_name}/refresh")

        assert response.status_code == 404
        auth_manager.is_authorized_dag.assert_not_called()

    def test_refresh_requires_authentication(self, session, unauthenticated_test_client):
        session.add(DagBundleModel(name="global-bundle"))
        session.commit()

        response = unauthenticated_test_client.post("/dagBundles/global-bundle/refresh")

        assert response.status_code == 401
