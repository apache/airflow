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

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags

pytestmark = pytest.mark.db_test


def _clear() -> None:
    clear_db_dags()
    clear_db_dag_bundles()


@pytest.fixture(autouse=True)
def bundles():
    """Two bundles with two live Dags each, and a third holding only a stale Dag, as when its every file fails."""
    _clear()
    with create_session() as session:
        session.add_all([DagBundleModel(name=name) for name in ("bundle_a", "bundle_b", "bundle_stale")])
        session.commit()
        for bundle_name, dag_ids, is_stale in [
            ("bundle_a", ["a1", "a2"], False),
            ("bundle_b", ["b1", "b2"], False),
            ("bundle_stale", ["stale"], True),
        ]:
            session.add_all(
                DagModel(dag_id=dag_id, bundle_name=bundle_name, fileloc=f"{dag_id}.py", is_stale=is_stale)
                for dag_id in dag_ids
            )
        session.commit()
    yield
    _clear()


def test_counts_bundles_holding_only_stale_dags(test_client):
    response = test_client.get("/dagBundles/count")

    assert response.status_code == 200
    assert response.json() == {"total_entries": 3}


def test_counts_only_bundles_holding_a_readable_dag(test_client):
    with mock.patch.object(
        test_client.app.state.auth_manager,
        "get_authorized_dag_ids",
        autospec=True,
        return_value={"a1", "a2"},
    ):
        response = test_client.get("/dagBundles/count")

    assert response.json() == {"total_entries": 1}


def test_a_viewer_may_count_bundles(test_client):
    """Viewers lack the bundle view, so this is how the import-error dialogs learn the count."""
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="viewer", role="viewer"))
    )

    response = test_client.get("/dagBundles/count", headers={"Authorization": f"Bearer {token}"})

    assert response.status_code == 200
    assert response.json() == {"total_entries": 3}


def test_should_raise_403_without_dag_read(unauthorized_test_client):
    assert unauthorized_test_client.get("/dagBundles/count").status_code == 403
