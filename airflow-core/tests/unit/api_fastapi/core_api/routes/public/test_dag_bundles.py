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
from sqlalchemy import select

from airflow.models.dagbundle import DagBundleModel, DagBundleRefreshRequest

pytestmark = pytest.mark.db_test

TEST_BUNDLE_NAME = "test-bundle"


class TestDagBundleRefreshEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self, session) -> None:
        # Clean up any existing test data
        session.query(DagBundleRefreshRequest).delete()
        session.query(DagBundleModel).filter(DagBundleModel.name == TEST_BUNDLE_NAME).delete()
        session.commit()

    @pytest.fixture
    def create_test_bundle(self, session):
        """Create a test bundle in the database."""
        bundle = DagBundleModel(name=TEST_BUNDLE_NAME)
        bundle.active = True
        session.add(bundle)
        session.commit()
        return bundle

    def test_refresh_existing_bundle(self, session, test_client, create_test_bundle):
        response = test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 201

        refresh_requests = session.scalars(select(DagBundleRefreshRequest)).all()
        assert len(refresh_requests) == 1
        assert refresh_requests[0].bundle_name == TEST_BUNDLE_NAME

    def test_refresh_nonexistent_bundle(self, session, test_client):
        response = test_client.post(
            "/dagBundles/nonexistent-bundle/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 404

        refresh_requests = session.scalars(select(DagBundleRefreshRequest)).all()
        assert refresh_requests == []

    def test_refresh_inactive_bundle(self, session, test_client):
        bundle = DagBundleModel(name=TEST_BUNDLE_NAME)
        bundle.active = False
        session.add(bundle)
        session.commit()

        response = test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 404

        refresh_requests = session.scalars(select(DagBundleRefreshRequest)).all()
        assert refresh_requests == []

    def test_duplicate_refresh_request(self, session, test_client, create_test_bundle):
        # First request should succeed
        response = test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 201

        # Duplicate request should return 409
        response = test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 409

        refresh_requests = session.scalars(select(DagBundleRefreshRequest)).all()
        assert len(refresh_requests) == 1

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            f"/dagBundles/{TEST_BUNDLE_NAME}/refresh",
            headers={"Accept": "application/json"},
        )
        assert response.status_code == 403
