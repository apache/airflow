#
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

from airflow.models.dag_bundle_manifest import DagBundleManifest, get_bundle_manifests

from tests_common.test_utils.asserts import assert_queries_count

pytestmark = pytest.mark.db_test


class TestGetBundleManifests:
    def test_returns_only_the_requested_versions_that_are_stored(self, session):
        session.add_all(
            [
                DagBundleManifest(bundle_name="bundle", bundle_version="v1", manifest={"version": "v1"}),
                DagBundleManifest(bundle_name="bundle", bundle_version="v2", manifest={"version": "v2"}),
                DagBundleManifest(bundle_name="other", bundle_version="v1", manifest={"version": "other"}),
            ]
        )
        session.flush()

        with assert_queries_count(1, session=session):
            manifests = get_bundle_manifests(
                [("bundle", "v1"), ("bundle", "v1"), ("other", "v1"), ("bundle", "missing")], session=session
            )

        assert manifests == {("bundle", "v1"): {"version": "v1"}, ("other", "v1"): {"version": "other"}}
        session.rollback()

    def test_no_keys_issue_no_query(self, session):
        with assert_queries_count(0, session=session):
            assert get_bundle_manifests([], session=session) == {}
