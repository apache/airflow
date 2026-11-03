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

from typing import TYPE_CHECKING

import pytest
from itsdangerous import URLSafeSerializer

from airflow.configuration import conf
from airflow.models.dagbundle import DagBundleModel

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_teams

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.team import Team

pytestmark = pytest.mark.db_test


class TestDagBundleModel:
    def teardown_method(self):
        clear_db_dag_bundles()
        clear_db_teams()

    def test_get_team_name(self, testing_team: Team, session: Session):
        bundle = DagBundleModel(name="test_bundle")
        bundle.teams.append(testing_team)
        session.add(bundle)
        session.flush()

        assert DagBundleModel.get_team_name("test_bundle", session=session) == "testing"

    def test_get_team_name_no_team(self, session: Session):
        bundle = DagBundleModel(name="test_bundle")
        session.add(bundle)
        session.flush()

        assert DagBundleModel.get_team_name("test_bundle", session=session) is None

    def test_get_team_name_unknown_bundle(self, session: Session):
        assert DagBundleModel.get_team_name("does_not_exist", session=session) is None

    def test_get_team_names(self, testing_team: Team, session: Session):
        mapped = DagBundleModel(name="mapped_bundle")
        mapped.teams.append(testing_team)
        unmapped = DagBundleModel(name="unmapped_bundle")
        session.add_all([mapped, unmapped])
        session.flush()

        result = DagBundleModel.get_team_names(
            ["mapped_bundle", "unmapped_bundle", "does_not_exist"], session=session
        )

        # Only bundles actually mapped to a team are returned; callers treat absent keys as None.
        assert result == {"mapped_bundle": "testing"}

    def test_get_team_names_empty(self, session: Session):
        assert DagBundleModel.get_team_names([], session=session) == {}


class TestRenderUrlWithoutAVersion:
    """
    What ``render_url`` does before a bundle has ever reported a version.

    A template that really interpolates the version has nothing to render, but one that merely
    mentions "version" inside some other placeholder renders fine -- the distinction a substring
    test cannot make.
    """

    @staticmethod
    def _bundle(url_template: str, template_params: dict | None = None) -> DagBundleModel:
        bundle = DagBundleModel(name="bundle")
        bundle.signed_url_template = URLSafeSerializer(conf.get_mandatory_value("core", "fernet_key")).dumps(
            {"url": url_template, "bundle_name": "bundle"}
        )
        bundle.template_params = template_params
        return bundle

    @pytest.mark.parametrize(
        ("url_template", "template_params", "expected"),
        [
            pytest.param(
                "https://example.com/tree/{version}",
                None,
                None,
                id="version-placeholder-cannot-render",
            ),
            pytest.param(
                "https://example.com/tree/{version.foo}",
                None,
                None,
                id="attribute-of-version-cannot-render",
            ),
            pytest.param(
                "https://example.com/{version_label}/tree",
                {"version_label": "stable"},
                "https://example.com/stable/tree",
                id="distinct-param-still-renders",
            ),
            pytest.param(
                "https://example.com/{{version}}/tree",
                None,
                "https://example.com/{version}/tree",
                id="escaped-braces-still-render",
            ),
            pytest.param(
                "https://example.com/tree",
                None,
                "https://example.com/tree",
                id="no-placeholder-still-renders",
            ),
            pytest.param(
                "https://example.com/{unclosed",
                None,
                None,
                id="malformed-template-degrades-to-none",
            ),
        ],
    )
    def test_render_url_without_a_version(self, url_template, template_params, expected):
        assert self._bundle(url_template, template_params).render_url(None) == expected

    def test_render_url_with_a_version_is_unaffected(self):
        bundle = self._bundle("https://example.com/tree/{version}/{subdir}", {"subdir": "dags"})

        assert bundle.render_url("abc123") == "https://example.com/tree/abc123/dags"
