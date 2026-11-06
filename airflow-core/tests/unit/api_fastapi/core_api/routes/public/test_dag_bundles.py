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

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from itsdangerous import URLSafeSerializer
from sqlalchemy import insert, update

from airflow.api_fastapi.auth.managers.models.resource_details import AccessView
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.configuration import conf
from airflow.models import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError
from airflow.models.team import Team, dag_bundle_team_association_table
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_teams,
)

if TYPE_CHECKING:
    from collections.abc import Generator

pytestmark = pytest.mark.db_test

GIT_BUNDLE = "git_bundle"
LOCAL_BUNDLE = "local_bundle"
GONE_BUNDLE = "gone_bundle"
OTHER_TEAM_BUNDLE = "other_team_bundle"
DAGLESS_BUNDLE = "dagless_bundle"
TEAM_NAME = "team_a"
# ``dag_in_git_bundle`` is seeded at ``dag_0.py``, so this is a file with a registered Dag.
REGISTERED_FILE = "dag_0.py"
UNREGISTERED_FILE = "broken.py"
# A second registered file in GIT_BUNDLE, which also has an import error.
SECOND_REGISTERED_FILE = "second.py"

GIT_VERSION = "8f0e5b1c9a2d4e6f8a0b1c2d3e4f5a6b7c8d9e0f"
REFRESHED_AT = datetime(2026, 9, 10, 12, 0, tzinfo=UTC)
PARSED_AT = datetime(2026, 9, 10, 12, 1, tzinfo=UTC)
PARSE_DURATION = 0.125
GONE_REFRESHED_AT = datetime(2026, 9, 1, 8, 30, tzinfo=UTC)

WITH_DAGS = (GIT_BUNDLE, LOCAL_BUNDLE, GONE_BUNDLE, OTHER_TEAM_BUNDLE)
REMOVED_DAG_ID = "removed_from_dag_0"
# Sorted by name, which is the endpoint's default order.
ALL_BUNDLES = [DAGLESS_BUNDLE, GIT_BUNDLE, GONE_BUNDLE, LOCAL_BUNDLE, OTHER_TEAM_BUNDLE]
GIT_BUNDLE_FILES = [UNREGISTERED_FILE, REGISTERED_FILE, SECOND_REGISTERED_FILE]


def _sign(url_template: str, bundle_name: str) -> str:
    """
    Sign a view-url template the way the Dag processor does.

    It has to be the *configured* fernet key: ``DagBundleModel._unsign_url`` reads that key, and
    signing with any other value fails the signature check and yields ``bundle_url: None`` -- which
    would let the malformed-template tests below pass for entirely the wrong reason.
    """
    serializer = URLSafeSerializer(conf.get_mandatory_value("core", "fernet_key"))
    return serializer.dumps({"url": url_template, "bundle_name": bundle_name})


def _clear() -> None:
    clear_db_import_errors()
    clear_db_dags()
    clear_db_dag_bundles()
    # clear_db_dag_bundles does not touch Team, and Team.name is a primary key, so the
    # multi-team test would hit an IntegrityError on a rerun without this.
    clear_db_teams()


def _make_bundle(
    name: str,
    *,
    version: str | None = None,
    last_refreshed: datetime | None = None,
    active: bool = True,
    url_template: str | None = None,
    template_params: dict | None = None,
) -> DagBundleModel:
    # ``DagBundleModel.__init__`` accepts only name and version, so the rest is set afterwards.
    bundle = DagBundleModel(name=name, version=version)
    bundle.last_refreshed = last_refreshed
    bundle.active = active
    bundle.signed_url_template = None if url_template is None else _sign(url_template, name)
    bundle.template_params = template_params
    return bundle


@pytest.fixture(autouse=True)
def bundles() -> Generator[None, None, None]:
    """
    Bundles covering versioned, non-versioned, deactivated, unreadable and Dag-less.

    Seeding and clearing live in one fixture on purpose: as two autouse fixtures pytest is free to
    order the clear after the seed, which silently empties the tables the tests assert on.
    """
    _clear()
    with create_session() as session:
        session.add_all(
            [
                _make_bundle(
                    GIT_BUNDLE,
                    version=GIT_VERSION,
                    last_refreshed=REFRESHED_AT,
                    url_template="https://github.com/example/repo/tree/{version}/{subdir}",
                    template_params={"subdir": "dags"},
                ),
                _make_bundle(LOCAL_BUNDLE, version=None, last_refreshed=REFRESHED_AT),
                _make_bundle(GONE_BUNDLE, version="deadbeef", last_refreshed=GONE_REFRESHED_AT, active=False),
                _make_bundle(OTHER_TEAM_BUNDLE, version="cafed00d", last_refreshed=REFRESHED_AT),
                # No Dag has ever parsed from this one, as on a first deploy whose only file fails.
                _make_bundle(DAGLESS_BUNDLE, version="0badcafe", last_refreshed=REFRESHED_AT),
            ]
        )
        # ``dag.bundle_name`` is a foreign key onto ``dag_bundle.name`` and nothing declares a
        # relationship between the two mappers, so the bundles have to land before the Dags.
        session.commit()

        for index, bundle_name in enumerate(WITH_DAGS):
            session.add(
                DagModel(
                    dag_id=f"dag_in_{bundle_name}",
                    fileloc=f"dag_{index}.py",
                    relative_fileloc=f"dag_{index}.py",
                    bundle_name=bundle_name,
                    is_paused=False,
                    # ``is_stale`` defaults to True on the model, and a parsed Dag is not stale.
                    is_stale=False,
                    last_parsed_time=PARSED_AT,
                    last_parse_duration=PARSE_DURATION,
                )
            )
        # Removed from REGISTERED_FILE but never deleted: stale, and frozen at an older parse
        # that took far longer. An unrestricted MAX over the file would pair this duration with
        # the live row's newer timestamp.
        session.add(
            DagModel(
                dag_id=REMOVED_DAG_ID,
                fileloc=REGISTERED_FILE,
                relative_fileloc=REGISTERED_FILE,
                bundle_name=GIT_BUNDLE,
                is_paused=False,
                is_stale=True,
                last_parsed_time=PARSED_AT - timedelta(hours=1),
                last_parse_duration=PARSE_DURATION * 100,
            )
        )
        # A second live Dag in GIT_BUNDLE, in a file of its own.
        session.add(
            DagModel(
                dag_id="second_in_git_bundle",
                fileloc=SECOND_REGISTERED_FILE,
                relative_fileloc=SECOND_REGISTERED_FILE,
                bundle_name=GIT_BUNDLE,
                is_paused=False,
                is_stale=False,
            )
        )
        session.add_all(
            [
                # An error in a file that also registered a Dag.
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=REGISTERED_FILE,
                    stacktrace="boom",
                    timestamp=REFRESHED_AT,
                ),
                # An error in a file that never registered a Dag.
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=UNREGISTERED_FILE,
                    stacktrace="boom before any dag",
                    timestamp=REFRESHED_AT,
                ),
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=SECOND_REGISTERED_FILE,
                    stacktrace="boom in a second file",
                    timestamp=REFRESHED_AT,
                ),
            ]
        )
        session.commit()

    yield

    _clear()


def _patch_dag_bundles_view(test_client, is_authorized):
    """Decide ``AccessView.DAG_BUNDLES`` with ``is_authorized(team_name)``; other views are left as they are."""
    auth_manager = test_client.app.state.auth_manager
    real_authorize_view = auth_manager.authorize_view

    def authorize_view(*, access_view, user, team_name=None):
        if access_view is AccessView.DAG_BUNDLES:
            return is_authorized(team_name)
        return real_authorize_view(access_view=access_view, user=user, team_name=team_name)

    return mock.patch.object(auth_manager, "authorize_view", autospec=True, side_effect=authorize_view)


@pytest.fixture
def no_dag_read_client(test_client):
    """A caller who holds the bundle view but may read no Dag at all."""
    with mock.patch.object(
        test_client.app.state.auth_manager, "get_authorized_dag_ids", autospec=True, return_value=set()
    ):
        yield test_client


@pytest.fixture
def no_bundle_view_client(test_client):
    """A caller who may read every Dag but is denied ``AccessView.DAG_BUNDLES``."""
    with _patch_dag_bundles_view(test_client, lambda team_name: False):
        yield test_client


def _headers_for(test_client, role: str) -> dict[str, str]:
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username=role, role=role))
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def op_headers(test_client):
    """
    A SimpleAuthManager op, the lowest role holding the bundle view by default.

    ``test_client`` authenticates as an admin, so this is the caller that shows the admin-only
    import-error views do not narrow the bundle pages either.
    """
    return _headers_for(test_client, "op")


class TestGetDagBundles:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get("/dagBundles").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get("/dagBundles").status_code == 403

    def test_should_raise_403_without_the_bundle_view(self, no_bundle_view_client):
        """Reading every Dag does not grant the bundle view."""
        assert no_bundle_view_client.get("/dagBundles").status_code == 403

    @pytest.mark.parametrize("role", ["viewer", "user"])
    def test_should_raise_403_below_op(self, test_client, role):
        """Bundles list every deployed file, so SimpleAuthManager keeps them from viewers and users."""
        response = test_client.get("/dagBundles", headers=_headers_for(test_client, role))

        assert response.status_code == 403

    def test_lists_every_bundle(self, test_client):
        response = test_client.get("/dagBundles")

        assert response.status_code == 200
        body = response.json()
        assert [bundle["name"] for bundle in body["dag_bundles"]] == ALL_BUNDLES
        assert body["total_entries"] == len(ALL_BUNDLES)

    def test_lists_every_bundle_to_a_caller_who_may_read_no_dag(self, no_dag_read_client):
        """Bundles are authorized on their own view, not derived from the Dags inside them."""
        body = no_dag_read_client.get("/dagBundles").json()

        assert [bundle["name"] for bundle in body["dag_bundles"]] == ALL_BUNDLES

    def test_import_error_count_covers_the_whole_bundle_for_an_op(self, op_headers, test_client):
        """
        Every error recorded against the bundle is counted, whoever the caller is.

        ``GIT_BUNDLE`` holds an error in a file with a Dag, one in a file that never registered a
        Dag, and one in a file holding another Dag. A count narrowed to what the caller may read
        reads as "nothing is wrong" when something is.
        """
        body = test_client.get("/dagBundles", headers=op_headers).json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert bundle["import_error_count"] == 3

    def test_dagless_bundle_reports_its_fields(self, test_client):
        """A Dag-less bundle is a real row, not a name-only placeholder."""
        body = test_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == DAGLESS_BUNDLE)

        assert bundle["version"] == "0badcafe"
        assert bundle["active"] is True
        assert bundle["last_refreshed"].startswith("2026-09-10T12:00:00")
        assert bundle["import_error_count"] == 0

    def test_versioned_bundle_fields(self, no_dag_read_client):
        """The bundle url is part of the bundle, so it needs no Dag or Dag version read."""
        with mock.patch.object(
            no_dag_read_client.app.state.auth_manager, "is_authorized_dag", autospec=True, return_value=False
        ):
            body = no_dag_read_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert bundle["version"] == GIT_VERSION
        assert bundle["active"] is True
        assert bundle["last_refreshed"].startswith("2026-09-10T12:00:00")
        # Rendered from the signed template with the stored template params filled in.
        assert bundle["bundle_url"] == f"https://github.com/example/repo/tree/{GIT_VERSION}/dags"

    def test_non_versioned_bundle_has_no_version_but_keeps_a_timestamp(self, test_client):
        """A bundle that does not support versioning still reports when it was last refreshed."""
        body = test_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == LOCAL_BUNDLE)

        assert bundle["version"] is None
        assert bundle["bundle_url"] is None
        assert bundle["last_refreshed"].startswith("2026-09-10T12:00:00")
        assert bundle["import_error_count"] == 0

    def test_reports_a_refresh_that_did_not_change_the_version(self, test_client, session):
        """
        The behaviour the whole feature rests on, guarded on the read side.

        A Dag processor bumps ``last_refreshed`` on every successful refresh and leaves ``version``
        alone when the source did not move (``update_bundle_state(..., version=None)``), which is
        what tells an author "a processor is alive and has looked" as distinct from "the code
        changed". The writer side is pinned by
        ``test_manager.py::TestDagFileProcessorManager::test_refresh_dag_bundles_versioned_version_unchanged_calls_update_bundle_state``;
        this pins that the endpoint actually surfaces it.
        """
        before = test_client.get("/dagBundles").json()
        was = next(b for b in before["dag_bundles"] if b["name"] == GIT_BUNDLE)

        later = REFRESHED_AT + timedelta(minutes=5)
        session.execute(
            update(DagBundleModel).where(DagBundleModel.name == GIT_BUNDLE).values(last_refreshed=later)
        )
        session.commit()

        after = test_client.get("/dagBundles").json()
        now = next(b for b in after["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert now["last_refreshed"] != was["last_refreshed"]
        assert now["last_refreshed"].startswith("2026-09-10T12:05:00")
        assert now["version"] == was["version"] == GIT_VERSION

    def test_deactivated_bundle_is_still_listed(self, test_client):
        """A bundle dropped from the config stays visible and flagged, rather than vanishing."""
        body = test_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GONE_BUNDLE)

        assert bundle["active"] is False
        assert bundle["version"] == "deadbeef"

    @pytest.mark.parametrize(
        "template",
        [
            pytest.param("https://example.com/{0}", id="positional-placeholder"),
            pytest.param("https://example.com/{version.foo}", id="attribute-placeholder"),
        ],
    )
    def test_a_malformed_url_template_does_not_fail_the_collection(self, test_client, template, session):
        """``render_url`` guards only KeyError/ValueError, so a bad template must degrade to null."""
        bundle = session.get(DagBundleModel, GIT_BUNDLE)
        bundle.signed_url_template = _sign(template, GIT_BUNDLE)
        session.commit()

        response = test_client.get("/dagBundles")

        assert response.status_code == 200
        body = response.json()
        assert next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)["bundle_url"] is None
        # Every other row is unaffected.
        assert body["total_entries"] == len(ALL_BUNDLES)

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param({"limit": 2}, [DAGLESS_BUNDLE, GIT_BUNDLE], id="limit"),
            pytest.param({"limit": 2, "offset": 2}, [GONE_BUNDLE, LOCAL_BUNDLE], id="offset"),
            pytest.param({"order_by": "-name"}, list(reversed(ALL_BUNDLES)), id="name-desc"),
            # GONE_BUNDLE alone was refreshed earlier; the rest tie on REFRESHED_AT.
            pytest.param({"order_by": "last_refreshed", "limit": 1}, [GONE_BUNDLE], id="last-refreshed-asc"),
        ],
    )
    def test_pagination_and_sorting(self, test_client, params, expected):
        response = test_client.get("/dagBundles", params=params)

        assert response.status_code == 200
        body = response.json()
        assert [bundle["name"] for bundle in body["dag_bundles"]] == expected
        # total_entries counts the whole permitted set, not the page.
        assert body["total_entries"] == len(ALL_BUNDLES)

    def test_rejects_an_unsupported_sort_column(self, test_client):
        response = test_client.get("/dagBundles", params={"order_by": "signed_url_template"})

        assert response.status_code == 400

    def test_never_exposes_the_signed_url_template(self, test_client):
        """The template is signed with the fernet key; only the rendered url may leave."""
        body = test_client.get("/dagBundles").json()

        for bundle in body["dag_bundles"]:
            assert "signed_url_template" not in bundle
            assert "template_params" not in bundle

    @conf_vars({("core", "multi_team"): "False"})
    def test_team_name_is_null_when_multi_team_is_off(self, test_client):
        body = test_client.get("/dagBundles").json()

        assert {bundle["team_name"] for bundle in body["dag_bundles"]} == {None}


class TestDagBundleTeamScoping:
    """In multi-team mode the bundle view is checked against each bundle's own team."""

    OTHER_TEAM = "team_b"

    @pytest.fixture(autouse=True)
    def team_bundles(self, session) -> None:
        session.add_all([Team(name=TEAM_NAME), Team(name=self.OTHER_TEAM)])
        session.commit()
        session.execute(
            insert(dag_bundle_team_association_table).values(
                [
                    {"dag_bundle_name": GIT_BUNDLE, "team_name": TEAM_NAME},
                    {"dag_bundle_name": OTHER_TEAM_BUNDLE, "team_name": self.OTHER_TEAM},
                ]
            )
        )
        session.commit()

    @conf_vars({("core", "multi_team"): "True"})
    def test_lists_only_the_bundles_of_permitted_teams(self, test_client):
        # ``None`` authorizes the team-less bundles.
        with _patch_dag_bundles_view(test_client, lambda team_name: team_name in (None, TEAM_NAME)):
            body = test_client.get("/dagBundles").json()

        names = [bundle["name"] for bundle in body["dag_bundles"]]
        assert OTHER_TEAM_BUNDLE not in names
        assert names == [DAGLESS_BUNDLE, GIT_BUNDLE, GONE_BUNDLE, LOCAL_BUNDLE]
        assert next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)["team_name"] == TEAM_NAME
        # Filtered in the query, so the other team's bundle is absent from the count as well.
        assert body["total_entries"] == len(names)

    @conf_vars({("core", "multi_team"): "True"})
    def test_authorizes_a_team_less_bundle_without_a_team(self, test_client):
        with _patch_dag_bundles_view(test_client, lambda team_name: team_name is None):
            body = test_client.get("/dagBundles").json()

        assert [bundle["name"] for bundle in body["dag_bundles"]] == [
            DAGLESS_BUNDLE,
            GONE_BUNDLE,
            LOCAL_BUNDLE,
        ]

    @conf_vars({("core", "multi_team"): "True"})
    def test_admits_a_caller_granted_the_view_for_only_one_team(self, test_client):
        with _patch_dag_bundles_view(test_client, lambda team_name: team_name == TEAM_NAME):
            response = test_client.get("/dagBundles")

        assert response.status_code == 200
        assert [bundle["name"] for bundle in response.json()["dag_bundles"]] == [GIT_BUNDLE]

    @conf_vars({("core", "multi_team"): "True"})
    def test_should_raise_403_without_the_view_for_any_team(self, no_bundle_view_client):
        assert no_bundle_view_client.get("/dagBundles").status_code == 403

    @conf_vars({("core", "multi_team"): "True"})
    def test_hides_another_teams_bundles_when_the_view_ignores_the_team(self, test_client):
        """An auth manager may grant the view whatever ``team_name`` is; team membership still applies."""
        with (
            _patch_dag_bundles_view(test_client, lambda team_name: True),
            mock.patch.object(
                test_client.app.state.auth_manager,
                "filter_authorized_teams",
                autospec=True,
                side_effect=lambda *, teams_names, user, method="GET": teams_names & {TEAM_NAME},
            ),
        ):
            body = test_client.get("/dagBundles").json()

        names = [bundle["name"] for bundle in body["dag_bundles"]]
        assert names == [DAGLESS_BUNDLE, GIT_BUNDLE, GONE_BUNDLE, LOCAL_BUNDLE]
        assert body["total_entries"] == len(names)

    @conf_vars({("core", "multi_team"): "True"})
    @pytest.mark.parametrize("path", ["", "/files"])
    def test_404_for_a_bundle_of_another_team(self, test_client, path):
        """A 404 rather than a 403, so the response does not confirm the bundle exists."""
        with _patch_dag_bundles_view(test_client, lambda team_name: team_name in (None, TEAM_NAME)):
            response = test_client.get(f"/dagBundles/{OTHER_TEAM_BUNDLE}{path}")

        assert response.status_code == 404


class TestGetDagBundle:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"/dagBundles/{GIT_BUNDLE}").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get(f"/dagBundles/{GIT_BUNDLE}").status_code == 403

    def test_should_raise_403_without_the_bundle_view(self, no_bundle_view_client):
        assert no_bundle_view_client.get(f"/dagBundles/{GIT_BUNDLE}").status_code == 403

    def test_returns_the_bundle(self, test_client):
        response = test_client.get(f"/dagBundles/{GIT_BUNDLE}")

        assert response.status_code == 200
        body = response.json()
        assert body["name"] == GIT_BUNDLE
        assert body["version"] == GIT_VERSION
        assert body["last_refreshed"] == "2026-09-10T12:00:00Z"
        assert body["active"] is True
        assert body["bundle_url"] == f"https://github.com/example/repo/tree/{GIT_VERSION}/dags"
        assert body["import_error_count"] == 3

    def test_counts_every_live_dag_whatever_the_caller_may_read(self, no_dag_read_client):
        """``GIT_BUNDLE`` holds two live Dags and one stale one."""
        body = no_dag_read_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["dag_count"] == 2

    def test_shows_a_bundle_with_no_dags(self, op_headers, test_client):
        """The first-deploy-is-broken case: a bundle whose only file fails before defining a Dag."""
        response = test_client.get(f"/dagBundles/{DAGLESS_BUNDLE}", headers=op_headers)

        assert response.status_code == 200
        assert response.json()["dag_count"] == 0

    def test_404_for_an_unknown_bundle(self, test_client):
        assert test_client.get("/dagBundles/no_such_bundle").status_code == 404

    def test_dag_count_excludes_stale_dags(self, test_client, session):
        """``GIT_BUNDLE``'s Dags all go stale, so nothing live is left to count."""
        session.execute(update(DagModel).where(DagModel.bundle_name == GIT_BUNDLE).values(is_stale=True))
        session.commit()

        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["dag_count"] == 0


class TestGetDagBundleFiles:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").status_code == 403

    def test_should_raise_403_without_the_bundle_view(self, no_bundle_view_client):
        assert no_bundle_view_client.get(f"/dagBundles/{GIT_BUNDLE}/files").status_code == 403

    def test_lists_files_ordered_by_path(self, test_client):
        response = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files")

        assert response.status_code == 200
        body = response.json()
        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == GIT_BUNDLE_FILES
        assert body["total_entries"] == len(GIT_BUNDLE_FILES)

    def test_lists_every_file_to_a_caller_who_may_read_no_dag(self, no_dag_read_client):
        """The list is the bundle's, so it is not narrowed to the files of readable Dags."""
        body = no_dag_read_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert list(by_path) == GIT_BUNDLE_FILES
        assert by_path[SECOND_REGISTERED_FILE]["dag_count"] == 1
        assert by_path[SECOND_REGISTERED_FILE]["import_error_count"] == 1

    def test_an_op_sees_a_file_that_registered_no_dag(self, op_headers, test_client):
        """The case the page most needs to show: a file that failed before defining a Dag."""
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files", headers=op_headers).json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[UNREGISTERED_FILE]["dag_count"] == 0
        assert by_path[UNREGISTERED_FILE]["import_error_count"] == 1
        assert by_path[UNREGISTERED_FILE]["last_parsed_time"] is None
        assert by_path[UNREGISTERED_FILE]["last_parse_duration"] is None

    def test_reports_parse_time_and_duration(self, test_client):
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["dag_count"] == 1
        assert by_path[REGISTERED_FILE]["last_parsed_time"] == "2026-09-10T12:01:00Z"
        assert by_path[REGISTERED_FILE]["last_parse_duration"] == PARSE_DURATION

    def test_a_file_whose_dags_went_stale_stays_listed_while_it_has_an_error(self, test_client, session):
        """Marking a file's Dags stale must not hide the row that explains the breakage."""
        session.execute(
            update(DagModel)
            .where(DagModel.relative_fileloc == REGISTERED_FILE, DagModel.bundle_name == GIT_BUNDLE)
            .values(is_stale=True)
        )
        session.commit()

        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["dag_count"] == 0
        assert by_path[REGISTERED_FILE]["import_error_count"] == 1

    def test_parse_duration_ignores_a_stale_dag_from_an_older_parse(self, test_client):
        """
        A Dag removed from a file keeps its row, frozen at the parse it was last seen in.

        Aggregating the two columns independently would pair that older, slower duration with the
        newer timestamp and report a parse time that never happened.
        """
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["last_parsed_time"] == "2026-09-10T12:01:00Z"
        assert by_path[REGISTERED_FILE]["last_parse_duration"] == PARSE_DURATION

    def test_a_file_with_no_live_dag_and_no_error_is_dropped(self, test_client, session):
        """A file whose Dags are all stale and which has no error is one the bundle no longer has."""
        session.execute(update(DagModel).where(DagModel.bundle_name == LOCAL_BUNDLE).values(is_stale=True))
        session.commit()

        body = test_client.get(f"/dagBundles/{LOCAL_BUNDLE}/files").json()

        assert body["dag_bundle_files"] == []
        assert body["total_entries"] == 0

    def test_a_file_with_no_error_reports_zero(self, test_client):
        body = test_client.get(f"/dagBundles/{LOCAL_BUNDLE}/files").json()

        assert [file["import_error_count"] for file in body["dag_bundle_files"]] == [0]

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param({"limit": 1}, [UNREGISTERED_FILE], id="limit"),
            pytest.param({"offset": 1}, [REGISTERED_FILE, SECOND_REGISTERED_FILE], id="offset"),
            pytest.param({"limit": 1, "offset": 1}, [REGISTERED_FILE], id="limit-and-offset"),
            # ``limit`` is a non-negative int, and ``Select.limit(0)`` returns nothing, so zero
            # has to mean nothing here too rather than being read as "unlimited".
            pytest.param({"limit": 0}, [], id="limit-zero"),
        ],
    )
    def test_pagination(self, test_client, params, expected):
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files", params=params).json()

        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == expected
        # The total is the whole file list, not the page.
        assert body["total_entries"] == len(GIT_BUNDLE_FILES)

    def test_404_for_an_unknown_bundle(self, test_client):
        assert test_client.get("/dagBundles/no_such_bundle/files").status_code == 404

    def test_an_admin_may_read_every_import_error(self, test_client):
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        assert {
            file["relative_fileloc"]: file["can_read_import_error"] for file in body["dag_bundle_files"]
        } == {
            UNREGISTERED_FILE: True,
            REGISTERED_FILE: True,
            SECOND_REGISTERED_FILE: True,
        }

    def test_a_file_with_no_error_has_nothing_to_read(self, test_client):
        body = test_client.get(f"/dagBundles/{LOCAL_BUNDLE}/files").json()

        assert [file["can_read_import_error"] for file in body["dag_bundle_files"]] == [False]

    def test_an_op_may_not_read_the_error_of_a_file_that_registered_no_dag(self, op_headers, test_client):
        """Such errors sit behind ``IMPORT_ERRORS_ALL``, which an op does not hold."""
        body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files", headers=op_headers).json()

        assert {
            file["relative_fileloc"]: file["can_read_import_error"] for file in body["dag_bundle_files"]
        } == {
            UNREGISTERED_FILE: False,
            REGISTERED_FILE: True,
            SECOND_REGISTERED_FILE: True,
        }

    def test_follows_the_dags_the_caller_may_read(self, test_client):
        """
        One readable Dag is enough, even beside an unreadable one, as the error is then redacted.

        ``dag_0.py`` also holds a stale Dag outside the readable set; ``second.py`` holds none
        the caller may read.
        """
        with mock.patch.object(
            test_client.app.state.auth_manager,
            "get_authorized_dag_ids",
            autospec=True,
            return_value={f"dag_in_{GIT_BUNDLE}"},
        ):
            body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {
            file["relative_fileloc"]: file["can_read_import_error"] for file in body["dag_bundle_files"]
        }
        assert by_path[REGISTERED_FILE] is True
        assert by_path[SECOND_REGISTERED_FILE] is False

    def test_no_import_error_is_readable_without_the_import_errors_view(self, test_client):
        auth_manager = test_client.app.state.auth_manager
        real_is_authorized_view = auth_manager.is_authorized_view

        def is_authorized_view(*, access_view, user, team_name=None):
            if access_view is AccessView.IMPORT_ERRORS:
                return False
            return real_is_authorized_view(access_view=access_view, user=user, team_name=team_name)

        with mock.patch.object(
            auth_manager, "is_authorized_view", autospec=True, side_effect=is_authorized_view
        ):
            body = test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        assert [file["can_read_import_error"] for file in body["dag_bundle_files"]] == [False, False, False]
        # The counts are the bundle's and stay put.
        assert [file["import_error_count"] for file in body["dag_bundle_files"]] == [1, 1, 1]
