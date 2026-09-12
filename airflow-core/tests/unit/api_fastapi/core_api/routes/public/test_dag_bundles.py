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

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from fastapi.testclient import TestClient
from itsdangerous import URLSafeSerializer
from sqlalchemy import insert, update

from airflow.api_fastapi.auth.managers.models.resource_details import DagAccessEntity
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
# A file inside GIT_BUNDLE whose Dag the caller may NOT read. Registered, so the admin-gated
# unregistered path cannot hide it -- only the readable-Dag filter excludes it.
UNREADABLE_FILE = "secret.py"

GIT_VERSION = "8f0e5b1c9a2d4e6f8a0b1c2d3e4f5a6b7c8d9e0f"
REFRESHED_AT = datetime(2026, 9, 10, 12, 0, tzinfo=timezone.utc)
PARSED_AT = datetime(2026, 9, 10, 12, 1, tzinfo=timezone.utc)
PARSE_DURATION = 0.125
GONE_REFRESHED_AT = datetime(2026, 9, 1, 8, 30, tzinfo=timezone.utc)

WITH_DAGS = (GIT_BUNDLE, LOCAL_BUNDLE, GONE_BUNDLE, OTHER_TEAM_BUNDLE)
# Everything except the Dag in OTHER_TEAM_BUNDLE.
REMOVED_DAG_ID = "removed_from_dag_0"
READABLE_DAG_IDS = {f"dag_in_{name}" for name in (GIT_BUNDLE, LOCAL_BUNDLE, GONE_BUNDLE)} | {REMOVED_DAG_ID}
# Sorted by name, which is the endpoint's default order.
READABLE_BUNDLES = [GIT_BUNDLE, GONE_BUNDLE, LOCAL_BUNDLE]


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
                # No Dag has ever parsed from this one -- the documented blind spot of deriving
                # visibility from readable Dags.
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
        # A co-located Dag in a visible bundle that the caller cannot read -- deliberately absent
        # from READABLE_DAG_IDS.
        session.add(
            DagModel(
                dag_id="unreadable_in_git_bundle",
                fileloc=UNREADABLE_FILE,
                relative_fileloc=UNREADABLE_FILE,
                bundle_name=GIT_BUNDLE,
                is_paused=False,
                is_stale=False,
            )
        )
        session.add_all(
            [
                # An error in the file the bundle's readable Dag comes from: visible to anyone who
                # can read that Dag.
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=REGISTERED_FILE,
                    stacktrace="boom",
                    timestamp=REFRESHED_AT,
                ),
                # An error in a file that never registered a Dag: admin-gated, because the file's
                # existence is itself the disclosure.
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=UNREGISTERED_FILE,
                    stacktrace="boom before any dag",
                    timestamp=REFRESHED_AT,
                ),
                # Registered to a Dag the caller cannot read: excluded by the readable-Dag filter
                # alone, so it is what makes that half of the authorization testable.
                ParseImportError(
                    bundle_name=GIT_BUNDLE,
                    filename=UNREADABLE_FILE,
                    stacktrace="not yours",
                    timestamp=REFRESHED_AT,
                ),
            ]
        )
        session.commit()

    yield

    _clear()


@pytest.fixture
def dag_scoped_client(test_client):
    """
    A caller who may read every Dag except the one in ``OTHER_TEAM_BUNDLE``.

    The list filter reads the Dag ids from the app's auth manager (``AuthManagerDep``), so the
    real instance is patched rather than the ``get_auth_manager`` module lookup.
    """
    auth_manager = test_client.app.state.auth_manager
    with mock.patch.object(
        auth_manager,
        "get_authorized_dag_ids",
        autospec=True,
        return_value=READABLE_DAG_IDS,
    ):
        yield test_client


@pytest.fixture
def viewer_client(test_client):
    """
    A viewer with the same readable Dags: may read import errors, but not the admin-gated view.

    ``test_client`` authenticates as an admin, which satisfies ``IMPORT_ERRORS_ALL`` under
    SimpleAuthManager and so cannot distinguish the two import-error kinds on its own.
    """
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="viewer", role="viewer"))
    )
    with (
        mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False),
        mock.patch.object(
            auth_manager, "get_authorized_dag_ids", autospec=True, return_value=READABLE_DAG_IDS
        ),
    ):
        yield TestClient(
            test_client.app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=str(test_client.base_url),
        )


class TestGetDagBundles:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get("/dagBundles").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get("/dagBundles").status_code == 403

    def test_lists_bundles_holding_a_readable_dag(self, dag_scoped_client):
        response = dag_scoped_client.get("/dagBundles")

        assert response.status_code == 200
        body = response.json()
        assert [bundle["name"] for bundle in body["dag_bundles"]] == READABLE_BUNDLES
        assert body["total_entries"] == 3

    def test_excludes_a_bundle_whose_dags_are_not_readable(self, dag_scoped_client):
        """The point of the Dag-derived filter: another team's bundle must not appear."""
        body = dag_scoped_client.get("/dagBundles").json()

        assert OTHER_TEAM_BUNDLE not in [bundle["name"] for bundle in body["dag_bundles"]]
        # Absent from the count too, so its existence does not leak through pagination.
        assert body["total_entries"] == 3

    def test_excludes_a_bundle_with_no_dags(self, dag_scoped_client):
        """Documented consequence: a bundle nothing has parsed from yet is not listed."""
        body = dag_scoped_client.get("/dagBundles").json()

        assert DAGLESS_BUNDLE not in [bundle["name"] for bundle in body["dag_bundles"]]

    def test_lists_a_bundle_whose_dags_are_all_stale(self, dag_scoped_client, session):
        """
        The other side of deriving visibility from Dags: deleting them must not hide the bundle.

        A bundle whose Dags have all been removed is exactly when an operator wants to watch it
        still being refreshed, and ``get_authorized_dag_ids`` does not filter on ``is_stale``
        either, so the stale row keeps the bundle visible.
        """
        session.execute(update(DagModel).where(DagModel.bundle_name == GIT_BUNDLE).values(is_stale=True))
        session.commit()

        body = dag_scoped_client.get("/dagBundles").json()

        assert GIT_BUNDLE in [bundle["name"] for bundle in body["dag_bundles"]]

    def test_returns_nothing_when_no_dag_is_readable(self, test_client):
        auth_manager = test_client.app.state.auth_manager
        with mock.patch.object(auth_manager, "get_authorized_dag_ids", autospec=True, return_value=set()):
            body = test_client.get("/dagBundles").json()

        assert body == {"dag_bundles": [], "total_entries": 0}

    def test_versioned_bundle_fields(self, dag_scoped_client):
        body = dag_scoped_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert bundle["version"] == GIT_VERSION
        assert bundle["active"] is True
        assert bundle["last_refreshed"].startswith("2026-09-10T12:00:00")
        # Rendered from the signed template with the stored template params filled in.
        assert bundle["bundle_url"] == f"https://github.com/example/repo/tree/{GIT_VERSION}/dags"
        # The admin client may read import errors for files with no Dag, so it sees both.
        assert bundle["import_error_count"] == 2

    def test_non_versioned_bundle_has_no_version_but_keeps_a_timestamp(self, dag_scoped_client):
        """A bundle that does not support versioning still reports when it was last refreshed."""
        body = dag_scoped_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == LOCAL_BUNDLE)

        assert bundle["version"] is None
        assert bundle["bundle_url"] is None
        assert bundle["last_refreshed"].startswith("2026-09-10T12:00:00")
        assert bundle["import_error_count"] == 0

    def test_reports_a_refresh_that_did_not_change_the_version(self, dag_scoped_client, session):
        """
        The behaviour the whole feature rests on, guarded on the read side.

        A Dag processor bumps ``last_refreshed`` on every successful refresh and leaves ``version``
        alone when the source did not move (``update_bundle_state(..., version=None)``), which is
        what tells an author "a processor is alive and has looked" as distinct from "the code
        changed". The writer side is pinned by
        ``test_manager.py::TestDagFileProcessorManager::test_refresh_dag_bundles_versioned_version_unchanged_calls_update_bundle_state``;
        this pins that the endpoint actually surfaces it.
        """
        before = dag_scoped_client.get("/dagBundles").json()
        was = next(b for b in before["dag_bundles"] if b["name"] == GIT_BUNDLE)

        later = REFRESHED_AT + timedelta(minutes=5)
        session.execute(
            update(DagBundleModel).where(DagBundleModel.name == GIT_BUNDLE).values(last_refreshed=later)
        )
        session.commit()

        after = dag_scoped_client.get("/dagBundles").json()
        now = next(b for b in after["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert now["last_refreshed"] != was["last_refreshed"]
        assert now["last_refreshed"].startswith("2026-09-10T12:05:00")
        assert now["version"] == was["version"] == GIT_VERSION

    def test_deactivated_bundle_is_still_listed(self, dag_scoped_client):
        """A bundle dropped from the config stays visible and flagged, rather than vanishing."""
        body = dag_scoped_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GONE_BUNDLE)

        assert bundle["active"] is False
        assert bundle["version"] == "deadbeef"

    def test_import_error_count_excludes_errors_the_caller_may_not_read(self, viewer_client):
        """
        Count on the same terms as ``GET /importErrors``, not "every row for this bundle".

        Those terms are a role boundary: an error in a file that never registered a Dag needs
        ``IMPORT_ERRORS_ALL``, which is admin-by-default precisely because the file's existence is
        the disclosure. Counting every row would hand that to any viewer, on a polling page.
        """
        body = viewer_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)

        # Only the error in the file whose Dag this viewer can read.
        assert bundle["import_error_count"] == 1

    def test_import_error_count_excludes_files_with_no_readable_dag(self, viewer_client):
        """
        The other half of the authorization: an error in a co-located file whose Dag the caller
        cannot read must not be counted either.

        ``UNREADABLE_FILE`` is deliberately *registered* -- it has a ``DagModel`` row in this very
        bundle -- so the admin-gated unregistered path cannot account for its exclusion. Only the
        ``dag_id IN readable_dag_ids`` restriction does, which makes this the one test that fails
        if that restriction is dropped.
        """
        body = viewer_client.get("/dagBundles").json()
        bundle = next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)

        assert bundle["import_error_count"] == 1

    def test_import_error_count_is_withheld_without_permission(self, dag_scoped_client):
        auth_manager = dag_scoped_client.app.state.auth_manager
        with mock.patch.object(auth_manager, "authorize_view", autospec=True, return_value=False):
            body = dag_scoped_client.get("/dagBundles").json()

        # ``None``, not 0: "you may not see this" must not read as "nothing is wrong".
        assert {bundle["import_error_count"] for bundle in body["dag_bundles"]} == {None}

    @pytest.mark.parametrize(
        "template",
        [
            pytest.param("https://example.com/{0}", id="positional-placeholder"),
            pytest.param("https://example.com/{version.foo}", id="attribute-placeholder"),
        ],
    )
    def test_a_malformed_url_template_does_not_fail_the_collection(
        self, dag_scoped_client, template, session
    ):
        """``render_url`` guards only KeyError/ValueError, so a bad template must degrade to null."""
        bundle = session.get(DagBundleModel, GIT_BUNDLE)
        bundle.signed_url_template = _sign(template, GIT_BUNDLE)
        session.commit()

        response = dag_scoped_client.get("/dagBundles")

        assert response.status_code == 200
        body = response.json()
        assert next(b for b in body["dag_bundles"] if b["name"] == GIT_BUNDLE)["bundle_url"] is None
        # Every other row is unaffected.
        assert body["total_entries"] == 3

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param({"limit": 2}, [GIT_BUNDLE, GONE_BUNDLE], id="limit"),
            pytest.param({"limit": 2, "offset": 2}, [LOCAL_BUNDLE], id="offset"),
            pytest.param({"order_by": "-name"}, [LOCAL_BUNDLE, GONE_BUNDLE, GIT_BUNDLE], id="name-desc"),
            pytest.param(
                {"order_by": "last_refreshed"},
                [GONE_BUNDLE, GIT_BUNDLE, LOCAL_BUNDLE],
                id="last-refreshed-asc",
            ),
        ],
    )
    def test_pagination_and_sorting(self, dag_scoped_client, params, expected):
        response = dag_scoped_client.get("/dagBundles", params=params)

        assert response.status_code == 200
        body = response.json()
        assert [bundle["name"] for bundle in body["dag_bundles"]] == expected
        # total_entries counts the whole permitted set, not the page.
        assert body["total_entries"] == 3

    def test_rejects_an_unsupported_sort_column(self, dag_scoped_client):
        response = dag_scoped_client.get("/dagBundles", params={"order_by": "signed_url_template"})

        assert response.status_code == 400

    def test_never_exposes_the_signed_url_template(self, dag_scoped_client):
        """The template is signed with the fernet key; only the rendered url may leave."""
        body = dag_scoped_client.get("/dagBundles").json()

        for bundle in body["dag_bundles"]:
            assert "signed_url_template" not in bundle
            assert "template_params" not in bundle

    @conf_vars({("core", "multi_team"): "False"})
    def test_team_name_is_null_when_multi_team_is_off(self, dag_scoped_client):
        body = dag_scoped_client.get("/dagBundles").json()

        assert {bundle["team_name"] for bundle in body["dag_bundles"]} == {None}

    @conf_vars({("core", "multi_team"): "True"})
    def test_reports_the_owning_team_in_multi_team_mode(self, dag_scoped_client, session):
        """
        Exercise the branch that reads teams at all.

        Without ``multi_team`` on -- which is the shipped default, and so what every other test
        here runs under -- the ``get_team_names`` call is skipped entirely, leaving the batched
        read that exists to avoid a per-row lazy load uncovered.
        """
        session.add(Team(name=TEAM_NAME))
        session.commit()
        session.execute(
            insert(dag_bundle_team_association_table).values(dag_bundle_name=GIT_BUNDLE, team_name=TEAM_NAME)
        )
        session.commit()

        body = dag_scoped_client.get("/dagBundles").json()

        by_name = {bundle["name"]: bundle for bundle in body["dag_bundles"]}
        assert by_name[GIT_BUNDLE]["team_name"] == TEAM_NAME
        # Bundles with no team mapping still report null rather than inheriting one.
        assert by_name[LOCAL_BUNDLE]["team_name"] is None


class TestGetDagBundle:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"/dagBundles/{GIT_BUNDLE}").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get(f"/dagBundles/{GIT_BUNDLE}").status_code == 403

    def test_returns_the_bundle(self, dag_scoped_client):
        response = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}")

        assert response.status_code == 200
        body = response.json()
        assert body["name"] == GIT_BUNDLE
        assert body["version"] == GIT_VERSION
        assert body["last_refreshed"] == "2026-09-10T12:00:00Z"
        assert body["active"] is True
        assert body["bundle_url"] == f"https://github.com/example/repo/tree/{GIT_VERSION}/dags"

    def test_dag_count_excludes_dags_the_caller_may_not_read(self, dag_scoped_client):
        """``GIT_BUNDLE`` holds two Dags, one of them absent from ``READABLE_DAG_IDS``."""
        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["dag_count"] == 1

    def test_404_for_a_bundle_whose_dags_are_not_readable(self, dag_scoped_client):
        """A 404 rather than a 403, so the response does not confirm the bundle exists."""
        assert dag_scoped_client.get(f"/dagBundles/{OTHER_TEAM_BUNDLE}").status_code == 404

    def test_404_for_a_bundle_with_no_dags(self, dag_scoped_client):
        assert dag_scoped_client.get(f"/dagBundles/{DAGLESS_BUNDLE}").status_code == 404

    def test_404_for_an_unknown_bundle(self, dag_scoped_client):
        assert dag_scoped_client.get("/dagBundles/no_such_bundle").status_code == 404

    def test_import_error_count_is_gated_like_the_collection(self, dag_scoped_client, viewer_client):
        """
        The admin sees the unregistered-file error as well; the viewer sees only the registered one.

        Same two-part authorization as ``GET /importErrors``, asserted here so the detail route
        cannot drift from the collection route it shares a helper with.
        """
        assert dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()["import_error_count"] == 2
        assert viewer_client.get(f"/dagBundles/{GIT_BUNDLE}").json()["import_error_count"] == 1

    def test_import_error_count_is_withheld_without_permission(self, dag_scoped_client):
        auth_manager = dag_scoped_client.app.state.auth_manager
        with mock.patch.object(auth_manager, "authorize_view", autospec=True, return_value=False):
            body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["import_error_count"] is None

    @conf_vars({("core", "multi_team"): "True"})
    def test_reports_the_owning_team_in_multi_team_mode(self, dag_scoped_client, session):
        """Without ``multi_team`` on -- the shipped default -- the team lookup is skipped entirely."""
        session.add(Team(name=TEAM_NAME))
        session.commit()
        session.execute(
            insert(dag_bundle_team_association_table).values(dag_bundle_name=GIT_BUNDLE, team_name=TEAM_NAME)
        )
        session.commit()

        assert dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()["team_name"] == TEAM_NAME

    def test_bundle_url_is_withheld_without_dag_version_read(self, dag_scoped_client):
        """
        A rendered bundle url is otherwise only reachable through ``GET /dags/{dag_id}/dagVersions``.

        That route additionally requires Dag *version* read, so a role that grants Dag read and
        withholds version read must not get the repository address here instead.
        """
        auth_manager = dag_scoped_client.app.state.auth_manager
        real = auth_manager.is_authorized_dag

        def _deny_versions(*args, **kwargs):
            if kwargs.get("access_entity") is DagAccessEntity.VERSION:
                return False
            return real(*args, **kwargs)

        with mock.patch.object(auth_manager, "is_authorized_dag", autospec=True, side_effect=_deny_versions):
            body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["version"] == GIT_VERSION
        assert body["bundle_url"] is None

    def test_dag_count_excludes_stale_dags(self, dag_scoped_client, session):
        """``GIT_BUNDLE``'s Dags all go stale, so nothing live is left to count."""
        session.execute(update(DagModel).where(DagModel.bundle_name == GIT_BUNDLE).values(is_stale=True))
        session.commit()

        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}").json()

        assert body["dag_count"] == 0


class TestGetDagBundleFiles:
    def test_should_raise_401_unauthenticated(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").status_code == 401

    def test_should_raise_403_unauthorized(self, unauthorized_test_client):
        assert unauthorized_test_client.get(f"/dagBundles/{GIT_BUNDLE}/files").status_code == 403

    def test_lists_files_ordered_by_path(self, dag_scoped_client):
        response = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files")

        assert response.status_code == 200
        body = response.json()
        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == [
            UNREGISTERED_FILE,
            REGISTERED_FILE,
        ]
        assert body["total_entries"] == 2

    def test_reports_parse_time_and_duration(self, dag_scoped_client):
        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["dag_count"] == 1
        assert by_path[REGISTERED_FILE]["last_parsed_time"] == "2026-09-10T12:01:00Z"
        assert by_path[REGISTERED_FILE]["last_parse_duration"] == PARSE_DURATION

    def test_a_file_that_registered_no_dag_is_listed_with_its_error(self, dag_scoped_client):
        """
        The case the page most needs to show: a file that failed before defining a Dag.

        It has no Dag to authorize on, so it is admin-gated, and without it a brand new broken
        file would be invisible on the very page someone opens to find out why.
        """
        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[UNREGISTERED_FILE]["dag_count"] == 0
        assert by_path[UNREGISTERED_FILE]["import_error_count"] == 1
        assert by_path[UNREGISTERED_FILE]["last_parsed_time"] is None
        assert by_path[UNREGISTERED_FILE]["last_parse_duration"] is None

    def test_excludes_a_file_whose_dag_is_not_readable(self, dag_scoped_client, viewer_client):
        """``UNREADABLE_FILE`` is registered, so only the readable-Dag filter keeps it out."""
        for client in (dag_scoped_client, viewer_client):
            body = client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()
            assert UNREADABLE_FILE not in {file["relative_fileloc"] for file in body["dag_bundle_files"]}

    def test_viewer_does_not_see_the_unregistered_file(self, viewer_client):
        body = viewer_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == [REGISTERED_FILE]
        assert body["total_entries"] == 1

    def test_import_error_count_is_withheld_without_permission(self, dag_scoped_client):
        """``None``, not 0: "you may not see this" must not read as "nothing is wrong"."""
        auth_manager = dag_scoped_client.app.state.auth_manager
        with mock.patch.object(auth_manager, "authorize_view", autospec=True, return_value=False):
            body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == [REGISTERED_FILE]
        assert body["dag_bundle_files"][0]["import_error_count"] is None

    def test_a_file_whose_dags_went_stale_stays_listed_while_it_has_an_error(
        self, dag_scoped_client, session
    ):
        """Marking a file's Dags stale must not hide the row that explains the breakage."""
        session.execute(
            update(DagModel)
            .where(DagModel.relative_fileloc == REGISTERED_FILE, DagModel.bundle_name == GIT_BUNDLE)
            .values(is_stale=True)
        )
        session.commit()

        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["dag_count"] == 0
        assert by_path[REGISTERED_FILE]["import_error_count"] == 1

    def test_parse_duration_ignores_a_stale_dag_from_an_older_parse(self, dag_scoped_client):
        """
        A Dag removed from a file keeps its row, frozen at the parse it was last seen in.

        Aggregating the two columns independently would pair that older, slower duration with the
        newer timestamp and report a parse time that never happened.
        """
        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files").json()

        by_path = {file["relative_fileloc"]: file for file in body["dag_bundle_files"]}
        assert by_path[REGISTERED_FILE]["last_parsed_time"] == "2026-09-10T12:01:00Z"
        assert by_path[REGISTERED_FILE]["last_parse_duration"] == PARSE_DURATION

    def test_a_file_with_no_live_dag_and_no_error_is_dropped(self, dag_scoped_client, session):
        """A file whose Dags are all stale and which has no error is one the bundle no longer has."""
        session.execute(update(DagModel).where(DagModel.bundle_name == LOCAL_BUNDLE).values(is_stale=True))
        session.commit()

        body = dag_scoped_client.get(f"/dagBundles/{LOCAL_BUNDLE}/files").json()

        assert body["dag_bundle_files"] == []
        assert body["total_entries"] == 0

    def test_a_file_with_no_error_reports_zero(self, dag_scoped_client):
        body = dag_scoped_client.get(f"/dagBundles/{LOCAL_BUNDLE}/files").json()

        assert [file["import_error_count"] for file in body["dag_bundle_files"]] == [0]

    @pytest.mark.parametrize(
        ("params", "expected"),
        [
            pytest.param({"limit": 1}, [UNREGISTERED_FILE], id="limit"),
            pytest.param({"offset": 1}, [REGISTERED_FILE], id="offset"),
            pytest.param({"limit": 1, "offset": 1}, [REGISTERED_FILE], id="limit-and-offset"),
            # ``limit`` is a non-negative int, and ``Select.limit(0)`` returns nothing, so zero
            # has to mean nothing here too rather than being read as "unlimited".
            pytest.param({"limit": 0}, [], id="limit-zero"),
        ],
    )
    def test_pagination(self, dag_scoped_client, params, expected):
        body = dag_scoped_client.get(f"/dagBundles/{GIT_BUNDLE}/files", params=params).json()

        assert [file["relative_fileloc"] for file in body["dag_bundle_files"]] == expected
        # The total is the whole file list, not the page.
        assert body["total_entries"] == 2

    def test_404_for_a_bundle_whose_dags_are_not_readable(self, dag_scoped_client):
        assert dag_scoped_client.get(f"/dagBundles/{OTHER_TEAM_BUNDLE}/files").status_code == 404

    def test_404_for_an_unknown_bundle(self, dag_scoped_client):
        assert dag_scoped_client.get("/dagBundles/no_such_bundle/files").status_code == 404
