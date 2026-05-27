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

import contextlib
import json
import os
from contextlib import nullcontext
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy import func, select, update

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager, _best_bundle_for_fileloc
from airflow.exceptions import AirflowConfigException
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError
from airflow.utils.session import create_session

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        pytest.param(None, {"dags-folder"}, id="default"),
        pytest.param("{}", set(), id="empty dict"),
        pytest.param(
            "[]",
            set(),
            id="empty list",
        ),
        pytest.param(
            json.dumps(
                [
                    {
                        "name": "my-bundle",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": "/tmp/hihi", "refresh_interval": 1},
                    }
                ]
            ),
            {"my-bundle"},
            id="remove_dags_folder_default_add_bundle",
        ),
        pytest.param(
            json.dumps(
                [
                    {
                        "name": "my-bundle",
                        "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                        "kwargs": {"path": "/tmp/hihi", "refresh_interval": 1},
                        "team_name": "test",
                    }
                ]
            ),
            "cannot have a team name when multi-team mode is disabled.",
            id="add_bundle_with_team",
        ),
        pytest.param(
            "[]",
            set(),
            id="remove_dags_folder_default",
        ),
        pytest.param("1", "key `dag_bundle_config_list` must be list", id="int"),
        pytest.param("abc", "Unable to parse .* as valid json", id="not_json"),
    ],
)
def test_parse_bundle_config(value, expected):
    """Test that bundle_configs are read from configuration."""
    envs = {"AIRFLOW__CORE__LOAD_EXAMPLES": "False"}
    if value:
        envs["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = value
    cm = nullcontext()
    exp_fail = False
    if isinstance(expected, str):
        exp_fail = True
        cm = pytest.raises(AirflowConfigException, match=expected)

    with patch.dict(os.environ, envs), cm:
        bundle_manager = DagBundlesManager()
        names = set(x.name for x in bundle_manager.get_all_dag_bundles())

    if not exp_fail:
        assert names == expected


class BasicBundle(BaseDagBundle):
    def refresh(self):
        pass

    def get_current_version(self):
        pass

    @property
    def path(self) -> Path:
        return Path("/__basic_bundle_unmatched__")


BASIC_BUNDLE_CONFIG = [
    {
        "name": "my-test-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]
SECOND_BUNDLE_CONFIG = [
    {
        "name": "second-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]


def test_get_bundle():
    """Test that get_bundle builds and returns a bundle."""
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        bundle_manager = DagBundlesManager()

        with pytest.raises(ValueError, match="'bundle-that-doesn't-exist' is not configured"):
            bundle_manager.get_bundle(name="bundle-that-doesn't-exist", version="hello")
        bundle = bundle_manager.get_bundle(name="my-test-bundle", version="hello")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version == "hello"
    assert bundle.refresh_interval == 1

    # And none for version also works!
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        bundle = bundle_manager.get_bundle(name="my-test-bundle")
    assert isinstance(bundle, BasicBundle)
    assert bundle.name == "my-test-bundle"
    assert bundle.version is None


@pytest.fixture
def clear_db():
    clear_db_dag_bundles()
    yield
    clear_db_dag_bundles()


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_bundles_to_db(clear_db, session):
    def _get_bundle_names_and_active():
        return session.execute(
            select(DagBundleModel.name, DagBundleModel.active).order_by(DagBundleModel.name)
        ).all()

    # Initial add
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [("my-test-bundle", True)]

    session.add(
        ParseImportError(
            bundle_name="my-test-bundle",  # simulate import error for this bundle
            filename="some_file.py",
            stacktrace="some error",
        )
    )
    session.flush()

    # simulate bundle config change (now 'dags-folder' is active, 'my-test-bundle' becomes inactive)
    manager = DagBundlesManager()
    manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [
        ("dags-folder", True),
        ("my-test-bundle", False),
    ]
    # Since my-test-bundle is inactive, the associated import errors should be deleted
    assert session.scalar(select(func.count(ParseImportError.id))) == 0

    # Re-enable one that reappears in config
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
    assert _get_bundle_names_and_active() == [
        ("dags-folder", False),
        ("my-test-bundle", True),
    ]


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_bundles_to_db_does_not_log_removing_none_team(clear_db, caplog):
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

        caplog.clear()
        caplog.set_level("WARNING")

        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    assert "Removing ownership of team 'None'" not in caplog.text


@conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(BASIC_BUNDLE_CONFIG)})
@pytest.mark.parametrize("version", [None, "hello"])
def test_view_url(version):
    """Test that view_url calls the bundle's view_url method."""
    bundle_manager = DagBundlesManager()
    with patch.object(BaseDagBundle, "view_url") as view_url_mock:
        # Test that deprecation warning is raised
        with pytest.warns(DeprecationWarning, match="'view_url' method is deprecated"):
            bundle_manager.view_url("my-test-bundle", version=version)
    view_url_mock.assert_called_once_with(version=version)


class BundleWithTemplate(BaseDagBundle):
    """Test bundle that provides a URL template."""

    def __init__(self, *, subdir: str | None = None, **kwargs):
        super().__init__(**kwargs)
        self.subdir = subdir

    def refresh(self):
        pass

    def get_current_version(self):
        return "v1.0"

    @property
    def path(self):
        return "/tmp/test"


TEMPLATE_BUNDLE_CONFIG = [
    {
        "name": "template-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
        "kwargs": {
            "view_url_template": "https://github.com/example/repo/tree/{version}/{subdir}",
            "subdir": "dags",
            "refresh_interval": 1,
        },
    }
]


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_bundles_to_db_with_template(clear_db, session):
    """Test that URL templates and parameters are stored in the database during sync."""
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    # Check that the template and parameters were stored
    bundle_model = session.scalars(select(DagBundleModel).filter_by(name="template-bundle").limit(1)).first()

    session.merge(bundle_model)

    assert bundle_model is not None
    assert bundle_model.render_url(version="v1.0") == "https://github.com/example/repo/tree/v1.0/dags"
    assert bundle_model.template_params == {"subdir": "dags"}
    assert bundle_model.active is True


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_bundle_model_render_url(clear_db, session):
    """Test the DagBundleModel render_url method."""
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()
        bundle_model = session.scalars(
            select(DagBundleModel).filter_by(name="template-bundle").limit(1)
        ).first()

        session.merge(bundle_model)
        assert bundle_model is not None

        url = bundle_model.render_url(version="main")
        assert url == "https://github.com/example/repo/tree/main/dags"
        url = bundle_model.render_url()
        assert url == "https://github.com/example/repo/tree/None/dags"


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_template_params_update_on_sync(clear_db, session):
    """Test that template parameters are updated when bundle configuration changes."""
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    # Verify initial template and parameters
    bundle_model = session.scalars(select(DagBundleModel).filter_by(name="template-bundle").limit(1)).first()
    url = bundle_model._unsign_url()
    assert url == "https://github.com/example/repo/tree/{version}/{subdir}"
    assert bundle_model.template_params == {"subdir": "dags"}

    # Update the bundle config with different parameters
    updated_config = [
        {
            "name": "template-bundle",
            "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
            "kwargs": {
                "view_url_template": "https://gitlab.com/example/repo/-/tree/{version}/{subdir}",
                "subdir": "workflows",
                "refresh_interval": 1,
            },
        }
    ]

    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(updated_config)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    # Verify the template and parameters were updated
    bundle_model = session.scalars(select(DagBundleModel).filter_by(name="template-bundle").limit(1)).first()
    url = bundle_model._unsign_url()
    assert url == "https://gitlab.com/example/repo/-/tree/{version}/{subdir}"
    assert bundle_model.template_params == {"subdir": "workflows"}
    assert bundle_model.render_url(version="v1") == "https://gitlab.com/example/repo/-/tree/v1/workflows"


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_template_update_on_sync(clear_db, session):
    """Test that templates are updated when bundle configuration changes."""
    # First, sync with initial template
    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(TEMPLATE_BUNDLE_CONFIG)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    # Verify initial template
    bundle_model = session.scalars(select(DagBundleModel).filter_by(name="template-bundle").limit(1)).first()
    url = bundle_model._unsign_url()
    assert url == "https://github.com/example/repo/tree/{version}/{subdir}"
    assert bundle_model.render_url(version="v1") == "https://github.com/example/repo/tree/v1/dags"

    # Update the bundle config with a different template
    updated_config = [
        {
            "name": "template-bundle",
            "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BundleWithTemplate",
            "kwargs": {
                "view_url_template": "https://gitlab.com/example/repo/-/tree/{version}/{subdir}",
                "subdir": "dags",
                "refresh_interval": 1,
            },
        }
    ]

    with patch.dict(
        os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(updated_config)}
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

    # Verify the template was updated
    bundle_model = session.scalars(select(DagBundleModel).filter_by(name="template-bundle").limit(1)).first()
    url = bundle_model._unsign_url()
    assert url == "https://gitlab.com/example/repo/-/tree/{version}/{subdir}"
    assert bundle_model.render_url("v1") == "https://gitlab.com/example/repo/-/tree/v1/dags"


def test_dag_bundle_model_render_url_with_invalid_template():
    """Test that DagBundleModel.render_url handles invalid templates gracefully."""
    bundle_model = DagBundleModel(name="test-bundle")
    bundle_model.signed_url_template = "https://github.com/example/repo/tree/{invalid_placeholder}"
    bundle_model.template_params = {"subdir": "dags"}

    # Should return None if rendering fails
    url = bundle_model.render_url("v1")
    assert url is None


def test_example_dags_bundle_added():
    manager = DagBundlesManager()
    manager.parse_config()
    assert "example_dags" in manager._bundle_config

    with conf_vars({("core", "LOAD_EXAMPLES"): "False"}):
        manager = DagBundlesManager()
        manager.parse_config()
        assert "example_dags" not in manager._bundle_config


def test_example_dags_name_is_reserved():
    reserved_name_config = [{"name": "example_dags", "classpath": "yo face", "kwargs": {}}]
    with conf_vars({("dag_processor", "dag_bundle_config_list"): json.dumps(reserved_name_config)}):
        with pytest.raises(AirflowConfigException, match="Bundle name 'example_dags' is a reserved name."):
            DagBundlesManager().parse_config()


class FailingBundle(BaseDagBundle):
    """Test bundle that raises an exception during initialization."""

    def __init__(self, *, should_fail: bool = True, **kwargs):
        super().__init__(**kwargs)
        if should_fail:
            raise ValueError("Bundle creation failed for testing")

    def refresh(self):
        pass

    def get_current_version(self):
        return None

    @property
    def path(self):
        return "/tmp/failing"


FAILING_BUNDLE_CONFIG = [
    {
        "name": "failing-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.FailingBundle",
        "kwargs": {"should_fail": True, "refresh_interval": 1},
    }
]


@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
@pytest.mark.db_test
def test_multiple_bundles_one_fails(clear_db, session):
    """Test that when one bundle fails to create, other bundles still load successfully."""
    mix_config = BASIC_BUNDLE_CONFIG + FAILING_BUNDLE_CONFIG

    with patch.dict(os.environ, {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(mix_config)}):
        manager = DagBundlesManager()

        bundles = list(manager.get_all_dag_bundles())
        assert len(bundles) == 1
        assert bundles[0].name == "my-test-bundle"
        assert isinstance(bundles[0], BasicBundle)

        manager.sync_bundles_to_db()
        bundle_names = {b.name for b in session.scalars(select(DagBundleModel)).all()}
        assert bundle_names == {"my-test-bundle"}


def test_get_all_bundle_names():
    assert DagBundlesManager().get_all_bundle_names() == ["dags-folder", "example_dags"]


@pytest.fixture
def clear_dags_and_bundles():
    clear_db_dags()
    clear_db_dag_bundles()
    yield
    clear_db_dags()
    clear_db_dag_bundles()


def _add_dag(session, dag_id: str, bundle_name: str) -> DagModel:
    dag = DagModel(dag_id=dag_id, bundle_name=bundle_name, fileloc=f"/tmp/{dag_id}.py")
    session.add(dag)
    session.flush()
    return dag


class TestBestBundleForFileloc:
    """Tests for ``_best_bundle_for_fileloc`` path normalisation and safety."""

    def test_returns_relative_path_for_match(self) -> None:
        assert _best_bundle_for_fileloc("/dags/team_x/dag.py", {"team-x": Path("/dags/team_x")}) == (
            "team-x",
            "dag.py",
        )

    def test_returns_none_for_no_match(self) -> None:
        assert _best_bundle_for_fileloc("/elsewhere/dag.py", {"team-x": Path("/dags/team_x")}) is None

    def test_returns_none_for_empty_paths(self) -> None:
        assert _best_bundle_for_fileloc("/dags/dag.py", {}) is None

    @pytest.mark.parametrize(
        "fileloc",
        [
            pytest.param("/dags/foo/../../outside.py", id="parent_traversal_escapes_root"),
            pytest.param("/dags/../outside.py", id="parent_traversal_at_root"),
            pytest.param("/dags/../../etc/passwd", id="multiple_parent_traversal"),
        ],
    )
    def test_rejects_parent_traversal_filelocs(self, fileloc: str) -> None:
        """A fileloc with ``..`` segments that escape the bundle root must not match.

        Without lexical normalisation, ``Path.relative_to`` on an unnormalised
        path with ``..`` segments returns a relative path like
        ``foo/../../outside.py`` that, when later joined with the bundle root,
        addresses files outside it. Normalising both sides collapses the
        traversal so the fileloc is no longer under the bundle and the helper
        returns ``None``.
        """
        assert _best_bundle_for_fileloc(fileloc, {"dags": Path("/dags")}) is None

    def test_normalises_redundant_separators_and_dots(self) -> None:
        assert _best_bundle_for_fileloc("/dags//team_x/./dag.py", {"team-x": Path("/dags/team_x")}) == (
            "team-x",
            "dag.py",
        )


@pytest.mark.db_test
class TestReassignDagsWithUnconfiguredBundles:
    """Tests for DagBundlesManager.reassign_dags_with_unconfigured_bundles."""

    def _manager_with_bundle_names(self, names: list[str]) -> DagBundlesManager:
        """Return a manager whose ``_resolve_active_bundle_paths`` reports *names* with non-matching paths.

        The fake paths are absolute roots that cannot contain any
        ``/tmp/{dag_id}.py`` test fileloc, so rows are routed as unmatched
        unless a test explicitly arranges otherwise.
        """
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
        paths = {name: Path(f"/__unmatched_for_test__/{name}") for name in names}
        manager._resolve_active_bundle_paths = lambda *, session: paths  # type: ignore[method-assign]
        return manager

    def test_no_configured_bundles_is_noop(self, clear_dags_and_bundles, session):
        """Return 0 without raising when no bundles are configured."""
        manager = self._manager_with_bundle_names([])
        assert manager.reassign_dags_with_unconfigured_bundles() == 0

    def test_already_configured_is_noop(self, clear_dags_and_bundles, session) -> None:
        """No reassignment when every Dag already points at a configured bundle."""
        bundle = DagBundleModel(name="bundle-a")
        bundle.active = True
        session.add(bundle)
        session.flush()
        _add_dag(session, "dag-1", "bundle-a")
        session.commit()

        manager = self._manager_with_bundle_names(["bundle-a"])
        assert manager.reassign_dags_with_unconfigured_bundles() == 0
        assert session.get(DagModel, "dag-1").bundle_name == "bundle-a"

    def test_unmatched_fileloc_leaves_row_untouched(self, clear_dags_and_bundles, session, caplog) -> None:
        """Rows whose fileloc has no configured bundle path keep their original bundle.

        The fallback that wrote ``bundle_name`` without a verified
        ``relative_fileloc`` produced active-but-un-runnable rows, so the
        helper now leaves such rows on their unconfigured bundle and emits a
        single warning naming the count.
        """
        active = DagBundleModel(name="active")
        active.active = True
        removed = DagBundleModel(name="removed-bundle")
        removed.active = False
        session.add(active)
        session.add(removed)
        session.flush()
        _add_dag(session, "dag-1", "removed-bundle")
        _add_dag(session, "dag-2", "removed-bundle")
        session.commit()

        manager = self._manager_with_bundle_names(["active"])
        with caplog.at_level("WARNING", logger="airflow.dag_processing.bundles.manager.DagBundlesManager"):
            assert manager.reassign_dags_with_unconfigured_bundles() == 0

        session.expire_all()
        for dag_id in ("dag-1", "dag-2"):
            row = session.get(DagModel, dag_id)
            assert row.bundle_name == "removed-bundle"
            assert row.relative_fileloc is None
        assert any("Skipped 2 legacy Dag(s)" in record.message for record in caplog.records)

    def test_row_with_populated_relative_fileloc_is_left_alone(self, clear_dags_and_bundles, session) -> None:
        """A 3.x row whose bundle is no longer configured must keep its bundle assignment.

        Only rows the 0082 migration touched (``relative_fileloc IS NULL``) are
        candidates for reassignment. Rows that already carry a relative path
        were written by 3.x serialization and must be left on their bundle so
        the regular stale-Dag deactivation path can handle a removed bundle.
        """
        active_bundle = DagBundleModel(name="active")
        active_bundle.active = True
        removed_bundle = DagBundleModel(name="removed-bundle")
        removed_bundle.active = False
        session.add(active_bundle)
        session.add(removed_bundle)
        session.flush()

        dag = DagModel(
            dag_id="dag-1",
            bundle_name="removed-bundle",
            fileloc="/tmp/dag-1.py",
        )
        dag.relative_fileloc = "dag-1.py"
        dag.bundle_version = "abc123"
        session.add(dag)
        session.flush()
        session.commit()

        manager = self._manager_with_bundle_names(["active"])
        assert manager.reassign_dags_with_unconfigured_bundles() == 0

        session.expire_all()
        refreshed = session.get(DagModel, "dag-1")
        assert refreshed.bundle_name == "removed-bundle"
        assert refreshed.relative_fileloc == "dag-1.py"
        assert refreshed.bundle_version == "abc123"

    def test_row_with_existing_dag_version_is_left_alone(self, clear_dags_and_bundles, session) -> None:
        """A Dag with any DagVersion row must not be reassigned.

        Defended at two layers: (1) the global DagVersion-existence fast-skip
        short-circuits before any work, and (2) the per-row predicate
        ``NOT EXISTS DagVersion`` excludes the row even if the fast-skip is
        bypassed. The parse path is the source of truth for any Dag with a
        DagVersion -- touching only the DagModel would leave the DagVersion
        stale, and scheduler/executor paths prefer DagVersion.bundle_name
        when building task workloads.
        """
        from airflow.models.dag_version import DagVersion

        active = DagBundleModel(name="active")
        active.active = True
        removed = DagBundleModel(name="removed-bundle")
        removed.active = False
        session.add(active)
        session.add(removed)
        session.flush()

        # NULL relative_fileloc would normally make this a repair candidate;
        # the DagVersion row should exclude it from the predicate.
        dag = DagModel(
            dag_id="versioned",
            bundle_name="removed-bundle",
            fileloc="/tmp/versioned.py",
        )
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()

        version = DagVersion(
            dag_id="versioned",
            version_number=1,
            bundle_name="removed-bundle",
            bundle_version="v1",
        )
        session.add(version)
        session.flush()
        session.commit()

        manager = self._manager_with_bundle_names(["active"])
        assert manager.reassign_dags_with_unconfigured_bundles() == 0

        session.expire_all()
        refreshed = session.get(DagModel, "versioned")
        assert refreshed.bundle_name == "removed-bundle"
        assert refreshed.relative_fileloc is None
        refreshed_version = session.get(DagVersion, version.id)
        assert refreshed_version.bundle_name == "removed-bundle"
        assert refreshed_version.bundle_version == "v1"

    @conf_vars({("core", "multi_team"): "True"})
    def test_runs_under_multi_team_mode(self, clear_dags_and_bundles, session, tmp_path) -> None:
        """Multi-team mode still repairs legacy rows.

        Each team's bundle owns a distinct on-disk path, so routing a legacy
        row to the most-specific bundle whose path contains its ``fileloc``
        cannot cross a team boundary. The repair therefore runs unchanged
        under ``core.multi_team`` and must not blanket-skip rows that have a
        safe target.
        """
        bundle_dir = tmp_path / "team_x"
        bundle_dir.mkdir()
        legacy_file = bundle_dir / "legacy.py"
        legacy_file.write_text("# legacy dag")

        config = [
            {
                "name": "team-x-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        active_bundle = DagBundleModel(name="team-x-bundle")
        active_bundle.active = True
        removed_bundle = DagBundleModel(name="removed-bundle")
        removed_bundle.active = False
        session.add(active_bundle)
        session.add(removed_bundle)
        session.flush()

        dag = DagModel(dag_id="legacy", bundle_name="removed-bundle", fileloc=str(legacy_file))
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()

        assert count == 1
        session.expire_all()
        refreshed = session.get(DagModel, "legacy")
        assert refreshed.bundle_name == "team-x-bundle"
        assert refreshed.relative_fileloc == "legacy.py"


@pytest.mark.db_test
class TestBackfillRelativeFileloc:
    """Tests for the legacy ``relative_fileloc`` backfill triggered by reassignment."""

    @pytest.mark.parametrize(
        ("fileloc_under_bundle", "expected_bundle_name", "expected_relative_fileloc"),
        [
            pytest.param(True, "my-bundle", "legacy.py", id="fileloc_under_bundle_path_is_backfilled"),
            pytest.param(False, "orphan-bundle", None, id="fileloc_outside_bundle_path_is_left_alone"),
        ],
    )
    def test_backfill_behavior(
        self,
        clear_dags_and_bundles,
        session,
        tmp_path,
        fileloc_under_bundle: bool,
        expected_bundle_name: str,
        expected_relative_fileloc: str | None,
    ) -> None:
        """Reassignment only happens when ``fileloc`` lies under a configured bundle path.

        When the fileloc matches, both ``bundle_name`` and ``relative_fileloc``
        are written atomically. When it does not match, the row is left on its
        unconfigured bundle so it cannot become an active-but-un-runnable row.

        :param fileloc_under_bundle: Whether the legacy Dag's absolute ``fileloc`` lies under
            the configured bundle's path.
        :param expected_bundle_name: Expected ``bundle_name`` after reassignment.
        :param expected_relative_fileloc: Expected ``relative_fileloc`` after reassignment.
        """
        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        legacy_file_under_bundle = bundle_dir / "legacy.py"
        legacy_file_under_bundle.write_text("# legacy dag")
        legacy_fileloc = str(legacy_file_under_bundle) if fileloc_under_bundle else "/elsewhere/foo.py"

        config = [
            {
                "name": "my-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        active_bundle = DagBundleModel(name="my-bundle")
        active_bundle.active = True
        orphan_bundle = DagBundleModel(name="orphan-bundle")
        orphan_bundle.active = False
        session.add(active_bundle)
        session.add(orphan_bundle)
        session.flush()

        dag = DagModel(dag_id="legacy", bundle_name="orphan-bundle", fileloc=legacy_fileloc)
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            manager.reassign_dags_with_unconfigured_bundles()

        session.expire_all()
        refreshed = session.get(DagModel, "legacy")
        assert refreshed.bundle_name == expected_bundle_name
        assert refreshed.relative_fileloc == expected_relative_fileloc

    def test_post_2x_to_3x_migration_with_renamed_bundle(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """End-to-end: 2.x→3.x upgrade where the operator's bundle is not named ``dags-folder``.

        Reproduces the original incident: the migration sets ``bundle_name='dags-folder'`` on
        legacy Dag rows and leaves ``relative_fileloc`` NULL, but the operator has configured a
        single LocalDagBundle named ``custom-bundle`` pointing at the same on-disk dags folder.
        After ``reassign_dags_with_unconfigured_bundles`` runs at DFP startup, the legacy Dags
        should be:

        1. Reassigned to ``custom-bundle`` so triggering DagRuns no longer fails with
           "Requested bundle 'dags-folder' is not configured."
        2. Have ``relative_fileloc`` backfilled from ``fileloc`` so the standard fileloc-based
           stale-detection path can later detect real deletions.
        """
        dags_folder = tmp_path / "dags"
        dags_folder.mkdir()
        legacy_file = dags_folder / "my_dag.py"
        legacy_file.write_text("# 2.x dag")

        config = [
            {
                "name": "custom-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(dags_folder), "refresh_interval": 1},
            }
        ]
        # State right after the 0082 migration: ``dags-folder`` row exists in dag_bundle (added
        # by migration backfill) but is not in the user's config; the user's ``custom-bundle``
        # has not been registered yet (sync_bundles_to_db does that on startup).
        legacy_default_bundle = DagBundleModel(name="dags-folder")
        legacy_default_bundle.active = True
        session.add(legacy_default_bundle)
        session.flush()

        legacy_dag = DagModel(
            dag_id="legacy_dag",
            bundle_name="dags-folder",
            fileloc=str(legacy_file),
        )
        legacy_dag.relative_fileloc = None
        session.add(legacy_dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            # DFP startup order: register configured bundles, then reassign.
            manager.sync_bundles_to_db(session=session)
            session.commit()
            count = manager.reassign_dags_with_unconfigured_bundles()

        assert count == 1
        session.expire_all()
        refreshed = session.get(DagModel, "legacy_dag")
        assert refreshed.bundle_name == "custom-bundle"
        assert refreshed.relative_fileloc == "my_dag.py"

    def test_backfill_commits_between_chunks(
        self, clear_dags_and_bundles, session, tmp_path, monkeypatch
    ) -> None:
        """The legacy backfill chunks and commits like the reassignment loop.

        Without chunked commits, a deployment where every legacy Dag is
        already on a configured bundle would still UPDATE the whole set in
        one transaction, holding row locks on the dag table for the full
        DFP startup.
        """
        from airflow.dag_processing.bundles import manager as bundles_manager_mod

        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        for i in range(5):
            (bundle_dir / f"legacy_{i}.py").write_text(f"# legacy {i}")

        config = [
            {
                "name": "my-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        active_bundle = DagBundleModel(name="my-bundle")
        active_bundle.active = True
        session.add(active_bundle)
        session.flush()

        for i in range(5):
            dag = DagModel(
                dag_id=f"legacy_{i}",
                bundle_name="my-bundle",
                fileloc=str(bundle_dir / f"legacy_{i}.py"),
            )
            dag.relative_fileloc = None
            session.add(dag)
        session.flush()
        session.commit()

        monkeypatch.setattr(bundles_manager_mod, "_REASSIGN_BATCH_SIZE", 2)
        # Each chunk opens its own ``create_session`` (which commits on exit),
        # so counting context-manager entries equals counting batch commits.
        session_open_count = [0]
        real_create_session = bundles_manager_mod.create_session

        @contextlib.contextmanager
        def _counting_create_session(*args, **kwargs):
            session_open_count[0] += 1
            with real_create_session(*args, **kwargs) as s:
                yield s

        monkeypatch.setattr(bundles_manager_mod, "create_session", _counting_create_session)

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            # No unconfigured rows exist, so the reassignment loop returns
            # immediately and the helper drives the chunking we are testing.
            manager.reassign_dags_with_unconfigured_bundles()

        # 1 session for the active-bundle read + 5 backfill rows / batch size 2
        # = chunks of 2, 2, 1, then an empty chunk that terminates the loop.
        assert session_open_count[0] >= 4

        session.expire_all()
        for i in range(5):
            refreshed = session.get(DagModel, f"legacy_{i}")
            assert refreshed.relative_fileloc == f"legacy_{i}.py"

    def test_backfill_does_not_overwrite_concurrent_parser_write(
        self, clear_dags_and_bundles, session, tmp_path, monkeypatch
    ) -> None:
        """A concurrent parser write between SELECT and UPDATE keeps its value.

        Mirrors the race-safety regression on the reassignment UPDATE; the
        backfill UPDATE re-asserts ``relative_fileloc IS NULL`` so the
        parser's write is authoritative.
        """
        from airflow.dag_processing.bundles import manager as bundles_manager_mod

        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        legacy_file = bundle_dir / "legacy.py"
        legacy_file.write_text("# legacy")

        config = [
            {
                "name": "my-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        active_bundle = DagBundleModel(name="my-bundle")
        active_bundle.active = True
        session.add(active_bundle)
        session.flush()

        dag = DagModel(
            dag_id="legacy",
            bundle_name="my-bundle",
            fileloc=str(legacy_file),
        )
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        # Drive the race: the helper invokes ``Path(fileloc)`` and then
        # ``.relative_to(bundle_path)``. Wrap the Path constructor so the
        # returned object commits a parser write before delegating to the
        # real ``relative_to``.
        original_path_cls = bundles_manager_mod.Path

        def _racey_path_factory(arg):
            real_path = original_path_cls(arg)

            def _racey_relative_to(*args, **kwargs):
                with create_session() as racer:
                    racer.execute(
                        update(DagModel)
                        .where(DagModel.dag_id == "legacy")
                        .values(relative_fileloc="parser_wrote_this.py")
                    )
                return type(real_path).relative_to(real_path, *args, **kwargs)

            patcher = mock.MagicMock(wraps=real_path)
            patcher.relative_to = _racey_relative_to
            return patcher

        monkeypatch.setattr(bundles_manager_mod, "Path", _racey_path_factory)

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            manager.reassign_dags_with_unconfigured_bundles()

        session.expire_all()
        refreshed = session.get(DagModel, "legacy")
        assert refreshed.relative_fileloc == "parser_wrote_this.py"


@pytest.mark.db_test
class TestSmarterRouting:
    """Tests for per-row best-bundle routing in ``reassign_dags_with_unconfigured_bundles``."""

    def test_routes_to_bundle_whose_path_contains_fileloc(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """When multiple bundles are configured, route each Dag to the bundle whose path matches."""
        bundle_a_dir = tmp_path / "bundle_a"
        bundle_b_dir = tmp_path / "bundle_b"
        bundle_a_dir.mkdir()
        bundle_b_dir.mkdir()
        dag_a_file = bundle_a_dir / "dag_a.py"
        dag_b_file = bundle_b_dir / "dag_b.py"
        dag_a_file.write_text("# a")
        dag_b_file.write_text("# b")

        config = [
            {
                "name": "bundle-a",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_a_dir), "refresh_interval": 1},
            },
            {
                "name": "bundle-b",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_b_dir), "refresh_interval": 1},
            },
        ]
        for name, active in [("bundle-a", True), ("bundle-b", True), ("orphan", False)]:
            b = DagBundleModel(name=name)
            b.active = active
            session.add(b)
        session.flush()

        for dag_id, fileloc in [("dag-a", str(dag_a_file)), ("dag-b", str(dag_b_file))]:
            dag = DagModel(dag_id=dag_id, bundle_name="orphan", fileloc=fileloc)
            dag.relative_fileloc = None
            session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            count = manager.reassign_dags_with_unconfigured_bundles()

        assert count == 2
        session.expire_all()
        # Each Dag goes to the bundle whose path contains its fileloc, not just the first one.
        a = session.get(DagModel, "dag-a")
        assert a.bundle_name == "bundle-a"
        assert a.relative_fileloc == "dag_a.py"
        b = session.get(DagModel, "dag-b")
        assert b.bundle_name == "bundle-b"
        assert b.relative_fileloc == "dag_b.py"

    def test_longest_matching_path_wins_for_overlapping_bundles(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """When bundle paths nest, route to the most-specific bundle."""
        outer_dir = tmp_path / "outer"
        inner_dir = outer_dir / "team_x"
        inner_dir.mkdir(parents=True)
        dag_file = inner_dir / "deep_dag.py"
        dag_file.write_text("# deep")

        config = [
            {
                "name": "outer-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(outer_dir), "refresh_interval": 1},
            },
            {
                "name": "team-x-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(inner_dir), "refresh_interval": 1},
            },
        ]
        for name in ("outer-bundle", "team-x-bundle"):
            b = DagBundleModel(name=name)
            b.active = True
            session.add(b)
        orphan = DagBundleModel(name="orphan")
        orphan.active = False
        session.add(orphan)
        session.flush()

        dag = DagModel(dag_id="deep", bundle_name="orphan", fileloc=str(dag_file))
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            manager.reassign_dags_with_unconfigured_bundles()

        session.expire_all()
        refreshed = session.get(DagModel, "deep")
        # Most-specific (deeper) bundle wins.
        assert refreshed.bundle_name == "team-x-bundle"
        assert refreshed.relative_fileloc == "deep_dag.py"

    def test_no_path_match_leaves_row_unchanged(
        self, clear_dags_and_bundles, session, tmp_path, caplog
    ) -> None:
        """A Dag whose fileloc is outside every configured bundle is left untouched.

        Writing ``bundle_name`` without a verified ``relative_fileloc`` would
        produce an active row that task workloads cannot execute (no
        ``dag_rel_path``). The row stays on its unconfigured bundle so the
        existing "Requested bundle '{name}' is not configured." error at
        trigger time gives the operator an actionable signal.
        """
        bundle_dir = tmp_path / "configured"
        bundle_dir.mkdir()

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        configured = DagBundleModel(name="configured-bundle")
        configured.active = True
        session.add(configured)
        orphan = DagBundleModel(name="orphan")
        orphan.active = False
        session.add(orphan)
        session.flush()

        dag = DagModel(dag_id="elsewhere", bundle_name="orphan", fileloc="/somewhere/else/dag.py")
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with (
            patch.dict(
                os.environ,
                {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
            ),
            caplog.at_level("WARNING", logger="airflow.dag_processing.bundles.manager.DagBundlesManager"),
        ):
            manager = DagBundlesManager()
            assert manager.reassign_dags_with_unconfigured_bundles() == 0

        session.expire_all()
        refreshed = session.get(DagModel, "elsewhere")
        assert refreshed.bundle_name == "orphan"
        assert refreshed.relative_fileloc is None
        assert any("Skipped 1 legacy Dag(s)" in record.message for record in caplog.records)

    def test_legacy_row_on_active_default_routed_to_better_match(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """Migration-assigned ``dags-folder`` rows are re-routed when another bundle owns the file.

        Migration 0082 assigns every legacy 2.x row to ``dags-folder`` by
        default. An operator that keeps a configured ``dags-folder`` bundle
        alongside another bundle whose path contains the Dag's ``fileloc``
        must see the Dag reassigned to the better-matching bundle -- not
        stranded on ``dags-folder`` just because that name is still active.
        """
        dags_folder_dir = tmp_path / "dags-folder"
        team_x_dir = tmp_path / "team_x"
        dags_folder_dir.mkdir()
        team_x_dir.mkdir()
        team_x_file = team_x_dir / "team_x_dag.py"
        team_x_file.write_text("# team-x dag")

        config = [
            {
                "name": "dags-folder",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(dags_folder_dir), "refresh_interval": 1},
            },
            {
                "name": "team-x-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(team_x_dir), "refresh_interval": 1},
            },
        ]
        for name in ("dags-folder", "team-x-bundle"):
            bundle = DagBundleModel(name=name)
            bundle.active = True
            session.add(bundle)
        session.flush()

        # Migration state: bundle_name set to ``dags-folder`` (still in the
        # active config!) but fileloc actually lives under ``team-x-bundle``.
        dag = DagModel(dag_id="team_x_dag", bundle_name="dags-folder", fileloc=str(team_x_file))
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()

        assert count == 1
        session.expire_all()
        refreshed = session.get(DagModel, "team_x_dag")
        assert refreshed.bundle_name == "team-x-bundle"
        assert refreshed.relative_fileloc == "team_x_dag.py"

    def test_legacy_row_on_correct_bundle_only_backfills_relative_fileloc(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """A legacy row whose ``fileloc`` matches its current bundle keeps the bundle.

        The repair must not rewrite ``bundle_name`` when the best match is
        the same bundle the row already points at; it only needs to fill in
        the missing ``relative_fileloc``.
        """
        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        legacy_file = bundle_dir / "legacy.py"
        legacy_file.write_text("# legacy")

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        configured = DagBundleModel(name="configured-bundle")
        configured.active = True
        session.add(configured)
        session.flush()

        dag = DagModel(dag_id="legacy", bundle_name="configured-bundle", fileloc=str(legacy_file))
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            # The reassignment counter only includes rows whose bundle_name
            # changed; a same-bundle backfill is not counted as a reassignment.
            count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()

        assert count == 0
        session.expire_all()
        refreshed = session.get(DagModel, "legacy")
        assert refreshed.bundle_name == "configured-bundle"
        assert refreshed.relative_fileloc == "legacy.py"


@pytest.mark.db_test
class TestBatching:
    """Tests for the chunked-commit pattern in ``reassign_dags_with_unconfigured_bundles``."""

    def test_repair_commits_between_chunks(
        self, clear_dags_and_bundles, session, tmp_path, monkeypatch
    ) -> None:
        """All matched rows are repaired and the loop commits between chunks.

        Each ``session.commit()`` bounds the row-lock window to one chunk,
        which is the load-bearing property on a large 2.x-upgraded
        deployment.
        """
        from airflow.dag_processing.bundles import manager as bundles_manager_mod

        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        dag_files = []
        for i in range(5):
            dag_file = bundle_dir / f"dag_{i}.py"
            dag_file.write_text(f"# dag {i}")
            dag_files.append(dag_file)

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        configured = DagBundleModel(name="configured-bundle")
        configured.active = True
        orphan = DagBundleModel(name="orphan")
        orphan.active = False
        session.add(configured)
        session.add(orphan)
        session.flush()

        for i, dag_file in enumerate(dag_files):
            dag = DagModel(
                dag_id=f"dag_{i}",
                bundle_name="orphan",
                fileloc=str(dag_file),
            )
            dag.relative_fileloc = None
            session.add(dag)
        session.flush()
        session.commit()  # baseline state visible to the repair's own commits

        monkeypatch.setattr(bundles_manager_mod, "_REASSIGN_BATCH_SIZE", 2)
        # Each chunk opens its own ``create_session`` (which commits on exit),
        # so counting context-manager entries equals counting batch commits.
        session_open_count = [0]
        real_create_session = bundles_manager_mod.create_session

        @contextlib.contextmanager
        def _counting_create_session(*args, **kwargs):
            session_open_count[0] += 1
            with real_create_session(*args, **kwargs) as s:
                yield s

        monkeypatch.setattr(bundles_manager_mod, "create_session", _counting_create_session)

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            count = manager.reassign_dags_with_unconfigured_bundles()

        assert count == 5
        # 1 session for the active-bundle read + 5 rows / batch size 2
        # = chunks of 2, 2, 1, then an empty chunk that terminates the loop.
        assert session_open_count[0] >= 4

        session.expire_all()
        for i in range(5):
            refreshed = session.get(DagModel, f"dag_{i}")
            assert refreshed.bundle_name == "configured-bundle"
            assert refreshed.relative_fileloc == f"dag_{i}.py"


@pytest.mark.db_test
class TestRaceSafety:
    """Tests that the repair UPDATE does not overwrite concurrent parser writes."""

    def test_concurrent_parse_between_select_and_update_wins(
        self, clear_dags_and_bundles, session, tmp_path, monkeypatch
    ) -> None:
        """A parser that lands a write between our SELECT and UPDATE keeps its values.

        The repair's UPDATE re-asserts ``relative_fileloc IS NULL`` (and the
        DagVersion absence) so SQL evaluates against committed state when the
        UPDATE runs. A concurrent parse that wrote the real values first
        makes our UPDATE match zero rows; we must not overwrite it.
        """
        from airflow.dag_processing.bundles import manager as bundles_manager_mod

        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        dag_file = bundle_dir / "raced.py"
        dag_file.write_text("# raced")

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        configured = DagBundleModel(name="configured-bundle")
        configured.active = True
        orphan = DagBundleModel(name="orphan")
        orphan.active = False
        session.add(configured)
        session.add(orphan)
        session.flush()

        dag = DagModel(dag_id="raced", bundle_name="orphan", fileloc=str(dag_file))
        dag.relative_fileloc = None
        session.add(dag)
        session.flush()
        session.commit()

        # Drive the race: before _best_bundle_for_fileloc returns (which
        # sits between the chunk SELECT and the per-row UPDATE), simulate a
        # concurrent parser that has already committed the real values.
        original = bundles_manager_mod._best_bundle_for_fileloc

        def _racey_match(fileloc, active_bundle_paths):
            with create_session() as racer:
                racer.execute(
                    update(DagModel)
                    .where(DagModel.dag_id == "raced")
                    .values(
                        bundle_name="configured-bundle",
                        relative_fileloc="parser_wrote_this.py",
                    )
                )
            return original(fileloc, active_bundle_paths)

        monkeypatch.setattr(bundles_manager_mod, "_best_bundle_for_fileloc", _racey_match)

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            count = manager.reassign_dags_with_unconfigured_bundles()

        # CAS guard prevents the repair from clobbering the parser write.
        assert count == 0
        session.expire_all()
        refreshed = session.get(DagModel, "raced")
        assert refreshed.bundle_name == "configured-bundle"
        assert refreshed.relative_fileloc == "parser_wrote_this.py"


@pytest.mark.db_test
class TestHighAvailabilityStartup:
    """Two DFPs entering the repair path concurrently must converge safely."""

    def _build_dataset(self, session, bundle_dir):
        """Insert one reassignment-eligible row and one backfill-eligible row."""
        reassign_file = bundle_dir / "reassign.py"
        backfill_file = bundle_dir / "backfill.py"
        reassign_file.write_text("# reassign")
        backfill_file.write_text("# backfill")

        configured = DagBundleModel(name="configured-bundle")
        configured.active = True
        orphan = DagBundleModel(name="orphan")
        orphan.active = False
        session.add(configured)
        session.add(orphan)
        session.flush()

        # Hits the reassignment branch: bundle is unconfigured, fileloc lies
        # under a configured bundle's path.
        reassign_dag = DagModel(dag_id="reassign", bundle_name="orphan", fileloc=str(reassign_file))
        reassign_dag.relative_fileloc = None
        session.add(reassign_dag)

        # Hits the backfill branch: bundle is already configured, but
        # relative_fileloc is NULL (legacy row).
        backfill_dag = DagModel(
            dag_id="backfill", bundle_name="configured-bundle", fileloc=str(backfill_file)
        )
        backfill_dag.relative_fileloc = None
        session.add(backfill_dag)
        session.flush()
        session.commit()

    def test_sequential_repeat_is_idempotent(self, clear_dags_and_bundles, session, tmp_path) -> None:
        """Running the full repair twice over the same dataset must be a no-op the second pass.

        Simulates two DFPs starting up one after the other (or the same DFP
        restarting): the second pass's SELECT must find an empty set because
        the first pass either repaired the row (now non-NULL
        relative_fileloc) or skipped it (cursor advances past on the first
        pass alone, but the second pass's predicate would also exclude
        repaired rows directly).
        """
        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        self._build_dataset(session, bundle_dir)

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]

        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            first_count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()
            session.expire_all()
            after_first = {
                row.dag_id: (row.bundle_name, row.relative_fileloc)
                for row in session.execute(select(DagModel)).scalars()
            }

            second_count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()
            session.expire_all()
            after_second = {
                row.dag_id: (row.bundle_name, row.relative_fileloc)
                for row in session.execute(select(DagModel)).scalars()
            }

        assert first_count == 1  # one reassignment fires on pass 1
        assert second_count == 0  # nothing to do on pass 2
        assert after_first == after_second
        assert after_first["reassign"] == ("configured-bundle", "reassign.py")
        assert after_first["backfill"] == ("configured-bundle", "backfill.py")

    def test_interleaved_dfps_do_not_overwrite_each_other(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        """Two DFPs both SELECT the same row; the second to UPDATE must no-op.

        Mirrors a multi-DFP startup where both processes hit the repair at
        the same time. The CAS guards on the UPDATE statements ensure that
        whichever transaction commits second sees zero rows match its
        WHERE clause, leaving the first commit authoritative.
        """
        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        self._build_dataset(session, bundle_dir)

        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]

        # Hand-driven interleaving: simulate DFP A and DFP B both observing
        # the unrepaired state, then DFP A commits first, then DFP B's
        # UPDATEs run against committed state with the CAS guards in place.
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            # DFP B reads the legacy state first (still NULL relative_fileloc).
            with create_session() as session_b:
                pre_b = session_b.execute(
                    select(DagModel.dag_id, DagModel.relative_fileloc).order_by(DagModel.dag_id)
                ).all()
            assert [r[1] for r in pre_b] == [None, None]

            # DFP A runs the full repair (commits internally per chunk).
            DagBundlesManager().reassign_dags_with_unconfigured_bundles()

            # DFP B now runs its repair against the post-A committed state.
            # The CAS guards (relative_fileloc IS NULL) make every UPDATE a
            # no-op because A has already filled in the values.
            b_count = DagBundlesManager().reassign_dags_with_unconfigured_bundles()
            assert b_count == 0

        session.expire_all()
        reassign_row = session.get(DagModel, "reassign")
        backfill_row = session.get(DagModel, "backfill")
        assert reassign_row.bundle_name == "configured-bundle"
        assert reassign_row.relative_fileloc == "reassign.py"
        assert backfill_row.bundle_name == "configured-bundle"
        assert backfill_row.relative_fileloc == "backfill.py"


@pytest.mark.db_test
class TestSyncAndReassign:
    """Tests for sync_bundles_to_db followed by reassign_dags_with_unconfigured_bundles."""

    def _sync_and_reassign(self, config: list[dict], session) -> None:
        """Sync bundles to DB and reassign DAGs with unconfigured bundles.

        :param config: Bundle config list to use for this sync cycle.
        :param session: SQLAlchemy session.
        """
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.commit()
            manager.reassign_dags_with_unconfigured_bundles()

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    @pytest.mark.parametrize(
        "second_config",
        [
            pytest.param(SECOND_BUNDLE_CONFIG, id="bundle_removed_no_path_match"),
            pytest.param(BASIC_BUNDLE_CONFIG, id="bundle_active_dag_unchanged"),
        ],
    )
    def test_sync_preserves_dag_bundle_without_path_match(
        self,
        clear_dags_and_bundles,
        session,
        second_config: list[dict],
    ) -> None:
        """Sync + reassign keeps the original bundle when no configured path matches the fileloc.

        Both parameter cases insert a Dag whose ``fileloc`` is ``/tmp/dag-1.py``,
        which is not under any test bundle's path (``BasicBundle.path`` is a
        no-op). When the original bundle is removed from config the helper used
        to silently rewrite ``bundle_name`` to the first configured bundle even
        with no path match; it now leaves the row alone so the row never
        becomes active-but-un-runnable. When the bundle is still configured the
        row is untouched as before.

        :param second_config: Bundle config for the second sync cycle.
        """
        self._sync_and_reassign(BASIC_BUNDLE_CONFIG, session)

        _add_dag(session, "dag-1", "my-test-bundle")
        session.commit()

        self._sync_and_reassign(second_config, session)

        dag = session.get(DagModel, "dag-1")
        assert dag.bundle_name == "my-test-bundle"


@pytest.mark.db_test
class TestSkippedRowLifecycle:
    """End-to-end coverage for the sync -> repair skip -> stale-scan -> re-parse path.

    A legacy 2.x row with an unconfigured bundle and a fileloc outside every
    configured bundle is intentionally left untouched by the repair. The
    inactive-bundle branch of ``deactivate_stale_dags`` then marks it stale,
    and a later successful parse from a now-configured bundle must restore it
    end-to-end. Each unit-level test exercises one stage of this lifecycle;
    this test pins the whole sequence so a future refactor can't silently
    break the recovery contract.
    """

    def test_skipped_row_is_recoverable_after_operator_fix(
        self, clear_dags_and_bundles, session, tmp_path
    ) -> None:
        from airflow.dag_processing.collection import update_dag_parsing_results_in_db
        from airflow.dag_processing.manager import DagFileProcessorManager
        from airflow.sdk import DAG
        from airflow.serialization.serialized_objects import LazyDeserializedDAG

        bundle_dir = tmp_path / "dags"
        bundle_dir.mkdir()
        legacy_file = bundle_dir / "legacy.py"
        legacy_file.write_text("# legacy dag")

        # Pre-sync state: legacy row points at the orphan bundle, fileloc is
        # outside any configured bundle path, relative_fileloc is NULL.
        orphan = DagBundleModel(name="orphan")
        orphan.active = True
        session.add(orphan)
        session.flush()
        legacy = DagModel(
            dag_id="legacy_dag",
            bundle_name="orphan",
            fileloc="/elsewhere/legacy.py",
            last_parsed_time=None,
        )
        legacy.relative_fileloc = None
        legacy.is_stale = False
        session.add(legacy)
        session.commit()

        # Operator removes the orphan bundle and configures a different one
        # whose path doesn't contain the legacy fileloc.
        config = [
            {
                "name": "configured-bundle",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": str(bundle_dir), "refresh_interval": 1},
            }
        ]
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(config)},
        ):
            manager = DagBundlesManager()
            manager.sync_bundles_to_db(session=session)
            session.commit()
            # Orphan bundle is now inactive; legacy row's bundle_name still points at it.
            assert session.get(DagBundleModel, "orphan").active is False

            # Repair skips the row -- no fileloc match means no atomic
            # bundle_name + relative_fileloc write is possible.
            reassigned = manager.reassign_dags_with_unconfigured_bundles()
            assert reassigned == 0
            session.expire_all()
            refreshed = session.get(DagModel, "legacy_dag")
            assert refreshed.bundle_name == "orphan"
            assert refreshed.relative_fileloc is None
            assert refreshed.is_stale is False

            # Stale-Dag scan deactivates the row via the inactive-bundle
            # branch, which runs before the NULL relative_fileloc guard.
            dfp_manager = DagFileProcessorManager(max_runs=1, processor_timeout=10 * 60)
            dfp_manager.deactivate_stale_dags(last_parsed={})
            session.expire_all()
            stale_row = session.get(DagModel, "legacy_dag")
            assert stale_row.is_stale is True

            # Operator fix: the legacy file is now inside the configured
            # bundle's path. The parser re-parses it and the standard
            # collection write resets is_stale, bundle_name, and
            # relative_fileloc in a single transaction.
            recovered_dag = DAG(dag_id="legacy_dag")
            recovered_dag.fileloc = str(legacy_file)
            recovered_dag.relative_fileloc = "legacy.py"
            update_dag_parsing_results_in_db(
                "configured-bundle",
                None,
                [LazyDeserializedDAG.from_dag(recovered_dag)],
                {},
                None,
                set(),
                session,
            )
            session.commit()

        session.expire_all()
        restored = session.get(DagModel, "legacy_dag")
        assert restored.is_stale is False
        assert restored.bundle_name == "configured-bundle"
        assert restored.relative_fileloc == "legacy.py"
