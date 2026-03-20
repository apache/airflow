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

import json
import os
from contextlib import nullcontext
from unittest.mock import patch

import pytest
from sqlalchemy import func, select

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.exceptions import AirflowConfigException
from airflow.models.dag import DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.errors import ParseImportError

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

    def path(self):
        pass


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


@pytest.mark.db_test
class TestReassignDagsWithUnconfiguredBundles:
    """Tests for DagBundlesManager.reassign_dags_with_unconfigured_bundles."""

    def _manager_with_bundle_names(self, names: list[str]) -> DagBundlesManager:
        """Return a DagBundlesManager whose ``bundle_names`` property returns *names*.

        :param names: Bundle names to expose via the ``bundle_names`` property.
        :return: A patched ``DagBundlesManager`` instance.
        """
        with patch.dict(
            os.environ,
            {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
        ):
            manager = DagBundlesManager()
        # Override bundle_names so the method uses the names we want without
        # requiring a full bundle config for each name variant.
        manager.__class__ = type(
            "PatchedManager", (DagBundlesManager,), {"bundle_names": property(lambda self: names)}
        )
        return manager

    def test_no_configured_bundles_raises(self, clear_dags_and_bundles, session):
        """Raise AirflowConfigException when no bundles are configured."""
        manager = self._manager_with_bundle_names([])
        with pytest.raises(AirflowConfigException, match="No Dag bundles are currently configured"):
            manager.reassign_dags_with_unconfigured_bundles(session=session)

    @pytest.mark.parametrize(
        ("configured_names", "dag_setups", "expected_count", "expected_bundle"),
        [
            pytest.param(
                ["bundle-a"],
                [("dag-1", "bundle-a")],
                0,
                "bundle-a",
                id="no_stale_dags",
            ),
            pytest.param(
                ["new-bundle"],
                [("dag-1", "old-bundle"), ("dag-2", "old-bundle"), ("dag-3", "new-bundle")],
                2,
                "new-bundle",
                id="unconfigured_bundle_reassigned",
            ),
            pytest.param(
                ["primary", "secondary"],
                [("dag-1", "removed-bundle")],
                1,
                "primary",
                id="first_configured_bundle_is_default",
            ),
            pytest.param(
                ["active"],
                [("dag-1", "old-a"), ("dag-2", "old-b"), ("dag-3", "active")],
                2,
                "active",
                id="multiple_unconfigured_bundles",
            ),
        ],
    )
    def test_reassign(
        self,
        clear_dags_and_bundles,
        session,
        configured_names: list[str],
        dag_setups: list[tuple[str, str]],
        expected_count: int,
        expected_bundle: str,
    ) -> None:
        """Reassign DAGs with unconfigured bundle names to the first configured bundle.

        :param configured_names: Bundle names to treat as configured.
        :param dag_setups: Pairs of (dag_id, bundle_name) to insert before reassignment.
        :param expected_count: Number of DAGs expected to be reassigned.
        :param expected_bundle: Bundle name all DAGs should have after reassignment.
        """
        all_bundle_names = {bundle_name for _, bundle_name in dag_setups} | set(configured_names)
        session.add_all(DagBundleModel(name=name) for name in all_bundle_names)
        session.flush()

        for dag_id, bundle_name in dag_setups:
            _add_dag(session, dag_id, bundle_name)

        manager = self._manager_with_bundle_names(configured_names)
        count = manager.reassign_dags_with_unconfigured_bundles(session=session)

        assert count == expected_count
        for dag_id, _ in dag_setups:
            assert session.get(DagModel, dag_id).bundle_name == expected_bundle


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
            manager.reassign_dags_with_unconfigured_bundles(session=session)
        session.commit()

    @conf_vars({("core", "LOAD_EXAMPLES"): "False"})
    @pytest.mark.parametrize(
        ("second_config", "expected_bundle"),
        [
            pytest.param(SECOND_BUNDLE_CONFIG, "second-bundle", id="bundle_removed_dag_reassigned"),
            pytest.param(BASIC_BUNDLE_CONFIG, "my-test-bundle", id="bundle_active_dag_unchanged"),
        ],
    )
    def test_sync_reassigns_or_preserves_dag_bundle(
        self,
        clear_dags_and_bundles,
        session,
        second_config: list[dict],
        expected_bundle: str,
    ) -> None:
        """Reassign DAG bundle after sync when removed from config, leave it alone when still active.

        :param second_config: Bundle config for the second sync cycle.
        :param expected_bundle: Expected ``bundle_name`` on ``dag-1`` after the second sync.
        """
        self._sync_and_reassign(BASIC_BUNDLE_CONFIG, session)

        _add_dag(session, "dag-1", "my-test-bundle")
        session.commit()

        self._sync_and_reassign(second_config, session)

        dag = session.get(DagModel, "dag-1")
        assert dag.bundle_name == expected_bundle
