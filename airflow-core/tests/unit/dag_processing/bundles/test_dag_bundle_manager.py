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
from airflow.dag_processing.bundles.manager import DagBundlesManager, reassign_dags_with_unconfigured_bundles
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
    """Tests for the reassign_dags_with_unconfigured_bundles utility function."""

    def test_no_configured_bundles_returns_zero(self, clear_dags_and_bundles, session):
        count = reassign_dags_with_unconfigured_bundles([], session=session)
        assert count == 0

    def test_no_stale_dags_returns_zero(self, clear_dags_and_bundles, session):
        session.add(DagBundleModel(name="bundle-a"))
        session.flush()
        _add_dag(session, "dag-1", "bundle-a")

        count = reassign_dags_with_unconfigured_bundles(["bundle-a"], session=session)
        assert count == 0

        dag = session.get(DagModel, "dag-1")
        assert dag.bundle_name == "bundle-a"

    def test_dags_with_unconfigured_bundle_are_reassigned(self, clear_dags_and_bundles, session):
        session.add(DagBundleModel(name="old-bundle"))
        session.add(DagBundleModel(name="new-bundle"))
        session.flush()

        _add_dag(session, "dag-1", "old-bundle")
        _add_dag(session, "dag-2", "old-bundle")
        _add_dag(session, "dag-3", "new-bundle")

        count = reassign_dags_with_unconfigured_bundles(["new-bundle"], session=session)
        assert count == 2

        assert session.get(DagModel, "dag-1").bundle_name == "new-bundle"
        assert session.get(DagModel, "dag-2").bundle_name == "new-bundle"
        assert session.get(DagModel, "dag-3").bundle_name == "new-bundle"

    def test_first_configured_bundle_is_used_as_default(self, clear_dags_and_bundles, session):
        session.add(DagBundleModel(name="removed-bundle"))
        session.add(DagBundleModel(name="primary"))
        session.add(DagBundleModel(name="secondary"))
        session.flush()

        _add_dag(session, "dag-1", "removed-bundle")

        count = reassign_dags_with_unconfigured_bundles(
            ["primary", "secondary"], session=session
        )
        assert count == 1
        assert session.get(DagModel, "dag-1").bundle_name == "primary"

    def test_multiple_unconfigured_bundles(self, clear_dags_and_bundles, session):
        session.add(DagBundleModel(name="old-a"))
        session.add(DagBundleModel(name="old-b"))
        session.add(DagBundleModel(name="active"))
        session.flush()

        _add_dag(session, "dag-1", "old-a")
        _add_dag(session, "dag-2", "old-b")
        _add_dag(session, "dag-3", "active")

        count = reassign_dags_with_unconfigured_bundles(["active"], session=session)
        assert count == 2

        assert session.get(DagModel, "dag-1").bundle_name == "active"
        assert session.get(DagModel, "dag-2").bundle_name == "active"
        assert session.get(DagModel, "dag-3").bundle_name == "active"


SECOND_BUNDLE_CONFIG = [
    {
        "name": "second-bundle",
        "classpath": "unit.dag_processing.bundles.test_dag_bundle_manager.BasicBundle",
        "kwargs": {"refresh_interval": 1},
    }
]


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_reassigns_dags_from_removed_bundle(clear_dags_and_bundles, session):
    """sync_bundles_to_db reassigns DAGs when their bundle is removed from config."""
    with patch.dict(
        os.environ,
        {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db(session=session)
    session.flush()

    _add_dag(session, "dag-1", "my-test-bundle")

    with patch.dict(
        os.environ,
        {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(SECOND_BUNDLE_CONFIG)},
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db(session=session)

    dag = session.get(DagModel, "dag-1")
    assert dag.bundle_name == "second-bundle"


@pytest.mark.db_test
@conf_vars({("core", "LOAD_EXAMPLES"): "False"})
def test_sync_does_not_reassign_dags_with_active_bundle(clear_dags_and_bundles, session):
    """sync_bundles_to_db leaves DAGs alone when their bundle is still configured."""
    with patch.dict(
        os.environ,
        {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db(session=session)
    session.flush()

    _add_dag(session, "dag-1", "my-test-bundle")

    with patch.dict(
        os.environ,
        {"AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(BASIC_BUNDLE_CONFIG)},
    ):
        manager = DagBundlesManager()
        manager.sync_bundles_to_db(session=session)

    dag = session.get(DagModel, "dag-1")
    assert dag.bundle_name == "my-test-bundle"
