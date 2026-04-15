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

import contextlib
import os
import sys
import tempfile
from unittest import mock
from unittest.mock import MagicMock, call, patch

import pytest
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import AsyncEngine

from airflow import settings
from airflow.exceptions import AirflowClusterPolicyViolation, AirflowConfigException

SETTINGS_FILE_CUSTOM_ENGINE = """
from sqlalchemy import create_engine, event

_engine_created = False

def create_metadata_engine(sql_alchemy_conn, *, engine_args, connect_args):
    global _engine_created
    _engine_created = True
    engine = create_engine(
        sql_alchemy_conn, connect_args=connect_args, **engine_args, future=True,
    )
    event.listen(engine, "do_connect", lambda *a, **kw: None)
    return engine
"""

SETTINGS_FILE_POLICY = """
def test_policy(task_instance):
    task_instance.run_as_user = "myself"
"""

SETTINGS_FILE_POLICY_WITH_DUNDER_ALL = """
__all__ = ["test_policy"]

def test_policy(task_instance):
    task_instance.run_as_user = "myself"

def not_policy():
    print("This shouldn't be imported")
"""

SETTINGS_FILE_POD_MUTATION_HOOK = """
def pod_mutation_hook(pod):
    pod.namespace = 'airflow-tests'
"""

SETTINGS_FILE_CUSTOM_POLICY = """
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowClusterPolicyViolation

def task_must_have_owners(task: BaseOperator):
    if not task.owner or task.owner.lower() == "airflow":
        raise AirflowClusterPolicyViolation(
            f'''Task must have non-None non-'airflow' owner.
            Current value: {task.owner}'''
        )
"""


class SettingsContext:
    def __init__(self, content: str, module_name: str):
        self.content = content
        self.settings_root = tempfile.mkdtemp()
        filename = f"{module_name}.py"
        self.settings_file = os.path.join(self.settings_root, filename)

    def __enter__(self):
        with open(self.settings_file, "w") as handle:
            handle.writelines(self.content)
        sys.path.append(self.settings_root)
        return self.settings_file

    def __exit__(self, *exc_info):
        sys.path.remove(self.settings_root)


class TestLocalSettings:
    # Make sure that the configure_logging is not cached
    def setup_method(self):
        self.old_modules = dict(sys.modules)

    def teardown_method(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    @mock.patch("airflow.settings.prepare_syspath_for_config_and_plugins")
    @mock.patch("airflow.settings.import_local_settings")
    def test_initialize_order(self, mock_import_local_settings, mock_prepare_syspath_for_config_and_plugins):
        """
        Tests that import_local_settings is called after prepare_syspath_for_config_and_plugins
        """
        mock_local_settings = mock.Mock()

        mock_local_settings.attach_mock(
            mock_prepare_syspath_for_config_and_plugins, "prepare_syspath_for_config_and_plugins"
        )
        mock_local_settings.attach_mock(mock_import_local_settings, "import_local_settings")

        import airflow.settings

        airflow.settings.initialize()

        expected_calls = [
            call.prepare_syspath_for_config_and_plugins(),
            call.import_local_settings(),
        ]

        mock_local_settings.assert_has_calls(expected_calls)

        assert mock_local_settings.mock_calls == expected_calls

    def test_import_with_dunder_all_not_specified(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            with pytest.raises(AttributeError):
                settings.not_policy()

    def test_import_with_dunder_all(self):
        """
        Tests that if __all__ is specified in airflow_local_settings,
        only module attributes specified within are imported.
        """
        with SettingsContext(SETTINGS_FILE_POLICY_WITH_DUNDER_ALL, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    @mock.patch("airflow.settings.log.debug")
    def test_import_local_settings_without_syspath(self, log_mock):
        """
        Tests that an ImportError is raised in import_local_settings
        if there is no airflow_local_settings module on the syspath.
        """
        from airflow import settings

        settings.import_local_settings()
        log_mock.assert_called_once_with("No airflow_local_settings to import.", exc_info=True)

    def test_policy_function(self):
        """
        Tests that task instances are mutated by the policy
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POLICY, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            task_instance = MagicMock()
            settings.test_policy(task_instance)

            assert task_instance.run_as_user == "myself"

    def test_pod_mutation_hook(self):
        """
        Tests that pods are mutated by the pod_mutation_hook
        function in airflow_local_settings.
        """
        with SettingsContext(SETTINGS_FILE_POD_MUTATION_HOOK, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            pod = MagicMock()
            settings.pod_mutation_hook(pod)

            assert pod.namespace == "airflow-tests"

    def test_custom_policy(self):
        with SettingsContext(SETTINGS_FILE_CUSTOM_POLICY, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            task_instance = MagicMock()
            task_instance.owner = "airflow"
            with pytest.raises(AirflowClusterPolicyViolation):
                settings.task_must_have_owners(task_instance)


class TestMetadataEngineHooks:
    """Tests for the overridable create_metadata_engine / create_async_metadata_engine hooks."""

    def setup_method(self):
        self.old_modules = dict(sys.modules)
        from airflow import settings

        self._orig_create_metadata_engine = settings.create_metadata_engine
        self._orig_create_async_metadata_engine = settings.create_async_metadata_engine

    def teardown_method(self):
        from airflow import settings

        settings.create_metadata_engine = self._orig_create_metadata_engine
        settings.create_async_metadata_engine = self._orig_create_async_metadata_engine
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

    @patch("airflow.settings.create_metadata_engine")
    @patch("airflow.settings._configure_async_session")
    def test_configure_orm_delegates_to_create_metadata_engine(self, mock_async_session, mock_create_engine):
        """configure_orm() must call create_metadata_engine, not create_engine directly."""
        from airflow import settings

        mock_create_engine.return_value = MagicMock()

        with (
            patch("os.environ", {"_AIRFLOW_SKIP_DB_TESTS": "false"}),
            patch("airflow.settings.SQL_ALCHEMY_CONN", "sqlite://"),
            patch("airflow.settings.Session"),
            patch("airflow.settings.engine"),
            patch("airflow.settings.setup_event_handlers"),
            patch("airflow.settings.mask_secret", create=True),
            patch("airflow._shared.secrets_masker.mask_secret"),
        ):
            settings.configure_orm()

        assert len(mock_create_engine.mock_calls) == 1
        assert mock_async_session.mock_calls == [call()]
        call_kwargs = mock_create_engine.call_args
        assert call_kwargs[0][0] == "sqlite://"
        assert "engine_args" in call_kwargs[1]
        assert "connect_args" in call_kwargs[1]

    @patch("airflow.settings.create_async_metadata_engine")
    def test_configure_async_session_delegates_to_create_async_metadata_engine(
        self, mock_create_async_engine
    ):
        """_configure_async_session() must call create_async_metadata_engine."""
        from airflow import settings

        mock_create_async_engine.return_value = MagicMock()

        with patch("airflow.settings.SQL_ALCHEMY_CONN_ASYNC", "sqlite+aiosqlite://"):
            settings._configure_async_session()

        mock_create_async_engine.assert_called_once()
        call_kwargs = mock_create_async_engine.call_args
        assert call_kwargs[0][0] == "sqlite+aiosqlite://"
        assert "connect_args" in call_kwargs[1]

    @patch("airflow.settings.create_async_metadata_engine")
    def test_configure_async_session_skips_when_no_async_conn(self, mock_create_async_engine):
        """_configure_async_session() must not call the hook when SQL_ALCHEMY_CONN_ASYNC is empty."""
        from airflow import settings

        with patch("airflow.settings.SQL_ALCHEMY_CONN_ASYNC", ""):
            settings._configure_async_session()

        assert mock_create_async_engine.mock_calls == []

    @patch("airflow.settings.create_engine")
    def test_default_create_metadata_engine_forwards_args(self, mock_sa_create_engine):
        """Default create_metadata_engine must forward all args to sqlalchemy.create_engine."""
        from airflow import settings

        mock_sa_create_engine.return_value = MagicMock()
        engine_args = {"pool_size": 5, "pool_recycle": 1800}
        connect_args = {"timeout": 30}

        settings.create_metadata_engine("sqlite://", engine_args=engine_args, connect_args=connect_args)

        assert mock_sa_create_engine.mock_calls == [
            call(
                "sqlite://",
                connect_args={"timeout": 30},
                pool_size=5,
                pool_recycle=1800,
                future=True,
            )
        ]

    @patch("airflow.settings.create_async_engine")
    def test_default_create_async_metadata_engine_forwards_args(self, mock_sa_create_async):
        """Default create_async_metadata_engine must forward args to sqlalchemy.create_async_engine."""
        from airflow import settings

        mock_sa_create_async.return_value = MagicMock()
        connect_args = {"timeout": 30}

        settings.create_async_metadata_engine("sqlite+aiosqlite://", connect_args=connect_args)

        mock_sa_create_async.assert_called_once_with(
            "sqlite+aiosqlite://",
            connect_args={"timeout": 30},
            future=True,
        )

    def test_override_via_local_settings(self):
        """An override in airflow_local_settings.py replaces the default create_metadata_engine."""
        with SettingsContext(SETTINGS_FILE_CUSTOM_ENGINE, "airflow_local_settings"):
            from airflow import settings

            settings.import_local_settings()

            import airflow_local_settings

            # Verify the override is wired in
            assert settings.create_metadata_engine is airflow_local_settings.create_metadata_engine
            assert not airflow_local_settings._engine_created

            # Actually call the override and verify it runs the custom code
            engine = settings.create_metadata_engine("sqlite://", engine_args={}, connect_args={})
            assert airflow_local_settings._engine_created
            assert engine is not None


_local_db_path_error = pytest.raises(AirflowConfigException, match=r"Cannot use relative path:")


@pytest.mark.parametrize(
    ("value", "expectation"),
    [
        ("sqlite:///./relative_path.db", _local_db_path_error),
        ("sqlite:///relative/path.db", _local_db_path_error),
        pytest.param(
            "sqlite:///C:/path/to/db",
            _local_db_path_error,
            marks=pytest.mark.skipif(sys.platform.startswith("win"), reason="Skip on Windows"),
        ),
        pytest.param(
            r"sqlite:///C:\path\to\db",
            _local_db_path_error,
            marks=pytest.mark.skipif(sys.platform.startswith("win"), reason="Skip on Windows"),
        ),
        ("sqlite://", contextlib.nullcontext()),
    ],
)
def test_sqlite_relative_path(value, expectation):
    from airflow import settings

    with (
        patch("os.environ", {"_AIRFLOW_SKIP_DB_TESTS": "true"}),
        patch("airflow.settings.SQL_ALCHEMY_CONN", value),
        patch("airflow.settings.Session"),
        patch("airflow.settings.engine"),
    ):
        with expectation:
            settings.configure_orm()


class TestDisposeOrm:
    """Tests for dispose_orm() async engine disposal."""

    def setup_method(self):
        self._orig = {
            "engine": settings.engine,
            "Session": settings.Session,
            "NonScopedSession": settings.NonScopedSession,
            "async_engine": settings.async_engine,
            "AsyncSession": settings.AsyncSession,
        }

    def teardown_method(self):
        for attr, val in self._orig.items():
            setattr(settings, attr, val)

    def test_disposes_async_engine(self):
        """dispose_orm() must dispose the async engine and clear AsyncSession."""
        mock_sync_engine = MagicMock(spec=Engine)
        mock_async_engine = MagicMock(spec=AsyncEngine)
        mock_async_engine.sync_engine = mock_sync_engine

        settings.engine = None
        settings.Session = None
        settings.NonScopedSession = None
        settings.async_engine = mock_async_engine
        settings.AsyncSession = MagicMock()

        settings.dispose_orm(do_log=False)

        mock_sync_engine.dispose.assert_called_once()
        assert settings.async_engine is None
        assert settings.AsyncSession is None

    def test_disposes_both_sync_and_async(self):
        """dispose_orm() disposes both engines when both are set."""
        mock_engine = MagicMock(spec=Engine)
        mock_sync_engine = MagicMock(spec=Engine)
        mock_async_engine = MagicMock(spec=AsyncEngine)
        mock_async_engine.sync_engine = mock_sync_engine

        settings.engine = mock_engine
        settings.Session = MagicMock()
        settings.NonScopedSession = MagicMock()
        settings.async_engine = mock_async_engine
        settings.AsyncSession = MagicMock()

        with patch("sqlalchemy.orm.session.close_all_sessions"):
            settings.dispose_orm(do_log=False)

        mock_engine.dispose.assert_called_once()
        mock_sync_engine.dispose.assert_called_once()
        assert settings.engine is None
        assert settings.async_engine is None
        assert settings.AsyncSession is None

    def test_early_return_when_all_none(self):
        """dispose_orm() returns early without side effects when nothing is configured."""
        settings.engine = None
        settings.Session = None
        settings.async_engine = None

        with patch("sqlalchemy.orm.session.close_all_sessions") as mock_close:
            settings.dispose_orm(do_log=False)

        mock_close.assert_not_called()
