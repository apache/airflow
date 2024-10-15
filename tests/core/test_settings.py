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
from argparse import Namespace
from unittest import mock
from unittest.mock import MagicMock, call, patch

import pytest

from airflow.__main__ import configure_internal_api
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.configuration import conf
from airflow.exceptions import AirflowClusterPolicyViolation, AirflowConfigException
from airflow.settings import _ENABLE_AIP_44, TracebackSession, is_usage_data_collection_enabled
from airflow.utils.session import create_session
from tests_common.test_utils.config import conf_vars

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


@pytest.fixture
def clear_internal_api():
    InternalApiConfig._use_internal_api = False
    InternalApiConfig._internal_api_endpoint = ""
    from airflow import settings

    old_engine = settings.engine
    old_session = settings.Session
    old_conn = settings.SQL_ALCHEMY_CONN
    try:
        yield
    finally:
        InternalApiConfig._use_internal_api = False
        InternalApiConfig._internal_api_endpoint = ""
        settings.engine = old_engine
        settings.Session = old_session
        settings.SQL_ALCHEMY_CONN = old_conn


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
    @mock.patch("airflow.settings.prepare_syspath_for_dags_folder")
    def test_initialize_order(
        self,
        mock_prepare_syspath_for_dags_folder,
        mock_import_local_settings,
        mock_prepare_syspath_for_config_and_plugins,
    ):
        """
        Tests that import_local_settings is called between prepare_syspath_for_config_and_plugins
        and prepare_syspath_for_dags_folder
        """
        mock_local_settings = mock.Mock()

        mock_local_settings.attach_mock(
            mock_prepare_syspath_for_config_and_plugins, "prepare_syspath_for_config_and_plugins"
        )
        mock_local_settings.attach_mock(mock_import_local_settings, "import_local_settings")
        mock_local_settings.attach_mock(
            mock_prepare_syspath_for_dags_folder, "prepare_syspath_for_dags_folder"
        )

        import airflow.settings

        airflow.settings.initialize()

        expected_calls = [
            call.prepare_syspath_for_config_and_plugins(),
            call.import_local_settings(),
            call.prepare_syspath_for_dags_folder(),
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


class TestUpdatedConfigNames:
    @conf_vars({("webserver", "session_lifetime_minutes"): "43200"})
    def test_config_val_is_default(self):
        from airflow import settings

        session_lifetime_config = settings.get_session_lifetime_config()
        assert session_lifetime_config == 43200

    @conf_vars({("webserver", "session_lifetime_minutes"): "43201"})
    def test_config_val_is_not_default(self):
        from airflow import settings

        session_lifetime_config = settings.get_session_lifetime_config()
        assert session_lifetime_config == 43201

    @conf_vars({("webserver", "session_lifetime_days"): ""})
    def test_uses_updated_session_timeout_config_by_default(self):
        from airflow import settings

        session_lifetime_config = settings.get_session_lifetime_config()
        default_timeout_minutes = 30 * 24 * 60
        assert session_lifetime_config == default_timeout_minutes


_local_db_path_error = pytest.raises(AirflowConfigException, match=r"Cannot use relative path:")


@pytest.mark.parametrize(
    ["value", "expectation"],
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

    with patch("os.environ", {"_AIRFLOW_SKIP_DB_TESTS": "true"}), patch(
        "airflow.settings.SQL_ALCHEMY_CONN", value
    ), patch("airflow.settings.Session"), patch("airflow.settings.engine"):
        with expectation:
            settings.configure_orm()


class TestEngineArgs:
    @staticmethod
    @patch("airflow.settings.conf")
    @patch("airflow.settings.is_sqlalchemy_v1")
    def test_encoding_present_in_v1(is_v1, mock_conf):
        from airflow import settings

        is_v1.return_value = True
        mock_conf.getjson.return_value = {}

        engine_args = settings.prepare_engine_args()

        assert "encoding" in engine_args

    @staticmethod
    @patch("airflow.settings.conf")
    @patch("airflow.settings.is_sqlalchemy_v1")
    def test_encoding_absent_in_v2(is_v1, mock_conf):
        from airflow import settings

        is_v1.return_value = False
        mock_conf.getjson.return_value = {}

        engine_args = settings.prepare_engine_args()

        assert "encoding" not in engine_args


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
@conf_vars(
    {
        ("core", "database_access_isolation"): "true",
        ("core", "internal_api_url"): "http://localhost:8888",
        ("database", "sql_alchemy_conn"): "none://",
    }
)
def test_get_traceback_session_if_aip_44_enabled(clear_internal_api):
    configure_internal_api(Namespace(subcommand="worker"), conf)
    assert InternalApiConfig.get_use_internal_api() is True

    with create_session() as session:
        assert isinstance(session, TracebackSession)

        # no error just to create the "session"
        # but below, when we try to use, it will raise

        with pytest.raises(
            RuntimeError,
            match="TracebackSession object was used but internal API is enabled.",
        ):
            session.execute()


@pytest.mark.skipif(not _ENABLE_AIP_44, reason="AIP-44 is disabled")
@conf_vars(
    {
        ("core", "database_access_isolation"): "true",
        ("core", "internal_api_url"): "http://localhost:8888",
        ("database", "sql_alchemy_conn"): "none://",
    }
)
@patch("airflow.utils.session.TracebackSession.__new__")
def test_create_session_ctx_mgr_no_call_methods(mock_new, clear_internal_api):
    configure_internal_api(Namespace(subcommand="worker"), conf)
    m = MagicMock()
    mock_new.return_value = m

    assert InternalApiConfig.get_use_internal_api() is True

    with create_session() as session:
        assert isinstance(session, MagicMock)
        assert session == m
    method_calls = [x[0] for x in m.method_calls]
    assert method_calls == []  # commit and close not called when using internal API


@pytest.mark.parametrize(
    "env_var, conf_setting, is_enabled",
    [
        ("false", "True", False),  # env forces disable
        ("false", "False", False),  # Both force disable
        ("False ", "False", False),  # Both force disable
        ("true", "True", True),  # Both enable
        ("true", "False", False),  # Conf forces disable
        (None, "True", True),  # Default env, conf enables
        (None, "False", False),  # Default env, conf disables
    ],
)
def test_usage_data_collection_disabled(env_var, conf_setting, is_enabled, clear_internal_api):
    conf_patch = conf_vars({("usage_data_collection", "enabled"): conf_setting})

    if env_var is not None:
        with conf_patch, patch.dict(os.environ, {"SCARF_ANALYTICS": env_var}):
            assert is_usage_data_collection_enabled() == is_enabled
    else:
        with conf_patch:
            assert is_usage_data_collection_enabled() == is_enabled
