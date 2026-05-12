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

import re
from unittest import mock

import pytest

from airflow.sdk._shared.configuration.exceptions import AirflowConfigException
from airflow.sdk.configuration import conf, get_airflow_config
from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime

from tests_common.test_utils.config import (
    CFG_FALLBACK_CONFIG_OPTIONS,
    PROVIDER_METADATA_CONFIG_OPTIONS,
    PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK,
    conf_vars,
    create_fresh_airflow_config,
)
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker


@pytest.fixture(scope="module", autouse=True)
def restore_providers_manager_runtime_configuration():
    yield
    ProvidersManagerTaskRuntime()._cleanup()


@skip_if_force_lowest_dependencies_marker
class TestSDKProviderConfigPriority:
    """Tests that SDK conf.get and conf.has_option respect provider metadata and cfg fallbacks."""

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        PROVIDER_METADATA_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in PROVIDER_METADATA_CONFIG_OPTIONS],
    )
    def test_get_returns_provider_metadata_value(self, section, option, expected):
        """conf.get returns provider metadata (provider.yaml) values."""
        assert conf.get(section, option) == expected

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        CFG_FALLBACK_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in CFG_FALLBACK_CONFIG_OPTIONS],
    )
    def test_cfg_fallback_has_expected_value(self, section, option, expected):
        """provider_config_fallback_defaults.cfg contains expected default values."""
        assert conf.get_from_provider_cfg_config_fallback_defaults(section, option) == expected

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        PROVIDER_METADATA_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in PROVIDER_METADATA_CONFIG_OPTIONS],
    )
    def test_has_option_true_for_provider_metadata(self, section, option, expected):
        """conf.has_option returns True for options defined in provider metadata."""
        assert conf.has_option(section, option) is True

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        CFG_FALLBACK_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in CFG_FALLBACK_CONFIG_OPTIONS],
    )
    def test_has_option_true_for_cfg_fallback(self, section, option, expected):
        """conf.has_option returns True for options in provider_config_fallback_defaults.cfg."""
        assert conf.has_option(section, option) is True

    def test_has_option_false_for_nonexistent_option(self):
        """conf.has_option returns False for options not in any source."""
        assert conf.has_option("celery", "totally_nonexistent_option_xyz") is False

    @pytest.mark.parametrize(
        ("section", "option", "metadata_value", "cfg_value"),
        PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK,
        ids=[f"{s}.{o}" for s, o, _, _ in PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK],
    )
    def test_provider_metadata_overrides_cfg_fallback(self, section, option, metadata_value, cfg_value):
        """Provider metadata values take priority over provider_config_fallback_defaults.cfg values."""
        assert conf.get(section, option) == metadata_value
        assert conf.get_from_provider_cfg_config_fallback_defaults(section, option) == cfg_value

    @pytest.mark.parametrize(
        ("section", "option", "metadata_value", "cfg_value"),
        PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK,
        ids=[f"{s}.{o}" for s, o, _, _ in PROVIDER_METADATA_OVERRIDES_CFG_FALLBACK],
    )
    def test_get_default_value_priority(self, section, option, metadata_value, cfg_value):
        """get_default_value checks provider metadata before cfg fallback."""
        assert conf.get_default_value(section, option) == metadata_value

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        CFG_FALLBACK_CONFIG_OPTIONS + PROVIDER_METADATA_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in CFG_FALLBACK_CONFIG_OPTIONS + PROVIDER_METADATA_CONFIG_OPTIONS],
    )
    def test_providers_disabled_dont_get_cfg_defaults_or_provider_metadata(self, section, option, expected):
        """With providers disabled, conf.get raises for provider-only options."""
        test_conf = create_fresh_airflow_config("task-sdk")
        with test_conf.make_sure_configuration_loaded(with_providers=False):
            with pytest.raises(
                AirflowConfigException,
                match=re.escape(f"section/key [{section}/{option}] not found in config"),
            ):
                test_conf.get(section, option)

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        CFG_FALLBACK_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in CFG_FALLBACK_CONFIG_OPTIONS],
    )
    def test_has_option_returns_false_for_cfg_fallback_when_providers_disabled(
        self, section, option, expected
    ):
        """With providers disabled, conf.has_option returns False for cfg-fallback-only options."""
        test_conf = create_fresh_airflow_config("task-sdk")
        with test_conf.make_sure_configuration_loaded(with_providers=False):
            assert test_conf.has_option(section, option) is False

    @pytest.mark.parametrize(
        ("section", "option", "expected"),
        PROVIDER_METADATA_CONFIG_OPTIONS,
        ids=[f"{s}.{o}" for s, o, _ in PROVIDER_METADATA_CONFIG_OPTIONS],
    )
    def test_has_option_returns_false_for_provider_metadata_when_providers_disabled(
        self, section, option, expected
    ):
        """With providers disabled, conf.has_option returns False for provider-metadata-only options."""
        test_conf = create_fresh_airflow_config("task-sdk")
        with test_conf.make_sure_configuration_loaded(with_providers=False):
            assert test_conf.has_option(section, option) is False

    def test_env_var_overrides_provider_values(self):
        """Environment variables override both provider metadata and cfg fallback values."""
        with mock.patch.dict("os.environ", {"AIRFLOW__CELERY__CELERY_APP_NAME": "env_override"}):
            assert conf.get("celery", "celery_app_name") == "env_override"

    def test_user_config_overrides_provider_values(self):
        """User-set config values (airflow.cfg) override provider defaults."""
        custom_value = "my_custom.celery_executor"
        with conf_vars({("celery", "celery_app_name"): custom_value}):
            assert conf.get("celery", "celery_app_name") == custom_value


class TestGetAirflowConfig:
    """Tests for get_airflow_config respecting AIRFLOW_CONFIG env var."""

    def test_returns_airflow_config_env_var(self):
        """get_airflow_config returns AIRFLOW_CONFIG when set."""
        with mock.patch.dict("os.environ", {"AIRFLOW_CONFIG": "/custom/path/airflow.cfg"}):
            assert get_airflow_config() == "/custom/path/airflow.cfg"

    def test_expands_env_var_in_airflow_config(self):
        """get_airflow_config expands env vars in AIRFLOW_CONFIG."""
        with mock.patch.dict(
            "os.environ", {"AIRFLOW_CONFIG": "$CUSTOM_DIR/airflow.cfg", "CUSTOM_DIR": "/resolved"}
        ):
            assert get_airflow_config() == "/resolved/airflow.cfg"

    def test_default_fallback_when_airflow_config_not_set(self):
        """get_airflow_config returns {AIRFLOW_HOME}/airflow.cfg when AIRFLOW_CONFIG is absent."""
        env = {"AIRFLOW_HOME": "/custom/home"}
        with mock.patch.dict("os.environ", env, clear=True):
            assert get_airflow_config() == "/custom/home/airflow.cfg"

    def test_expands_env_var_in_airflow_home_fallback(self):
        """get_airflow_config expands env vars in AIRFLOW_HOME when AIRFLOW_CONFIG is absent."""
        env = {"AIRFLOW_HOME": "$CUSTOM_DIR/airflow", "CUSTOM_DIR": "/resolved"}
        with mock.patch.dict("os.environ", env, clear=True):
            assert get_airflow_config() == "/resolved/airflow/airflow.cfg"
