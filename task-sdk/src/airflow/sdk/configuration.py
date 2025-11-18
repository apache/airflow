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
"""SDK configuration parser that extends the shared parser."""

from __future__ import annotations

import logging
import os
import pathlib
from configparser import ConfigParser
from io import StringIO
from typing import Any

from airflow._shared.configuration.parser import AirflowConfigParser as _SharedAirflowConfigParser
from airflow.utils import yaml

log = logging.getLogger(__name__)


def _default_config_file_path(file_name: str) -> str:
    """Get path to airflow core config.yml file."""
    # TODO: Task SDK will have its own config.yml
    core_config_dir = (
        pathlib.Path(__file__).parent.parent.parent.parent.parent
        / "airflow-core"
        / "src"
        / "airflow"
        / "config_templates"
    )
    return str(core_config_dir / file_name)


def retrieve_configuration_description() -> dict[str, dict[str, Any]]:
    """
    Read Airflow configuration description from Core's YAML file.

    SDK reads airflow core config.yml. Eventually SDK will have its own config.yml
    with only authoring related configs.

    :return: Python dictionary containing configs & their info
    """
    base_configuration_description: dict[str, dict[str, Any]] = {}
    with open(_default_config_file_path("config.yml")) as config_file:
        base_configuration_description.update(yaml.safe_load(config_file))
    return base_configuration_description


def create_default_config_parser(configuration_description: dict[str, dict[str, Any]]) -> ConfigParser:
    """
    Create default config parser based on configuration description.

    This is a simplified version that doesn't expand variables (SDK doesn't need
    Core-specific expansion variables like SECRET_KEY, FERNET_KEY, etc.).

    :param configuration_description: configuration description from config.yml
    :return: Default Config Parser with default values
    """
    parser = ConfigParser()
    for section, section_desc in configuration_description.items():
        parser.add_section(section)
        options = section_desc["options"]
        for key in options:
            default_value = options[key]["default"]
            is_template = options[key].get("is_template", False)
            if default_value is not None:
                if is_template or not isinstance(default_value, str):
                    parser.set(section, key, str(default_value))
                else:
                    parser.set(section, key, default_value)
    return parser


def get_airflow_config() -> str:
    """Get path to airflow.cfg file."""
    airflow_home = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
    return os.path.join(airflow_home, "airflow.cfg")


class AirflowSDKConfigParser(_SharedAirflowConfigParser):
    """
    SDK configuration parser that extends the shared parser.

    In Phase 1, this reads Core's config.yml and can optionally read airflow.cfg.
    Eventually SDK will have its own config.yml with only authoring-related configs.
    """

    # Override base class type annotations - task sdk always sets these in __init__
    configuration_description: dict[str, dict[str, Any]]
    _default_values: ConfigParser

    def __init__(
        self,
        default_config: str | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        # Read Core's config.yml (Phase 1: shared config.yml)
        self.configuration_description = retrieve_configuration_description()
        # Create default values parser
        self._default_values = create_default_config_parser(self.configuration_description)
        self._suppress_future_warnings = False

        # Optionally load from airflow.cfg if it exists
        airflow_config = get_airflow_config()
        if os.path.exists(airflow_config):
            try:
                self.read(airflow_config)
            except Exception as e:
                log.warning("Could not read airflow.cfg from %s: %s", airflow_config, e)

        if default_config is not None:
            self._update_defaults_from_string(default_config)

    def get_provider_config_fallback_defaults(self, section: str, key: str, **kwargs) -> Any:
        """
        Get provider config fallback default values.

        SDK doesn't load providers in Phase 1, so always return None.
        """
        return None

    def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
        """
        Get Config option values from Secret Backend.

        SDK doesn't have secrets backend integration in Phase 1.
        Workers can access secrets via the Execution API if needed.
        """
        return None

    def load_test_config(self):
        """
        Use the test configuration instead of Airflow defaults.

        Unit tests load values from `unit_tests.cfg` to ensure consistent behavior. Realistically we should
        not have this needed but this is temporary to help fix the tests that use dag_maker and rely on few
        confs.

        The SDK does not expand template variables (FERNET_KEY, JWT_SECRET_KEY, etc.) because it does not use
        the config fields that require expansion.
        """
        unit_test_config_file = (
            pathlib.Path(__file__).parent.parent.parent.parent.parent
            / "airflow-core"
            / "src"
            / "airflow"
            / "config_templates"
            / "unit_tests.cfg"
        )
        unit_test_config = unit_test_config_file.read_text()
        self.remove_all_read_configurations()
        with StringIO(unit_test_config) as test_config_file:
            self.read_file(test_config_file)
        log.info("Unit test configuration loaded from 'unit_tests.cfg'")

    def remove_all_read_configurations(self):
        """Remove all read configurations, leaving only default values in the config."""
        for section in self.sections():
            self.remove_section(section)


def initialize_config() -> AirflowSDKConfigParser:
    """
    Initialize SDK configuration parser.

    Called automatically when SDK is imported.
    """
    airflow_config_parser = AirflowSDKConfigParser()
    if airflow_config_parser.getboolean("core", "unit_test_mode", fallback=False):
        airflow_config_parser.load_test_config()
    return airflow_config_parser


conf: AirflowSDKConfigParser = initialize_config()
