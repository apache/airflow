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
from airflow.sdk import yaml
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH

log = logging.getLogger(__name__)


def _default_config_file_path(file_name: str) -> str:
    """Get path to airflow core config.yml file."""
    # TODO: Task SDK will have its own config.yml
    # Temporary: SDK uses Core's config files during development

    # Option 1: For installed packages
    config_path = pathlib.Path(__file__).parent.parent / "config_templates" / file_name
    if config_path.exists():
        return str(config_path)

    # Option 2: Monorepo structure
    config_path = (
        pathlib.Path(__file__).parent.parent.parent.parent.parent
        / "airflow-core"
        / "src"
        / "airflow"
        / "config_templates"
        / file_name
    )
    if config_path.exists():
        return str(config_path)

    raise FileNotFoundError(f"Could not find '{file_name}' in config_templates. ")


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

    def __init__(
        self,
        default_config: str | None = None,
        *args,
        **kwargs,
    ):
        # Read Core's config.yml (Phase 1: shared config.yml)
        configuration_description = retrieve_configuration_description()
        # Create default values parser
        _default_values = create_default_config_parser(configuration_description)
        super().__init__(configuration_description, _default_values, *args, **kwargs)
        self.configuration_description = configuration_description
        self._default_values = _default_values
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


def get_custom_secret_backend(worker_mode: bool = False):
    """
    Get Secret Backend if defined in airflow.cfg.

    Conditionally selects the section, key and kwargs key based on whether it is called from worker or not.

    This is a convenience function that calls conf._get_custom_secret_backend().
    Uses SDK's conf instead of Core's conf.
    """
    # Lazy import to trigger __getattr__ and lazy initialization
    from airflow.sdk.configuration import conf

    return conf._get_custom_secret_backend(worker_mode=worker_mode)


def initialize_secrets_backends(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
):
    """
    Initialize secrets backend.

    * import secrets backend classes
    * instantiate them and return them in a list

    Uses SDK's conf instead of Core's conf.
    """
    from airflow.sdk.module_loading import import_string

    backend_list = []
    worker_mode = False
    # Determine worker mode - if default_backends is not the server default, it's worker mode
    # This is a simplified check; in practice, worker mode is determined by the caller
    if default_backends != [
        "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
        "airflow.secrets.metastore.MetastoreBackend",
    ]:
        worker_mode = True

    custom_secret_backend = get_custom_secret_backend(worker_mode)

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

    for class_name in default_backends:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


def ensure_secrets_loaded(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
) -> list:
    """
    Ensure that all secrets backends are loaded.

    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends.

    # Check if we are loading the backends for worker too by checking if the default_backends is equal
    # to DEFAULT_SECRETS_SEARCH_PATH.
    secrets_backend_list = initialize_secrets_backends()
    if len(secrets_backend_list) == 2 or default_backends != DEFAULT_SECRETS_SEARCH_PATH:
        return initialize_secrets_backends(default_backends=default_backends)
    return secrets_backend_list


def initialize_config() -> AirflowSDKConfigParser:
    """
    Initialize SDK configuration parser.

    Called automatically when SDK is imported.
    """
    airflow_config_parser = AirflowSDKConfigParser()
    if airflow_config_parser.getboolean("core", "unit_test_mode", fallback=False):
        airflow_config_parser.load_test_config()
    return airflow_config_parser


def __getattr__(name: str):
    if name == "conf":
        val = initialize_config()
        globals()[name] = val
        return val
    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")
