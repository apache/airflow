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

import logging
import os
import pathlib
import re
import stat
import sys
import warnings
from base64 import b64encode
from configparser import ConfigParser
from typing import TYPE_CHECKING, Any

# Import shared configuration parser and utilities
from airflow._shared.configuration import (
    AirflowConfigException,
    AirflowConfigParser,
    expand_env_var,
)

# Import private utility functions from parser module directly
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH
from airflow.utils import yaml
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
    from airflow.secrets import BaseSecretsBackend

log = logging.getLogger(__name__)

# show Airflow's deprecation warnings
if not sys.warnoptions:
    warnings.filterwarnings(action="default", category=DeprecationWarning, module="airflow")
    warnings.filterwarnings(action="default", category=PendingDeprecationWarning, module="airflow")

_SQLITE3_VERSION_PATTERN = re.compile(r"(?P<version>^\d+(?:\.\d+)*)\D?.*$")

ConfigType = str | int | float | bool
ConfigOptionsDictType = dict[str, ConfigType]
ConfigSectionSourcesType = dict[str, str | tuple[str, str]]
ConfigSourcesType = dict[str, ConfigSectionSourcesType]

ENV_VAR_PREFIX = "AIRFLOW__"


class ConfigModifications:
    """
    Holds modifications to be applied when writing out the config.

    :param rename: Mapping from (old_section, old_option) to (new_section, new_option)
    :param remove: Set of (section, option) to remove
    :param default_updates: Mapping from (section, option) to new default value
    """

    def __init__(self) -> None:
        self.rename: dict[tuple[str, str], tuple[str, str]] = {}
        self.remove: set[tuple[str, str]] = set()
        self.default_updates: dict[tuple[str, str], str] = {}

    def add_rename(self, old_section: str, old_option: str, new_section: str, new_option: str) -> None:
        self.rename[(old_section, old_option)] = (new_section, new_option)

    def add_remove(self, section: str, option: str) -> None:
        self.remove.add((section, option))

    def add_default_update(self, section: str, option: str, new_default: str) -> None:
        self.default_updates[(section, option)] = new_default


def _default_config_file_path(file_name: str) -> str:
    templates_dir = os.path.join(os.path.dirname(__file__), "config_templates")
    return os.path.join(templates_dir, file_name)


def retrieve_configuration_description(
    include_airflow: bool = True,
    include_providers: bool = True,
    selected_provider: str | None = None,
) -> dict[str, dict[str, Any]]:
    """
    Read Airflow configuration description from YAML file.

    :param include_airflow: Include Airflow configs
    :param include_providers: Include provider configs
    :param selected_provider: If specified, include selected provider only
    :return: Python dictionary containing configs & their info
    """
    base_configuration_description: dict[str, dict[str, Any]] = {}
    if include_airflow:
        with open(_default_config_file_path("config.yml")) as config_file:
            base_configuration_description.update(yaml.safe_load(config_file))
    if include_providers:
        from airflow.providers_manager import ProvidersManager

        for provider, config in ProvidersManager().provider_configs:
            if not selected_provider or provider == selected_provider:
                base_configuration_description.update(config)
    return base_configuration_description


def get_airflow_home() -> str:
    """Get path to Airflow Home."""
    return expand_env_var(os.environ.get("AIRFLOW_HOME", "~/airflow"))


def get_airflow_config(airflow_home: str) -> str:
    """Get Path to airflow.cfg path."""
    airflow_config_var = os.environ.get("AIRFLOW_CONFIG")
    if airflow_config_var is None:
        return os.path.join(airflow_home, "airflow.cfg")
    return expand_env_var(airflow_config_var)


def get_all_expansion_variables() -> dict[str, Any]:
    return {k: v for d in [globals(), locals()] for k, v in d.items() if not k.startswith("_")}


def _generate_fernet_key() -> str:
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


def create_default_config_parser(configuration_description: dict[str, dict[str, Any]]) -> ConfigParser:
    """
    Create default config parser based on configuration description.

    It creates ConfigParser with all default values retrieved from the configuration description and
    expands all the variables from the global and local variables defined in this module.

    :param configuration_description: configuration description - retrieved from config.yaml files
        following the schema defined in "config.yml.schema.json" in the config_templates folder.
    :return: Default Config Parser that can be used to read configuration values from.
    """
    parser = ConfigParser()
    all_vars = get_all_expansion_variables()
    for section, section_desc in configuration_description.items():
        parser.add_section(section)
        options = section_desc["options"]
        for key in options:
            default_value = options[key]["default"]
            is_template = options[key].get("is_template", False)
            if default_value is not None:
                if is_template or not isinstance(default_value, str):
                    parser.set(section, key, default_value)
                else:
                    parser.set(section, key, default_value.format(**all_vars))
    return parser


def create_provider_config_fallback_defaults() -> ConfigParser:
    """
    Create fallback defaults.

    This parser contains provider defaults for Airflow configuration, containing fallback default values
    that might be needed when provider classes are being imported - before provider's configuration
    is loaded.

    Unfortunately airflow currently performs a lot of stuff during importing and some of that might lead
    to retrieving provider configuration before the defaults for the provider are loaded.

    Those are only defaults, so if you have "real" values configured in your configuration (.cfg file or
    environment variables) those will be used as usual.

    NOTE!! Do NOT attempt to remove those default fallbacks thinking that they are unnecessary duplication,
    at least not until we fix the way how airflow imports "do stuff". This is unlikely to succeed.

    You've been warned!
    """
    config_parser = ConfigParser()
    config_parser.read(_default_config_file_path("provider_config_fallback_defaults.cfg"))
    return config_parser


def write_default_airflow_configuration_if_needed() -> AirflowConfigParser:
    global FERNET_KEY, JWT_SECRET_KEY
    airflow_config = pathlib.Path(AIRFLOW_CONFIG)
    if airflow_config.is_dir():
        msg = (
            "Airflow config expected to be a path to the configuration file, "
            f"but got a directory {airflow_config.__fspath__()!r}."
        )
        raise IsADirectoryError(msg)
    if not airflow_config.exists():
        log.debug("Creating new Airflow config file in: %s", airflow_config.__fspath__())
        config_directory = airflow_config.parent
        if not config_directory.exists():
            if not config_directory.is_relative_to(AIRFLOW_HOME):
                msg = (
                    f"Config directory {config_directory.__fspath__()!r} not exists "
                    f"and it is not relative to AIRFLOW_HOME {AIRFLOW_HOME!r}. "
                    "Please create this directory first."
                )
                raise FileNotFoundError(msg) from None
            log.debug("Create directory %r for Airflow config", config_directory.__fspath__())
            config_directory.mkdir(parents=True, exist_ok=True)
        if conf.get("core", "fernet_key", fallback=None) in (None, ""):
            # We know that FERNET_KEY is not set, so we can generate it, set as global key
            # and also write it to the config file so that same key will be used next time
            FERNET_KEY = _generate_fernet_key()
            conf.configuration_description["core"]["options"]["fernet_key"]["default"] = FERNET_KEY

        JWT_SECRET_KEY = b64encode(os.urandom(16)).decode("utf-8")
        conf.configuration_description["api_auth"]["options"]["jwt_secret"]["default"] = JWT_SECRET_KEY
        pathlib.Path(airflow_config.__fspath__()).touch()
        make_group_other_inaccessible(airflow_config.__fspath__())
        with open(airflow_config, "w") as file:
            conf.write(
                file,
                include_sources=False,
                include_env_vars=True,
                include_providers=True,
                extra_spacing=True,
                only_defaults=True,
            )
    return conf


def load_standard_airflow_configuration(airflow_config_parser: AirflowConfigParser):
    """
    Load standard airflow configuration.

    In case it finds that the configuration file is missing, it will create it and write the default
    configuration values there, based on defaults passed, and will add the comments and examples
    from the default configuration.

    :param airflow_config_parser: parser to which the configuration will be loaded

    """
    global AIRFLOW_HOME
    log.info("Reading the config from %s", AIRFLOW_CONFIG)
    airflow_config_parser.read(AIRFLOW_CONFIG)
    if airflow_config_parser.has_option("core", "AIRFLOW_HOME"):
        msg = (
            "Specifying both AIRFLOW_HOME environment variable and airflow_home "
            "in the config file is deprecated. Please use only the AIRFLOW_HOME "
            "environment variable and remove the config file entry."
        )
        if "AIRFLOW_HOME" in os.environ:
            warnings.warn(msg, category=DeprecationWarning, stacklevel=1)
        elif airflow_config_parser.get("core", "airflow_home") == AIRFLOW_HOME:
            warnings.warn(
                "Specifying airflow_home in the config file is deprecated. As you "
                "have left it at the default value you should remove the setting "
                "from your airflow.cfg and suffer no change in behaviour.",
                category=DeprecationWarning,
                stacklevel=1,
            )
        else:
            # there
            AIRFLOW_HOME = airflow_config_parser.get("core", "airflow_home")
            warnings.warn(msg, category=DeprecationWarning, stacklevel=1)


def inject_core_parser_helpers():
    import airflow._shared.configuration.parser as shared_parser

    shared_parser._default_config_file_path = _default_config_file_path
    shared_parser.retrieve_configuration_description = retrieve_configuration_description
    shared_parser.create_default_config_parser = create_default_config_parser
    shared_parser.create_provider_config_fallback_defaults = create_provider_config_fallback_defaults
    shared_parser.get_all_expansion_variables = get_all_expansion_variables


def initialize_config() -> AirflowConfigParser:
    """
    Load the Airflow config files.

    Called for you automatically as part of the Airflow boot process.
    """
    inject_core_parser_helpers()
    airflow_config_parser = AirflowConfigParser()
    if airflow_config_parser.getboolean("core", "unit_test_mode"):
        airflow_config_parser.load_test_config()
    else:
        load_standard_airflow_configuration(airflow_config_parser)
        # If the user set unit_test_mode in the airflow.cfg, we still
        # want to respect that and then load the default unit test configuration
        # file on top of it.
        if airflow_config_parser.getboolean("core", "unit_test_mode"):
            airflow_config_parser.load_test_config()
    return airflow_config_parser


def make_group_other_inaccessible(file_path: str):
    try:
        permissions = os.stat(file_path)
        os.chmod(file_path, permissions.st_mode & (stat.S_IRUSR | stat.S_IWUSR))
    except Exception as e:
        log.warning(
            "Could not change permissions of config file to be group/other inaccessible. "
            "Continuing with original permissions: %s",
            e,
        )


def ensure_secrets_loaded(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
) -> list[BaseSecretsBackend]:
    """
    Ensure that all secrets backends are loaded.

    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends.

    # Check if we are loading the backends for worker too by checking if the default_backends is equal
    # to DEFAULT_SECRETS_SEARCH_PATH.
    if len(secrets_backend_list) == 2 or default_backends != DEFAULT_SECRETS_SEARCH_PATH:
        return initialize_secrets_backends(default_backends=default_backends)
    return secrets_backend_list


def get_custom_secret_backend(worker_mode: bool = False) -> BaseSecretsBackend | None:
    """
    Get Secret Backend if defined in airflow.cfg.

    Conditionally selects the section, key and kwargs key based on whether it is called from worker or not.
    """
    section = "workers" if worker_mode else "secrets"
    key = "secrets_backend" if worker_mode else "backend"
    kwargs_key = "secrets_backend_kwargs" if worker_mode else "backend_kwargs"

    secrets_backend_cls = conf.getimport(section=section, key=key)

    if not secrets_backend_cls:
        if worker_mode:
            # if we find no secrets backend for worker, return that of secrets backend
            secrets_backend_cls = conf.getimport(section="secrets", key="backend")
            if not secrets_backend_cls:
                return None
            # When falling back to secrets backend, use its kwargs
            kwargs_key = "backend_kwargs"
            section = "secrets"
        else:
            return None

    try:
        backend_kwargs = conf.getjson(section=section, key=kwargs_key)
        if not backend_kwargs:
            backend_kwargs = {}
        elif not isinstance(backend_kwargs, dict):
            raise ValueError("not a dict")
    except AirflowConfigException:
        log.warning("Failed to parse [%s] %s as JSON, defaulting to no kwargs.", section, kwargs_key)
        backend_kwargs = {}
    except ValueError:
        log.warning("Failed to parse [%s] %s into a dict, defaulting to no kwargs.", section, kwargs_key)
        backend_kwargs = {}

    return secrets_backend_cls(**backend_kwargs)


def initialize_secrets_backends(
    default_backends: list[str] = DEFAULT_SECRETS_SEARCH_PATH,
) -> list[BaseSecretsBackend]:
    """
    Initialize secrets backend.

    * import secrets backend classes
    * instantiate them and return them in a list
    """
    backend_list = []
    worker_mode = False
    if default_backends != DEFAULT_SECRETS_SEARCH_PATH:
        worker_mode = True

    custom_secret_backend = get_custom_secret_backend(worker_mode)

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

    for class_name in default_backends:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


def initialize_auth_manager() -> BaseAuthManager:
    """
    Initialize auth manager.

    * import user manager class
    * instantiate it and return it
    """
    auth_manager_cls = conf.getimport(section="core", key="auth_manager")

    if not auth_manager_cls:
        raise AirflowConfigException(
            "No auth manager defined in the config. Please specify one using section/key [core/auth_manager]."
        )

    return auth_manager_cls()


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "$AIRFLOW_HOME/airflow.cfg" respectively as defaults.
AIRFLOW_HOME = get_airflow_home()
AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)

# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "tests", "dags"
)
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, "dags")

# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), "tests", "plugins"
)
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, "plugins")

SECRET_KEY = b64encode(os.urandom(16)).decode("utf-8")
FERNET_KEY = ""  # Set only if needed when generating a new file
JWT_SECRET_KEY = ""

conf: AirflowConfigParser = initialize_config()
secrets_backend_list = initialize_secrets_backends()
conf.validate()
