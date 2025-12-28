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
import logging
import multiprocessing
import os
import pathlib
import re
import shlex
import stat
import subprocess
import sys
import warnings
from base64 import b64encode
from collections.abc import Callable
from configparser import ConfigParser
from copy import deepcopy
from inspect import ismodule
from io import StringIO
from re import Pattern
from typing import IO, TYPE_CHECKING, Any
from urllib.parse import urlsplit

from typing_extensions import overload

from airflow._shared.configuration.parser import (
    VALUE_NOT_FOUND_SENTINEL,
    AirflowConfigParser as _SharedAirflowConfigParser,
    ValueNotFound,
)
from airflow._shared.module_loading import import_string
from airflow.exceptions import AirflowConfigException, RemovedInAirflow4Warning
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH
from airflow.task.weight_rule import WeightRule
from airflow.utils import yaml

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


class _SecretKeys:
    """Holds the secret keys used in Airflow during runtime."""

    fernet_key: str | None = None
    jwt_secret_key: str | None = None


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


def _parse_sqlite_version(s: str) -> tuple[int, ...]:
    match = _SQLITE3_VERSION_PATTERN.match(s)
    if match is None:
        return ()
    return tuple(int(p) for p in match.group("version").split("."))


@overload
def expand_env_var(env_var: None) -> None: ...


@overload
def expand_env_var(env_var: str) -> str: ...


def expand_env_var(env_var: str | None) -> str | None:
    """
    Expand (potentially nested) env vars.

    Repeat and apply `expandvars` and `expanduser` until
    interpolation stops having any effect.
    """
    if not env_var or not isinstance(env_var, str):
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        env_var = interpolated


def run_command(command: str) -> str:
    """Run command and returns stdout."""
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, stderr = (stream.decode(sys.getdefaultencoding(), "ignore") for stream in process.communicate())

    if process.returncode != 0:
        raise AirflowConfigException(
            f"Cannot execute {command}. Error code is: {process.returncode}. "
            f"Output: {output}, Stderr: {stderr}"
        )

    return output


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


class AirflowConfigParser(_SharedAirflowConfigParser):
    """
    Custom Airflow Configparser supporting defaults and deprecated options.

    This is a subclass of the shared AirflowConfigParser that adds Core-specific initialization
    and functionality (providers, validation, writing, etc.).

    The defaults are stored in the ``_default_values``. The configuration description keeps
    description of all the options available in Airflow (description follow config.yaml.schema).

    :param default_config: default configuration (in the form of ini file).
    :param configuration_description: description of configuration to use
    """

    def __init__(
        self,
        default_config: str | None = None,
        *args,
        **kwargs,
    ):
        configuration_description = retrieve_configuration_description(include_providers=False)
        # For those who would like to use a different data structure to keep defaults:
        # We have to keep the default values in a ConfigParser rather than in any other
        # data structure, because the values we have might contain %% which are ConfigParser
        # interpolation placeholders. The _default_values config parser will interpolate them
        # properly when we call get() on it.
        _default_values = create_default_config_parser(configuration_description)
        super().__init__(configuration_description, _default_values, *args, **kwargs)
        self.configuration_description = configuration_description
        self._default_values = _default_values
        self._provider_config_fallback_default_values = create_provider_config_fallback_defaults()
        if default_config is not None:
            self._update_defaults_from_string(default_config)
        self._update_logging_deprecated_template_to_one_from_defaults()
        self.is_validated = False
        self._suppress_future_warnings = False
        self._providers_configuration_loaded = False

    @property
    def _validators(self) -> list[Callable[[], None]]:
        """Overring _validators from shared base class to add core-specific validators."""
        return [
            self._validate_sqlite3_version,
            self._validate_enums,
            self._validate_deprecated_values,
            self._upgrade_postgres_metastore_conn,
        ]

    @property
    def _lookup_sequence(self) -> list[Callable]:
        """Overring _lookup_sequence from shared base class to add provider fallbacks."""
        return super()._lookup_sequence + [self._get_option_from_provider_fallbacks]

    def _get_config_sources_for_as_dict(self) -> list[tuple[str, ConfigParser]]:
        """Override the base method to add provider fallbacks."""
        return [
            ("provider-fallback-defaults", self._provider_config_fallback_default_values),
            ("default", self._default_values),
            ("airflow.cfg", self),
        ]

    def _get_option_from_provider_fallbacks(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from provider fallback defaults."""
        if self.get_provider_config_fallback_defaults(section, key) is not None:
            # no expansion needed
            return self.get_provider_config_fallback_defaults(section, key, **kwargs)
        return VALUE_NOT_FOUND_SENTINEL

    def _update_logging_deprecated_template_to_one_from_defaults(self):
        default = self.get_default_value("logging", "log_filename_template")
        if default:
            # Tuple does not support item assignment, so we have to create a new tuple and replace it
            original_replacement = self.deprecated_values["logging"]["log_filename_template"]
            self.deprecated_values["logging"]["log_filename_template"] = (
                original_replacement[0],
                default,
            )

    def get_provider_config_fallback_defaults(self, section: str, key: str, **kwargs) -> Any:
        """Get provider config fallback default values."""
        return self._provider_config_fallback_default_values.get(section, key, fallback=None, **kwargs)

    # A mapping of old default values that we want to change and warn the user
    # about. Mapping of section -> setting -> { old, replace }
    deprecated_values: dict[str, dict[str, tuple[Pattern, str]]] = {
        "logging": {
            "log_filename_template": (
                re.compile(
                    re.escape(
                        "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id }}/task_id={{ ti.task_id }}/{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}attempt={{ try_number }}.log"
                    )
                ),
                # The actual replacement value will be updated after defaults are loaded from config.yml
                "XX-set-after-default-config-loaded-XX",
            ),
        },
        "core": {
            "executor": (re.compile(re.escape("SequentialExecutor")), "LocalExecutor"),
        },
    }

    _available_logging_levels = ["CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "INFO", "DEBUG"]
    enums_options = {
        ("core", "default_task_weight_rule"): sorted(WeightRule.all_weight_rules()),
        ("core", "dag_ignore_file_syntax"): ["regexp", "glob"],
        ("core", "mp_start_method"): multiprocessing.get_all_start_methods(),
        ("dag_processor", "file_parsing_sort_mode"): [
            "modified_time",
            "random_seeded_by_host",
            "alphabetical",
        ],
        ("logging", "logging_level"): _available_logging_levels,
        ("logging", "fab_logging_level"): _available_logging_levels,
        # celery_logging_level can be empty, which uses logging_level as fallback
        ("logging", "celery_logging_level"): [*_available_logging_levels, ""],
        # uvicorn and gunicorn logging levels for web servers
        ("logging", "uvicorn_logging_level"): _available_logging_levels,
        ("logging", "gunicorn_logging_level"): _available_logging_levels,
        ("webserver", "analytical_tool"): ["google_analytics", "metarouter", "segment", "matomo", ""],
        ("api", "grid_view_sorting_order"): ["topological", "hierarchical_alphabetical"],
    }

    upgraded_values: dict[tuple[str, str], str]
    """Mapping of (section,option) to the old value that was upgraded"""

    def write_custom_config(
        self,
        file: IO[str],
        comment_out_defaults: bool = True,
        include_descriptions: bool = True,
        extra_spacing: bool = True,
        modifications: ConfigModifications | None = None,
    ) -> None:
        """
        Write a configuration file using a ConfigModifications object.

        This method includes only options from the current airflow.cfg. For each option:
          - If it's marked for removal, omit it.
          - If renamed, output it under its new name and add a comment indicating its original location.
          - If a default update is specified, apply the new default and output the option as a commented line.
          - Otherwise, if the current value equals the default and comment_out_defaults is True, output it as a comment.
        Options absent from the current airflow.cfg are omitted.

        :param file: File to write the configuration.
        :param comment_out_defaults: If True, options whose value equals the default are written as comments.
        :param include_descriptions: Whether to include section descriptions.
        :param extra_spacing: Whether to insert an extra blank line after each option.
        :param modifications: ConfigModifications instance with rename, remove, and default updates.
        """
        modifications = modifications or ConfigModifications()
        output: dict[str, list[tuple[str, str, bool, str]]] = {}

        for section in self._sections:  # type: ignore[attr-defined]  # accessing _sections from ConfigParser
            for option, orig_value in self._sections[section].items():  # type: ignore[attr-defined]
                key = (section.lower(), option.lower())
                if key in modifications.remove:
                    continue

                mod_comment = ""
                if key in modifications.rename:
                    new_sec, new_opt = modifications.rename[key]
                    effective_section = new_sec
                    effective_option = new_opt
                    mod_comment += f"# Renamed from {section}.{option}\n"
                else:
                    effective_section = section
                    effective_option = option

                value = orig_value
                if key in modifications.default_updates:
                    mod_comment += (
                        f"# Default updated from {orig_value} to {modifications.default_updates[key]}\n"
                    )
                    value = modifications.default_updates[key]

                default_value = self.get_default_value(effective_section, effective_option, fallback="")
                is_default = str(value) == str(default_value)
                output.setdefault(effective_section.lower(), []).append(
                    (effective_option, str(value), is_default, mod_comment)
                )

        for section, options in output.items():
            section_buffer = StringIO()
            section_buffer.write(f"[{section}]\n")
            if include_descriptions:
                description = self.configuration_description.get(section, {}).get("description", "")
                if description:
                    for line in description.splitlines():
                        section_buffer.write(f"# {line}\n")
                    section_buffer.write("\n")
            for option, value_str, is_default, mod_comment in options:
                key = (section.lower(), option.lower())
                if key in modifications.default_updates and comment_out_defaults:
                    section_buffer.write(f"# {option} = {value_str}\n")
                else:
                    if mod_comment:
                        section_buffer.write(mod_comment)
                    if is_default and comment_out_defaults:
                        section_buffer.write(f"# {option} = {value_str}\n")
                    else:
                        section_buffer.write(f"{option} = {value_str}\n")
                if extra_spacing:
                    section_buffer.write("\n")
            content = section_buffer.getvalue().strip()
            if content:
                file.write(f"{content}\n\n")

    def _ensure_providers_config_loaded(self) -> None:
        """Ensure providers configurations are loaded."""
        if not self._providers_configuration_loaded:
            from airflow.providers_manager import ProvidersManager

            ProvidersManager()._initialize_providers_configuration()

    def _ensure_providers_config_unloaded(self) -> bool:
        """Ensure providers configurations are unloaded temporarily to load core configs. Returns True if providers get unloaded."""
        if self._providers_configuration_loaded:
            self.restore_core_default_configuration()
            return True
        return False

    def _reload_provider_configs(self) -> None:
        """Reload providers configuration."""
        self.load_providers_configuration()

    def restore_core_default_configuration(self) -> None:
        """
        Restore default configuration for core Airflow.

        It does not restore configuration for providers. If you want to restore configuration for
        providers, you need to call ``load_providers_configuration`` method.
        """
        self.configuration_description = retrieve_configuration_description(include_providers=False)
        self._default_values = create_default_config_parser(self.configuration_description)
        self._providers_configuration_loaded = False

    def _upgrade_postgres_metastore_conn(self):
        """
        Upgrade SQL schemas.

        As of SQLAlchemy 1.4, schemes `postgres+psycopg2` and `postgres`
        must be replaced with `postgresql`.
        """
        section, key = "database", "sql_alchemy_conn"
        old_value = self.get(section, key, _extra_stacklevel=1)
        bad_schemes = ["postgres+psycopg2", "postgres"]
        good_scheme = "postgresql"
        parsed = urlsplit(old_value)
        if parsed.scheme in bad_schemes:
            warnings.warn(
                f"Bad scheme in Airflow configuration [database] sql_alchemy_conn: `{parsed.scheme}`. "
                "As of SQLAlchemy 1.4 (adopted in Airflow 2.3) this is no longer supported.  You must "
                f"change to `{good_scheme}` before the next Airflow release.",
                FutureWarning,
                stacklevel=1,
            )
            self.upgraded_values[(section, key)] = old_value
            new_value = re.sub("^" + re.escape(f"{parsed.scheme}://"), f"{good_scheme}://", old_value)
            self._update_env_var(section=section, name=key, new_value=new_value)

            # if the old value is set via env var, we need to wipe it
            # otherwise, it'll "win" over our adjusted value
            old_env_var = self._env_var_name("core", key)
            os.environ.pop(old_env_var, None)

    def _validate_enums(self):
        """Validate that enum type config has an accepted value."""
        for (section_key, option_key), enum_options in self.enums_options.items():
            if self.has_option(section_key, option_key):
                value = self.get(section_key, option_key, fallback=None)
                if value and value not in enum_options:
                    raise AirflowConfigException(
                        f"`[{section_key}] {option_key}` should not be "
                        f"{value!r}. Possible values: {', '.join(enum_options)}."
                    )

    def _validate_sqlite3_version(self):
        """
        Validate SQLite version.

        Some features in storing rendered fields require SQLite >= 3.15.0.
        """
        if "sqlite" not in self.get("database", "sql_alchemy_conn"):
            return

        import sqlite3

        min_sqlite_version = (3, 15, 0)
        if _parse_sqlite_version(sqlite3.sqlite_version) >= min_sqlite_version:
            return

        from airflow.utils.docs import get_docs_url

        min_sqlite_version_str = ".".join(str(s) for s in min_sqlite_version)
        raise AirflowConfigException(
            f"error: SQLite C library too old (< {min_sqlite_version_str}). "
            f"See {get_docs_url('howto/set-up-database.html#setting-up-a-sqlite-database')}"
        )

    def mask_secrets(self):
        from airflow._shared.secrets_masker import mask_secret as mask_secret_core
        from airflow.sdk.log import mask_secret as mask_secret_sdk

        for section, key in self.sensitive_config_values:
            try:
                with self.suppress_future_warnings():
                    value = self.get(section, key, suppress_warnings=True)
            except AirflowConfigException:
                log.debug(
                    "Could not retrieve value from section %s, for key %s. Skipping redaction of this conf.",
                    section,
                    key,
                )
                continue
            mask_secret_core(value)
            mask_secret_sdk(value)

    def load_test_config(self):
        """
        Use test configuration rather than the configuration coming from airflow defaults.

        When running tests we use special the unit_test configuration to avoid accidental modifications and
        different behaviours when running the tests. Values for those test configuration are stored in
        the "unit_tests.cfg" configuration file in the ``airflow/config_templates`` folder
        and you need to change values there if you want to make some specific configuration to be used
        """
        from cryptography.fernet import Fernet

        unit_test_config_file = pathlib.Path(__file__).parent / "config_templates" / "unit_tests.cfg"
        unit_test_config = unit_test_config_file.read_text()
        self.remove_all_read_configurations()
        with StringIO(unit_test_config) as test_config_file:
            self.read_file(test_config_file)

        # We need those globals before we run "get_all_expansion_variables" because this is where
        # the variables are expanded from in the configuration - set to random values for tests
        _SecretKeys.fernet_key = Fernet.generate_key().decode()
        _SecretKeys.jwt_secret_key = b64encode(os.urandom(16)).decode("utf-8")
        self.expand_all_configuration_values()
        log.info("Unit test configuration loaded from 'config_unit_tests.cfg'")

    def expand_all_configuration_values(self):
        """Expand all configuration values using global and local variables defined in this module."""
        all_vars = get_all_expansion_variables()
        for section in self.sections():
            for key, value in self.items(section):
                if value is not None:
                    if self.has_option(section, key):
                        self.remove_option(section, key)
                    if self.is_template(section, key) or not isinstance(value, str):
                        self.set(section, key, value)
                    else:
                        self.set(section, key, value.format(**all_vars))

    def remove_all_read_configurations(self):
        """Remove all read configurations, leaving only default values in the config."""
        for section in self.sections():
            self.remove_section(section)

    @property
    def providers_configuration_loaded(self) -> bool:
        """Checks if providers have been loaded."""
        return self._providers_configuration_loaded

    def load_providers_configuration(self):
        """
        Load configuration for providers.

        This should be done after initial configuration have been performed. Initializing and discovering
        providers is an expensive operation and cannot be performed when we load configuration for the first
        time when airflow starts, because we initialize configuration very early, during importing of the
        `airflow` package and the module is not yet ready to be used when it happens and until configuration
        and settings are loaded. Therefore, in order to reload provider configuration we need to additionally
        load provider - specific configuration.
        """
        log.debug("Loading providers configuration")
        from airflow.providers_manager import ProvidersManager

        self.restore_core_default_configuration()
        for provider, config in ProvidersManager().already_initialized_provider_configs:
            for provider_section, provider_section_content in config.items():
                provider_options = provider_section_content["options"]
                section_in_current_config = self.configuration_description.get(provider_section)
                if not section_in_current_config:
                    self.configuration_description[provider_section] = deepcopy(provider_section_content)
                    section_in_current_config = self.configuration_description.get(provider_section)
                    section_in_current_config["source"] = f"default-{provider}"
                    for option in provider_options:
                        section_in_current_config["options"][option]["source"] = f"default-{provider}"
                else:
                    section_source = section_in_current_config.get("source", "Airflow's core package").split(
                        "default-"
                    )[-1]
                    raise AirflowConfigException(
                        f"The provider {provider} is attempting to contribute "
                        f"configuration section {provider_section} that "
                        f"has already been added before. The source of it: {section_source}. "
                        "This is forbidden. A provider can only add new sections. It "
                        "cannot contribute options to existing sections or override other "
                        "provider's configuration.",
                        UserWarning,
                    )
        self._default_values = create_default_config_parser(self.configuration_description)
        # sensitive_config_values needs to be refreshed here. This is a cached_property, so we can delete
        # the cached values, and it will be refreshed on next access.
        with contextlib.suppress(AttributeError):
            # no problem if cache is not set yet
            del self.sensitive_config_values
        self._providers_configuration_loaded = True

    def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
        """
        Override to use module-level function that reads from global conf.

        This ensures as_dict() and other methods use the same secrets backend
        configuration as the global conf instance (set via conf_vars in tests).
        """
        secrets_client = get_custom_secret_backend()
        if not secrets_client:
            return None
        try:
            return secrets_client.get_config(config_key)
        except Exception as e:
            raise AirflowConfigException(
                "Cannot retrieve config from alternative secrets backend. "
                "Make sure it is configured properly and that the Backend "
                "is accessible.\n"
                f"{e}"
            )

    def __getstate__(self) -> dict[str, Any]:
        """Return the state of the object as a dictionary for pickling."""
        return {
            name: getattr(self, name)
            for name in [
                "_sections",
                "is_validated",
                "configuration_description",
                "upgraded_values",
                "_default_values",
            ]
        }

    def __setstate__(self, state) -> None:
        """Restore the state of the object from a dictionary representation."""
        self.__init__()  # type: ignore[misc]
        config = state.pop("_sections")
        self.read_dict(config)
        self.__dict__.update(state)


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
    return {
        "FERNET_KEY": _SecretKeys.fernet_key,
        "JWT_SECRET_KEY": _SecretKeys.jwt_secret_key,
        **{
            k: v
            for k, v in globals().items()
            if not k.startswith("_") and not callable(v) and not ismodule(v)
        },
    }


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
            # We know that fernet_key is not set, so we can generate it, set as global key
            # and also write it to the config file so that same key will be used next time
            _SecretKeys.fernet_key = _generate_fernet_key()
            conf.configuration_description["core"]["options"]["fernet_key"]["default"] = (
                _SecretKeys.fernet_key
            )

        _SecretKeys.jwt_secret_key = b64encode(os.urandom(16)).decode("utf-8")
        conf.configuration_description["api_auth"]["options"]["jwt_secret"]["default"] = (
            _SecretKeys.jwt_secret_key
        )
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
    global AIRFLOW_HOME  # to be cleaned in Airflow 4.0
    log.info("Reading the config from %s", AIRFLOW_CONFIG)
    airflow_config_parser.read(AIRFLOW_CONFIG)
    if airflow_config_parser.has_option("core", "AIRFLOW_HOME"):
        msg = (
            "Specifying both AIRFLOW_HOME environment variable and airflow_home "
            "in the config file is deprecated. Please use only the AIRFLOW_HOME "
            "environment variable and remove the config file entry."
        )
        if "AIRFLOW_HOME" in os.environ:
            warnings.warn(msg, category=RemovedInAirflow4Warning, stacklevel=1)
        elif airflow_config_parser.get("core", "airflow_home") == AIRFLOW_HOME:
            warnings.warn(
                "Specifying airflow_home in the config file is deprecated. As you "
                "have left it at the default value you should remove the setting "
                "from your airflow.cfg and suffer no change in behaviour.",
                category=RemovedInAirflow4Warning,
                stacklevel=1,
            )
        else:
            AIRFLOW_HOME = airflow_config_parser.get("core", "airflow_home")
            warnings.warn(msg, category=RemovedInAirflow4Warning, stacklevel=1)


def initialize_config() -> AirflowConfigParser:
    """
    Load the Airflow config files.

    Called for you automatically as part of the Airflow boot process.
    """
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

    This is a convenience function that calls conf._get_custom_secret_backend().
    """
    return conf._get_custom_secret_backend(worker_mode=worker_mode)


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

conf: AirflowConfigParser = initialize_config()
secrets_backend_list = initialize_secrets_backends()
conf.validate()
