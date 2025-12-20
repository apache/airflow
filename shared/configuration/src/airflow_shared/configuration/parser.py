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
"""Base configuration parser with pure parsing logic."""

from __future__ import annotations

import contextlib
import datetime
import functools
import itertools
import json
import logging
import os
import shlex
import subprocess
import sys
import warnings
from collections.abc import Callable, Generator, Iterable
from configparser import ConfigParser, NoOptionError, NoSectionError
from contextlib import contextmanager
from enum import Enum
from json.decoder import JSONDecodeError
from re import Pattern
from typing import IO, Any, TypeVar, overload

from .exceptions import AirflowConfigException

log = logging.getLogger(__name__)


ConfigType = str | int | float | bool
ConfigOptionsDictType = dict[str, ConfigType]
ConfigSectionSourcesType = dict[str, str | tuple[str, str]]
ConfigSourcesType = dict[str, ConfigSectionSourcesType]
ENV_VAR_PREFIX = "AIRFLOW__"


class ValueNotFound:
    """Object of this is raised when a configuration value cannot be found."""

    pass


VALUE_NOT_FOUND_SENTINEL = ValueNotFound()


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


def _is_template(configuration_description: dict[str, dict[str, Any]], section: str, key: str) -> bool:
    """
    Check if the config is a template.

    :param configuration_description: description of configuration
    :param section: section
    :param key: key
    :return: True if the config is a template
    """
    return configuration_description.get(section, {}).get(key, {}).get("is_template", False)


class AirflowConfigParser(ConfigParser):
    """
    Base configuration parser with pure parsing logic.

    This class provides the core parsing methods that work with:
    - configuration_description: dict describing config options (required in __init__)
    - _default_values: ConfigParser with default values (required in __init__)
    - deprecated_options: class attribute mapping new -> old options
    - deprecated_sections: class attribute mapping new -> old sections
    """

    # A mapping of section -> setting -> { old, replace } for deprecated default values.
    # Subclasses can override this to define deprecated values that should be upgraded.
    deprecated_values: dict[str, dict[str, tuple[Pattern, str]]] = {}

    # A mapping of (new section, new option) -> (old section, old option, since_version).
    # When reading new option, the old option will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old option will be used instead
    deprecated_options: dict[tuple[str, str], tuple[str, str, str]] = {
        ("dag_processor", "refresh_interval"): ("scheduler", "dag_dir_list_interval", "3.0"),
        ("api", "base_url"): ("webserver", "base_url", "3.0"),
        ("api", "host"): ("webserver", "web_server_host", "3.0"),
        ("api", "port"): ("webserver", "web_server_port", "3.0"),
        ("api", "workers"): ("webserver", "workers", "3.0"),
        ("api", "worker_timeout"): ("webserver", "web_server_worker_timeout", "3.0"),
        ("api", "ssl_cert"): ("webserver", "web_server_ssl_cert", "3.0"),
        ("api", "ssl_key"): ("webserver", "web_server_ssl_key", "3.0"),
        ("api", "access_logfile"): ("webserver", "access_logfile", "3.0"),
        ("triggerer", "capacity"): ("triggerer", "default_capacity", "3.0"),
        ("api", "expose_config"): ("webserver", "expose_config", "3.0.1"),
        ("fab", "access_denied_message"): ("webserver", "access_denied_message", "3.0.2"),
        ("fab", "expose_hostname"): ("webserver", "expose_hostname", "3.0.2"),
        ("fab", "navbar_color"): ("webserver", "navbar_color", "3.0.2"),
        ("fab", "navbar_text_color"): ("webserver", "navbar_text_color", "3.0.2"),
        ("fab", "navbar_hover_color"): ("webserver", "navbar_hover_color", "3.0.2"),
        ("fab", "navbar_text_hover_color"): ("webserver", "navbar_text_hover_color", "3.0.2"),
        ("api", "secret_key"): ("webserver", "secret_key", "3.0.2"),
        ("api", "enable_swagger_ui"): ("webserver", "enable_swagger_ui", "3.0.2"),
        ("dag_processor", "parsing_pre_import_modules"): ("scheduler", "parsing_pre_import_modules", "3.0.4"),
        ("api", "grid_view_sorting_order"): ("webserver", "grid_view_sorting_order", "3.1.0"),
        ("api", "log_fetch_timeout_sec"): ("webserver", "log_fetch_timeout_sec", "3.1.0"),
        ("api", "hide_paused_dags_by_default"): ("webserver", "hide_paused_dags_by_default", "3.1.0"),
        ("api", "page_size"): ("webserver", "page_size", "3.1.0"),
        ("api", "default_wrap"): ("webserver", "default_wrap", "3.1.0"),
        ("api", "auto_refresh_interval"): ("webserver", "auto_refresh_interval", "3.1.0"),
        ("api", "require_confirmation_dag_change"): ("webserver", "require_confirmation_dag_change", "3.1.0"),
        ("api", "instance_name"): ("webserver", "instance_name", "3.1.0"),
        ("api", "log_config"): ("api", "access_logfile", "3.1.0"),
        ("scheduler", "ti_metrics_interval"): ("scheduler", "running_metrics_interval", "3.2.0"),
    }

    # A mapping of new section -> (old section, since_version).
    deprecated_sections: dict[str, tuple[str, str]] = {}

    @property
    def _lookup_sequence(self) -> list[Callable]:
        """
        Define the sequence of lookup methods for get(). The definition here does not have provider lookup.

        Subclasses can override this to customise lookup order.
        """
        return [
            self._get_environment_variables,
            self._get_option_from_config_file,
            self._get_option_from_commands,
            self._get_option_from_secrets,
            self._get_option_from_defaults,
        ]

    @property
    def _validators(self) -> list[Callable[[], None]]:
        """
        Return list of validators defined on a config parser class. Base class will return an empty list.

        Subclasses can override this to customize the validators that are run during validation on the
        config parser instance.
        """
        return []

    def validate(self) -> None:
        """Run all registered validators."""
        for validator in self._validators:
            validator()
        self.is_validated = True

    def _validate_deprecated_values(self) -> None:
        """Validate and upgrade deprecated default values."""
        for section, replacement in self.deprecated_values.items():
            for name, info in replacement.items():
                old, new = info
                current_value = self.get(section, name, fallback="")
                if self._using_old_value(old, current_value):
                    self.upgraded_values[(section, name)] = current_value
                    new_value = old.sub(new, current_value)
                    self._update_env_var(section=section, name=name, new_value=new_value)
                    self._create_future_warning(
                        name=name,
                        section=section,
                        current_value=current_value,
                        new_value=new_value,
                    )

    def _using_old_value(self, old: Pattern, current_value: str) -> bool:
        """Check if current_value matches the old pattern."""
        return old.search(current_value) is not None

    def _update_env_var(self, section: str, name: str, new_value: str) -> None:
        """Update environment variable with new value."""
        env_var = self._env_var_name(section, name)
        # Set it as an env var so that any subprocesses keep the same override!
        os.environ[env_var] = new_value

    @staticmethod
    def _create_future_warning(name: str, section: str, current_value: Any, new_value: Any) -> None:
        """Create a FutureWarning for deprecated default values."""
        warnings.warn(
            f"The {name!r} setting in [{section}] has the old default value of {current_value!r}. "
            f"This value has been changed to {new_value!r} in the running config, but please update your config.",
            FutureWarning,
            stacklevel=3,
        )

    def __init__(
        self,
        configuration_description: dict[str, dict[str, Any]],
        _default_values: ConfigParser,
        *args,
        **kwargs,
    ):
        """
        Initialize the parser.

        :param configuration_description: Description of configuration options
        :param _default_values: ConfigParser with default values
        """
        super().__init__(*args, **kwargs)
        self.configuration_description = configuration_description
        self._default_values = _default_values
        self._suppress_future_warnings = False
        self.upgraded_values = {}

    @functools.cached_property
    def inversed_deprecated_options(self):
        """Build inverse mapping from old options to new options."""
        return {(sec, name): key for key, (sec, name, ver) in self.deprecated_options.items()}

    @functools.cached_property
    def inversed_deprecated_sections(self):
        """Build inverse mapping from old sections to new sections."""
        return {
            old_section: new_section for new_section, (old_section, ver) in self.deprecated_sections.items()
        }

    @functools.cached_property
    def sensitive_config_values(self) -> set[tuple[str, str]]:
        """Get set of sensitive config values that should be masked."""
        flattened = {
            (s, k): item
            for s, s_c in self.configuration_description.items()
            for k, item in s_c.get("options", {}).items()
        }
        sensitive = {
            (section.lower(), key.lower())
            for (section, key), v in flattened.items()
            if v.get("sensitive") is True
        }
        depr_option = {self.deprecated_options[x][:-1] for x in sensitive if x in self.deprecated_options}
        depr_section = {
            (self.deprecated_sections[s][0], k) for s, k in sensitive if s in self.deprecated_sections
        }
        sensitive.update(depr_section, depr_option)
        return sensitive

    @overload  # type: ignore[override]
    def get(self, section: str, key: str, fallback: str = ..., **kwargs) -> str: ...

    @overload
    def get(self, section: str, key: str, **kwargs) -> str | None: ...

    def _update_defaults_from_string(self, config_string: str) -> None:
        """
        Update the defaults in _default_values based on values in config_string ("ini" format).

        Override shared parser's method to add validation for template variables.
        Note that those values are not validated and cannot contain variables because we are using
        regular config parser to load them. This method is used to test the config parser in unit tests.

        :param config_string:  ini-formatted config string
        """
        parser = ConfigParser()
        parser.read_string(config_string)
        for section in parser.sections():
            if section not in self._default_values.sections():
                self._default_values.add_section(section)
            errors = False
            for key, value in parser.items(section):
                if not self.is_template(section, key) and "{" in value:
                    errors = True
                    log.error(
                        "The %s.%s value %s read from string contains variable. This is not supported",
                        section,
                        key,
                        value,
                    )
                self._default_values.set(section, key, value)
            if errors:
                raise AirflowConfigException(
                    f"The string config passed as default contains variables. "
                    f"This is not supported. String config: {config_string}"
                )

    def get_default_value(self, section: str, key: str, fallback: Any = None, raw=False, **kwargs) -> Any:
        """
        Retrieve default value from default config parser.

        This will retrieve the default value from the default config parser. Optionally a raw, stored
        value can be retrieved by setting skip_interpolation to True. This is useful for example when
        we want to write the default value to a file, and we don't want the interpolation to happen
        as it is going to be done later when the config is read.

        :param section: section of the config
        :param key: key to use
        :param fallback: fallback value to use
        :param raw: if raw, then interpolation will be reversed
        :param kwargs: other args
        :return:
        """
        value = self._default_values.get(section, key, fallback=fallback, **kwargs)
        if raw and value is not None:
            return value.replace("%", "%%")
        return value

    def _get_custom_secret_backend(self, worker_mode: bool = False) -> Any | None:
        """
        Get Secret Backend if defined in airflow.cfg.

        Conditionally selects the section, key and kwargs key based on whether it is called from worker or not.
        """
        section = "workers" if worker_mode else "secrets"
        key = "secrets_backend" if worker_mode else "backend"
        kwargs_key = "secrets_backend_kwargs" if worker_mode else "backend_kwargs"

        secrets_backend_cls = self.getimport(section=section, key=key)

        if not secrets_backend_cls:
            if worker_mode:
                # if we find no secrets backend for worker, return that of secrets backend
                secrets_backend_cls = self.getimport(section="secrets", key="backend")
                if not secrets_backend_cls:
                    return None
                # When falling back to secrets backend, use its kwargs
                kwargs_key = "backend_kwargs"
                section = "secrets"
            else:
                return None

        try:
            backend_kwargs = self.getjson(section=section, key=kwargs_key)
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

    def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
        """
        Get Config option values from Secret Backend.

        Called by the shared parser's _get_secret_option() method as part of the lookup chain.
        Uses _get_custom_secret_backend() to get the backend instance.

        :param config_key: the config key to retrieve
        :return: config value or None
        """
        try:
            secrets_client = self._get_custom_secret_backend()
            if not secrets_client:
                return None
            return secrets_client.get_config(config_key)
        except Exception as e:
            raise AirflowConfigException(
                "Cannot retrieve config from alternative secrets backend. "
                "Make sure it is configured properly and that the Backend "
                "is accessible.\n"
                f"{e}"
            )

    def _get_cmd_option_from_config_sources(
        self, config_sources: ConfigSourcesType, section: str, key: str
    ) -> str | None:
        fallback_key = key + "_cmd"
        if (section, key) in self.sensitive_config_values:
            section_dict = config_sources.get(section)
            if section_dict is not None:
                command_value = section_dict.get(fallback_key)
                if command_value is not None:
                    if isinstance(command_value, str):
                        command = command_value
                    else:
                        command = command_value[0]
                    return run_command(command)
        return None

    def _get_secret_option_from_config_sources(
        self, config_sources: ConfigSourcesType, section: str, key: str
    ) -> str | None:
        fallback_key = key + "_secret"
        if (section, key) in self.sensitive_config_values:
            section_dict = config_sources.get(section)
            if section_dict is not None:
                secrets_path_value = section_dict.get(fallback_key)
                if secrets_path_value is not None:
                    if isinstance(secrets_path_value, str):
                        secrets_path = secrets_path_value
                    else:
                        secrets_path = secrets_path_value[0]
                    return self._get_config_value_from_secret_backend(secrets_path)
        return None

    def _include_secrets(
        self,
        config_sources: ConfigSourcesType,
        display_sensitive: bool,
        display_source: bool,
        raw: bool,
    ):
        for section, key in self.sensitive_config_values:
            value: str | None = self._get_secret_option_from_config_sources(config_sources, section, key)
            if value:
                if not display_sensitive:
                    value = "< hidden >"
                if display_source:
                    opt: str | tuple[str, str] = (value, "secret")
                elif raw:
                    opt = value.replace("%", "%%")
                else:
                    opt = value
                config_sources.setdefault(section, {}).update({key: opt})
                del config_sources[section][key + "_secret"]

    def _include_commands(
        self,
        config_sources: ConfigSourcesType,
        display_sensitive: bool,
        display_source: bool,
        raw: bool,
    ):
        for section, key in self.sensitive_config_values:
            opt = self._get_cmd_option_from_config_sources(config_sources, section, key)
            if not opt:
                continue
            opt_to_set: str | tuple[str, str] | None = opt
            if not display_sensitive:
                opt_to_set = "< hidden >"
            if display_source:
                opt_to_set = (str(opt_to_set), "cmd")
            elif raw:
                opt_to_set = str(opt_to_set).replace("%", "%%")
            if opt_to_set is not None:
                dict_to_update: dict[str, str | tuple[str, str]] = {key: opt_to_set}
                config_sources.setdefault(section, {}).update(dict_to_update)
                del config_sources[section][key + "_cmd"]

    def _include_envs(
        self,
        config_sources: ConfigSourcesType,
        display_sensitive: bool,
        display_source: bool,
        raw: bool,
    ):
        for env_var in [
            os_environment for os_environment in os.environ if os_environment.startswith(ENV_VAR_PREFIX)
        ]:
            try:
                _, section, key = env_var.split("__", 2)
                opt = self._get_env_var_option(section, key)
            except ValueError:
                continue
            if opt is None:
                log.warning("Ignoring unknown env var '%s'", env_var)
                continue
            if not display_sensitive and env_var != self._env_var_name("core", "unit_test_mode"):
                # Don't hide cmd/secret values here
                if not env_var.lower().endswith(("cmd", "secret")):
                    if (section, key) in self.sensitive_config_values:
                        opt = "< hidden >"
            elif raw:
                opt = opt.replace("%", "%%")
            if display_source:
                opt = (opt, "env var")

            section = section.lower()
            key = key.lower()
            config_sources.setdefault(section, {}).update({key: opt})

    def _filter_by_source(
        self,
        config_sources: ConfigSourcesType,
        display_source: bool,
        getter_func,
    ):
        """
        Delete default configs from current configuration.

        An OrderedDict of OrderedDicts, if it would conflict with special sensitive_config_values.

        This is necessary because bare configs take precedence over the command
        or secret key equivalents so if the current running config is
        materialized with Airflow defaults they in turn override user set
        command or secret key configs.

        :param config_sources: The current configuration to operate on
        :param display_source: If False, configuration options contain raw
            values. If True, options are a tuple of (option_value, source).
            Source is either 'airflow.cfg', 'default', 'env var', or 'cmd'.
        :param getter_func: A callback function that gets the user configured
            override value for a particular sensitive_config_values config.
        :return: None, the given config_sources is filtered if necessary,
            otherwise untouched.
        """
        for section, key in self.sensitive_config_values:
            # Don't bother if we don't have section / key
            if section not in config_sources or key not in config_sources[section]:
                continue
            # Check that there is something to override defaults
            try:
                getter_opt = getter_func(section, key)
            except ValueError:
                continue
            if not getter_opt:
                continue
            # Check to see that there is a default value
            if self.get_default_value(section, key) is None:
                continue
            # Check to see if bare setting is the same as defaults
            if display_source:
                # when display_source = true, we know that the config_sources contains tuple
                opt, source = config_sources[section][key]  # type: ignore
            else:
                opt = config_sources[section][key]
            if opt == self.get_default_value(section, key):
                del config_sources[section][key]

    @staticmethod
    def _deprecated_value_is_set_in_config(
        deprecated_section: str,
        deprecated_key: str,
        configs: Iterable[tuple[str, ConfigParser]],
    ) -> bool:
        for config_type, config in configs:
            if config_type != "default":
                with contextlib.suppress(NoSectionError):
                    deprecated_section_array = config.items(section=deprecated_section, raw=True)
                    if any(key == deprecated_key for key, _ in deprecated_section_array):
                        return True
        return False

    @staticmethod
    def _deprecated_variable_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f"{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}")
            is not None
        )

    @staticmethod
    def _deprecated_command_is_set_in_config(
        deprecated_section: str,
        deprecated_key: str,
        configs: Iterable[tuple[str, ConfigParser]],
    ) -> bool:
        return AirflowConfigParser._deprecated_value_is_set_in_config(
            deprecated_section=deprecated_section, deprecated_key=deprecated_key + "_cmd", configs=configs
        )

    @staticmethod
    def _deprecated_variable_command_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f"{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}_CMD")
            is not None
        )

    @staticmethod
    def _deprecated_secret_is_set_in_config(
        deprecated_section: str,
        deprecated_key: str,
        configs: Iterable[tuple[str, ConfigParser]],
    ) -> bool:
        return AirflowConfigParser._deprecated_value_is_set_in_config(
            deprecated_section=deprecated_section, deprecated_key=deprecated_key + "_secret", configs=configs
        )

    @staticmethod
    def _deprecated_variable_secret_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f"{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}_SECRET")
            is not None
        )

    @staticmethod
    def _replace_config_with_display_sources(
        config_sources: ConfigSourcesType,
        configs: Iterable[tuple[str, ConfigParser]],
        configuration_description: dict[str, dict[str, Any]],
        display_source: bool,
        raw: bool,
        deprecated_options: dict[tuple[str, str], tuple[str, str, str]],
        include_env: bool,
        include_cmds: bool,
        include_secret: bool,
    ):
        for source_name, config in configs:
            sections = config.sections()
            for section in sections:
                AirflowConfigParser._replace_section_config_with_display_sources(
                    config,
                    config_sources,
                    configuration_description,
                    display_source,
                    raw,
                    section,
                    source_name,
                    deprecated_options,
                    configs,
                    include_env=include_env,
                    include_cmds=include_cmds,
                    include_secret=include_secret,
                )

    @staticmethod
    def _replace_section_config_with_display_sources(
        config: ConfigParser,
        config_sources: ConfigSourcesType,
        configuration_description: dict[str, dict[str, Any]],
        display_source: bool,
        raw: bool,
        section: str,
        source_name: str,
        deprecated_options: dict[tuple[str, str], tuple[str, str, str]],
        configs: Iterable[tuple[str, ConfigParser]],
        include_env: bool,
        include_cmds: bool,
        include_secret: bool,
    ):
        sect = config_sources.setdefault(section, {})
        if isinstance(config, AirflowConfigParser):
            with config.suppress_future_warnings():
                items: Iterable[tuple[str, Any]] = config.items(section=section, raw=raw)
        else:
            items = config.items(section=section, raw=raw)
        for k, val in items:
            deprecated_section, deprecated_key, _ = deprecated_options.get((section, k), (None, None, None))
            if deprecated_section and deprecated_key:
                if source_name == "default":
                    # If deprecated entry has some non-default value set for any of the sources requested,
                    # We should NOT set default for the new entry (because it will override anything
                    # coming from the deprecated ones)
                    if AirflowConfigParser._deprecated_value_is_set_in_config(
                        deprecated_section, deprecated_key, configs
                    ):
                        continue
                    if include_env and AirflowConfigParser._deprecated_variable_is_set(
                        deprecated_section, deprecated_key
                    ):
                        continue
                    if include_cmds and (
                        AirflowConfigParser._deprecated_variable_command_is_set(
                            deprecated_section, deprecated_key
                        )
                        or AirflowConfigParser._deprecated_command_is_set_in_config(
                            deprecated_section, deprecated_key, configs
                        )
                    ):
                        continue
                    if include_secret and (
                        AirflowConfigParser._deprecated_variable_secret_is_set(
                            deprecated_section, deprecated_key
                        )
                        or AirflowConfigParser._deprecated_secret_is_set_in_config(
                            deprecated_section, deprecated_key, configs
                        )
                    ):
                        continue
            if display_source:
                updated_source_name = source_name
                if source_name == "default":
                    # defaults can come from other sources (default-<PROVIDER>) that should be used here
                    source_description_section = configuration_description.get(section, {})
                    source_description_key = source_description_section.get("options", {}).get(k, {})
                    if source_description_key is not None:
                        updated_source_name = source_description_key.get("source", source_name)
                sect[k] = (val, updated_source_name)
            else:
                sect[k] = val

    def _warn_deprecate(
        self, section: str, key: str, deprecated_section: str, deprecated_name: str, extra_stacklevel: int
    ):
        """Warn about deprecated config option usage."""
        if section == deprecated_section:
            warnings.warn(
                f"The {deprecated_name} option in [{section}] has been renamed to {key} - "
                f"the old setting has been used, but please update your config.",
                DeprecationWarning,
                stacklevel=4 + extra_stacklevel,
            )
        else:
            warnings.warn(
                f"The {deprecated_name} option in [{deprecated_section}] has been moved to the {key} option "
                f"in [{section}] - the old setting has been used, but please update your config.",
                DeprecationWarning,
                stacklevel=4 + extra_stacklevel,
            )

    @contextmanager
    def suppress_future_warnings(self):
        """
        Context manager to temporarily suppress future warnings.

        This is a stub used by the shared parser's lookup methods when checking deprecated options.
        Subclasses can override this to customize warning suppression behavior.

        :return: context manager that suppresses future warnings
        """
        suppress_future_warnings = self._suppress_future_warnings
        self._suppress_future_warnings = True
        yield self
        self._suppress_future_warnings = suppress_future_warnings

    def _env_var_name(self, section: str, key: str, team_name: str | None = None) -> str:
        """Generate environment variable name for a config option."""
        team_component: str = f"{team_name.upper()}___" if team_name else ""
        return f"{ENV_VAR_PREFIX}{team_component}{section.replace('.', '_').upper()}__{key.upper()}"

    def _get_env_var_option(self, section: str, key: str, team_name: str | None = None):
        """Get config option from environment variable."""
        env_var: str = self._env_var_name(section, key, team_name=team_name)
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])
        # alternatively AIRFLOW__{SECTION}__{KEY}_CMD (for a command)
        env_var_cmd = env_var + "_CMD"
        if env_var_cmd in os.environ:
            # if this is a valid command key...
            if (section, key) in self.sensitive_config_values:
                return run_command(os.environ[env_var_cmd])
        # alternatively AIRFLOW__{SECTION}__{KEY}_SECRET (to get from Secrets Backend)
        env_var_secret_path = env_var + "_SECRET"
        if env_var_secret_path in os.environ:
            # if this is a valid secret path...
            if (section, key) in self.sensitive_config_values:
                return self._get_config_value_from_secret_backend(os.environ[env_var_secret_path])
        return None

    def _get_cmd_option(self, section: str, key: str):
        """Get config option from command execution."""
        fallback_key = key + "_cmd"
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                command = super().get(section, fallback_key)
                try:
                    cmd_output = run_command(command)
                except AirflowConfigException as e:
                    raise e
                except Exception as e:
                    raise AirflowConfigException(
                        f"Cannot run the command for the config section [{section}]{fallback_key}_cmd."
                        f" Please check the {fallback_key} value."
                    ) from e
                return cmd_output
        return None

    def _get_secret_option(self, section: str, key: str) -> str | None:
        """Get Config option values from Secret Backend."""
        fallback_key = key + "_secret"
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                secrets_path = super().get(section, fallback_key)
                return self._get_config_value_from_secret_backend(secrets_path)
        return None

    def _get_environment_variables(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from environment variables."""
        team_name = kwargs.get("team_name", None)
        option = self._get_env_var_option(section, key, team_name=team_name)
        if option is not None:
            return option
        if deprecated_section and deprecated_key:
            with self.suppress_future_warnings():
                option = self._get_env_var_option(deprecated_section, deprecated_key, team_name=team_name)
            if option is not None:
                if issue_warning:
                    self._warn_deprecate(section, key, deprecated_section, deprecated_key, extra_stacklevel)
                return option
        return VALUE_NOT_FOUND_SENTINEL

    def _get_option_from_config_file(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from config file."""
        if team_name := kwargs.get("team_name", None):
            section = f"{team_name}={section}"
            # since this is the last lookup that supports team_name, pop it
            kwargs.pop("team_name")
        if super().has_option(section, key):
            return expand_env_var(super().get(section, key, **kwargs))
        if deprecated_section and deprecated_key:
            if super().has_option(deprecated_section, deprecated_key):
                if issue_warning:
                    self._warn_deprecate(section, key, deprecated_section, deprecated_key, extra_stacklevel)
                with self.suppress_future_warnings():
                    return expand_env_var(super().get(deprecated_section, deprecated_key, **kwargs))
        return VALUE_NOT_FOUND_SENTINEL

    def _get_option_from_commands(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from command execution."""
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_section and deprecated_key:
            with self.suppress_future_warnings():
                option = self._get_cmd_option(deprecated_section, deprecated_key)
            if option:
                if issue_warning:
                    self._warn_deprecate(section, key, deprecated_section, deprecated_key, extra_stacklevel)
                return option
        return VALUE_NOT_FOUND_SENTINEL

    def _get_option_from_secrets(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from secrets backend."""
        option = self._get_secret_option(section, key)
        if option:
            return option
        if deprecated_section and deprecated_key:
            with self.suppress_future_warnings():
                option = self._get_secret_option(deprecated_section, deprecated_key)
            if option:
                if issue_warning:
                    self._warn_deprecate(section, key, deprecated_section, deprecated_key, extra_stacklevel)
                return option
        return VALUE_NOT_FOUND_SENTINEL

    def _get_option_from_defaults(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
        team_name: str | None = None,
        **kwargs,
    ) -> str | ValueNotFound:
        """Get config option from default values."""
        if self.get_default_value(section, key) is not None or "fallback" in kwargs:
            return expand_env_var(self.get_default_value(section, key, **kwargs))
        return VALUE_NOT_FOUND_SENTINEL

    def _resolve_deprecated_lookup(
        self,
        section: str,
        key: str,
        lookup_from_deprecated: bool,
        extra_stacklevel: int = 0,
    ) -> tuple[str, str, str | None, str | None, bool]:
        """
        Resolve deprecated section/key mappings and determine deprecated values.

        :param section: Section name (will be lowercased)
        :param key: Key name (will be lowercased)
        :param lookup_from_deprecated: Whether to lookup from deprecated options
        :param extra_stacklevel: Extra stack level for warnings
        :return: Tuple of (resolved_section, resolved_key, deprecated_section, deprecated_key, warning_emitted)
        """
        section = section.lower()
        key = key.lower()
        warning_emitted = False
        deprecated_section: str | None = None
        deprecated_key: str | None = None

        if not lookup_from_deprecated:
            return section, key, deprecated_section, deprecated_key, warning_emitted

        option_description = self.configuration_description.get(section, {}).get("options", {}).get(key, {})
        if option_description.get("deprecated"):
            deprecation_reason = option_description.get("deprecation_reason", "")
            warnings.warn(
                f"The '{key}' option in section {section} is deprecated. {deprecation_reason}",
                DeprecationWarning,
                stacklevel=2 + extra_stacklevel,
            )
        # For the cases in which we rename whole sections
        if section in self.inversed_deprecated_sections:
            deprecated_section, deprecated_key = (section, key)
            section = self.inversed_deprecated_sections[section]
            if not self._suppress_future_warnings:
                warnings.warn(
                    f"The config section [{deprecated_section}] has been renamed to "
                    f"[{section}]. Please update your `conf.get*` call to use the new name",
                    FutureWarning,
                    stacklevel=2 + extra_stacklevel,
                )
            # Don't warn about individual rename if the whole section is renamed
            warning_emitted = True
        elif (section, key) in self.inversed_deprecated_options:
            # Handle using deprecated section/key instead of the new section/key
            new_section, new_key = self.inversed_deprecated_options[(section, key)]
            if not self._suppress_future_warnings and not warning_emitted:
                warnings.warn(
                    f"section/key [{section}/{key}] has been deprecated, you should use"
                    f"[{new_section}/{new_key}] instead. Please update your `conf.get*` call to use the "
                    "new name",
                    FutureWarning,
                    stacklevel=2 + extra_stacklevel,
                )
                warning_emitted = True
            deprecated_section, deprecated_key = section, key
            section, key = (new_section, new_key)
        elif section in self.deprecated_sections:
            # When accessing the new section name, make sure we check under the old config name
            deprecated_key = key
            deprecated_section = self.deprecated_sections[section][0]
        else:
            deprecated_section, deprecated_key, _ = self.deprecated_options.get(
                (section, key), (None, None, None)
            )

        return section, key, deprecated_section, deprecated_key, warning_emitted

    @overload  # type: ignore[override]
    def get(self, section: str, key: str, fallback: str = ..., **kwargs) -> str: ...

    @overload  # type: ignore[override]
    def get(self, section: str, key: str, **kwargs) -> str | None: ...

    def get(  # type: ignore[misc, override]
        self,
        section: str,
        key: str,
        suppress_warnings: bool = False,
        lookup_from_deprecated: bool = True,
        _extra_stacklevel: int = 0,
        team_name: str | None = None,
        **kwargs,
    ) -> str | None:
        """
        Get config value by iterating through lookup sequence.

        Priority order is defined by _lookup_sequence property.
        """
        section, key, deprecated_section, deprecated_key, warning_emitted = self._resolve_deprecated_lookup(
            section=section,
            key=key,
            lookup_from_deprecated=lookup_from_deprecated,
            extra_stacklevel=_extra_stacklevel,
        )

        if team_name is not None:
            kwargs["team_name"] = team_name

        for lookup_method in self._lookup_sequence:
            value = lookup_method(
                deprecated_key=deprecated_key,
                deprecated_section=deprecated_section,
                key=key,
                section=section,
                issue_warning=not warning_emitted,
                extra_stacklevel=_extra_stacklevel,
                **kwargs,
            )
            if value is not VALUE_NOT_FOUND_SENTINEL:
                return value

        # Check if fallback was explicitly provided (even if None)
        if "fallback" in kwargs:
            return kwargs["fallback"]

        if not suppress_warnings:
            log.warning("section/key [%s/%s] not found in config", section, key)

        raise AirflowConfigException(f"section/key [{section}/{key}] not found in config")

    def getboolean(self, section: str, key: str, **kwargs) -> bool:  # type: ignore[override]
        """Get config value as boolean."""
        val = str(self.get(section, key, _extra_stacklevel=1, **kwargs)).lower().strip()
        if "#" in val:
            val = val.split("#")[0].strip()
        if val in ("t", "true", "1"):
            return True
        if val in ("f", "false", "0"):
            return False
        raise AirflowConfigException(
            f'Failed to convert value to bool. Please check "{key}" key in "{section}" section. '
            f'Current value: "{val}".'
        )

    def getint(self, section: str, key: str, **kwargs) -> int:  # type: ignore[override]
        """Get config value as integer."""
        val = self.get(section, key, _extra_stacklevel=1, **kwargs)
        if val is None:
            raise AirflowConfigException(
                f"Failed to convert value None to int. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return int(val)
        except ValueError:
            raise AirflowConfigException(
                f'Failed to convert value to int. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getfloat(self, section: str, key: str, **kwargs) -> float:  # type: ignore[override]
        """Get config value as float."""
        val = self.get(section, key, _extra_stacklevel=1, **kwargs)
        if val is None:
            raise AirflowConfigException(
                f"Failed to convert value None to float. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return float(val)
        except ValueError:
            raise AirflowConfigException(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getlist(self, section: str, key: str, delimiter=",", **kwargs):
        """Get config value as list."""
        val = self.get(section, key, **kwargs)
        if val is None:
            if "fallback" in kwargs:
                return kwargs["fallback"]
            raise AirflowConfigException(
                f"Failed to convert value None to list. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return [item.strip() for item in val.split(delimiter)]
        except Exception:
            raise AirflowConfigException(
                f'Failed to parse value to a list. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    E = TypeVar("E", bound=Enum)

    def getenum(self, section: str, key: str, enum_class: type[E], **kwargs) -> E:
        """Get config value as enum."""
        val = self.get(section, key, **kwargs)
        enum_names = [enum_item.name for enum_item in enum_class]

        if val is None:
            raise AirflowConfigException(
                f'Failed to convert value. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}" and it must be one of {", ".join(enum_names)}'
            )

        try:
            return enum_class[val]
        except KeyError:
            if "fallback" in kwargs and kwargs["fallback"] in enum_names:
                return enum_class[kwargs["fallback"]]
            raise AirflowConfigException(
                f'Failed to convert value. Please check "{key}" key in "{section}" section. '
                f"the value must be one of {', '.join(enum_names)}"
            )

    def getenumlist(self, section: str, key: str, enum_class: type[E], delimiter=",", **kwargs) -> list[E]:
        """Get config value as list of enums."""
        string_list = self.getlist(section, key, delimiter, **kwargs)
        enum_names = [enum_item.name for enum_item in enum_class]
        enum_list = []

        for val in string_list:
            try:
                enum_list.append(enum_class[val])
            except KeyError:
                log.warning(
                    "Failed to convert value. Please check %s key in %s section. "
                    "it must be one of %s, if not the value is ignored",
                    key,
                    section,
                    ", ".join(enum_names),
                )

        return enum_list

    def getimport(self, section: str, key: str, **kwargs) -> Any:
        """
        Read options, import the full qualified name, and return the object.

        In case of failure, it throws an exception with the key and section names

        :return: The object or None, if the option is empty
        """
        # Fixed: use self.get() instead of conf.get()
        full_qualified_path = self.get(section=section, key=key, **kwargs)
        if not full_qualified_path:
            return None

        try:
            # Import here to avoid circular dependency
            from ..module_loading import import_string

            return import_string(full_qualified_path)
        except ImportError as e:
            log.warning(e)
            raise AirflowConfigException(
                f'The object could not be loaded. Please check "{key}" key in "{section}" section. '
                f'Current value: "{full_qualified_path}".'
            )

    def getjson(
        self, section: str, key: str, fallback=None, **kwargs
    ) -> dict | list | str | int | float | None:
        """
        Return a config value parsed from a JSON string.

        ``fallback`` is *not* JSON parsed but used verbatim when no config value is given.
        """
        try:
            data = self.get(section=section, key=key, fallback=None, _extra_stacklevel=1, **kwargs)
        except (NoSectionError, NoOptionError):
            data = None

        if data is None or data == "":
            return fallback

        try:
            return json.loads(data)
        except JSONDecodeError as e:
            raise AirflowConfigException(f"Unable to parse [{section}] {key!r} as valid json") from e

    def gettimedelta(
        self, section: str, key: str, fallback: Any = None, **kwargs
    ) -> datetime.timedelta | None:
        """
        Get the config value for the given section and key, and convert it into datetime.timedelta object.

        If the key is missing, then it is considered as `None`.

        :param section: the section from the config
        :param key: the key defined in the given section
        :param fallback: fallback value when no config value is given, defaults to None
        :raises AirflowConfigException: raised because ValueError or OverflowError
        :return: datetime.timedelta(seconds=<config_value>) or None
        """
        val = self.get(section, key, fallback=fallback, _extra_stacklevel=1, **kwargs)

        if val:
            # the given value must be convertible to integer
            try:
                int_val = int(val)
            except ValueError:
                raise AirflowConfigException(
                    f'Failed to convert value to int. Please check "{key}" key in "{section}" section. '
                    f'Current value: "{val}".'
                )

            try:
                return datetime.timedelta(seconds=int_val)
            except OverflowError as err:
                raise AirflowConfigException(
                    f"Failed to convert value to timedelta in `seconds`. "
                    f"{err}. "
                    f'Please check "{key}" key in "{section}" section. Current value: "{val}".'
                )

        return fallback

    def get_mandatory_value(self, section: str, key: str, **kwargs) -> str:
        """Get mandatory config value, raising ValueError if not found."""
        value = self.get(section, key, _extra_stacklevel=1, **kwargs)
        if value is None:
            raise ValueError(f"The value {section}/{key} should be set!")
        return value

    def get_mandatory_list_value(self, section: str, key: str, **kwargs) -> list[str]:
        """Get mandatory config value as list, raising ValueError if not found."""
        value = self.getlist(section, key, **kwargs)
        if value is None:
            raise ValueError(f"The value {section}/{key} should be set!")
        return value

    def read(
        self,
        filenames: str | bytes | os.PathLike | Iterable[str | bytes | os.PathLike],
        encoding: str | None = None,
    ) -> list[str]:
        return super().read(filenames=filenames, encoding=encoding)

    def read_dict(  # type: ignore[override]
        self, dictionary: dict[str, dict[str, Any]], source: str = "<dict>"
    ) -> None:
        """
        We define a different signature here to add better type hints and checking.

        :param dictionary: dictionary to read from
        :param source: source to be used to store the configuration
        :return:
        """
        super().read_dict(dictionary=dictionary, source=source)

    def get_sections_including_defaults(self) -> list[str]:
        """
        Retrieve all sections from the configuration parser, including sections defined by built-in defaults.

        :return: list of section names
        """
        sections_from_config = self.sections()
        sections_from_description = list(self.configuration_description.keys())
        return list(dict.fromkeys(itertools.chain(sections_from_description, sections_from_config)))

    def get_options_including_defaults(self, section: str) -> list[str]:
        """
        Retrieve all possible options from the configuration parser for the section given.

        Includes options defined by built-in defaults.

        :param section: section name
        :return: list of option names for the section given
        """
        my_own_options = self.options(section) if self.has_section(section) else []
        all_options_from_defaults = list(
            self.configuration_description.get(section, {}).get("options", {}).keys()
        )
        return list(dict.fromkeys(itertools.chain(all_options_from_defaults, my_own_options)))

    def has_option(self, section: str, option: str, lookup_from_deprecated: bool = True, **kwargs) -> bool:
        """
        Check if option is defined.

        Uses self.get() to avoid reimplementing the priority order of config variables
        (env, config, cmd, defaults).

        :param section: section to get option from
        :param option: option to get
        :param lookup_from_deprecated: If True, check if the option is defined in deprecated sections
        :param kwargs: additional keyword arguments to pass to get(), such as team_name
        :return:
        """
        try:
            value = self.get(
                section,
                option,
                fallback=None,
                _extra_stacklevel=1,
                suppress_warnings=True,
                lookup_from_deprecated=lookup_from_deprecated,
                **kwargs,
            )
            if value is None:
                return False
            return True
        except (NoOptionError, NoSectionError, AirflowConfigException):
            return False

    def set(self, section: str, option: str, value: str | None = None) -> None:
        """
        Set an option to the given value.

        This override just makes sure the section and option are lower case, to match what we do in `get`.
        """
        section = section.lower()
        option = option.lower()
        defaults = self.configuration_description or {}
        if not self.has_section(section) and section in defaults:
            # Trying to set a key in a section that exists in default, but not in the user config;
            # automatically create it
            self.add_section(section)
        super().set(section, option, value)

    def remove_option(self, section: str, option: str, remove_default: bool = True):
        """
        Remove an option if it exists in config from a file or default config.

        If both of config have the same option, this removes the option
        in both configs unless remove_default=False.
        """
        section = section.lower()
        option = option.lower()
        if super().has_option(section, option):
            super().remove_option(section, option)

        if self.get_default_value(section, option) is not None and remove_default:
            self._default_values.remove_option(section, option)

    def optionxform(self, optionstr: str) -> str:
        """
        Transform option names on every read, get, or set operation.

        This changes from the default behaviour of ConfigParser from lower-casing
        to instead be case-preserving.

        :param optionstr:
        :return:
        """
        return optionstr

    def _get_config_sources_for_as_dict(self) -> list[tuple[str, ConfigParser]]:
        """
        Get list of config sources to use in as_dict().

        Subclasses can override to add additional sources (e.g., provider configs).
        """
        return [
            ("default", self._default_values),
            ("airflow.cfg", self),
        ]

    def as_dict(
        self,
        display_source: bool = False,
        display_sensitive: bool = False,
        raw: bool = False,
        include_env: bool = True,
        include_cmds: bool = True,
        include_secret: bool = True,
    ) -> ConfigSourcesType:
        """
        Return the current configuration as an OrderedDict of OrderedDicts.

        When materializing current configuration Airflow defaults are
        materialized along with user set configs. If any of the `include_*`
        options are False then the result of calling command or secret key
        configs do not override Airflow defaults and instead are passed through.
        In order to then avoid Airflow defaults from overwriting user set
        command or secret key configs we filter out bare sensitive_config_values
        that are set to Airflow defaults when command or secret key configs
        produce different values.

        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg', 'default', 'env var', or 'cmd'.
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :param raw: Should the values be output as interpolated values, or the
            "raw" form that can be fed back in to ConfigParser
        :param include_env: Should the value of configuration from AIRFLOW__
            environment variables be included or not
        :param include_cmds: Should the result of calling any ``*_cmd`` config be
            set (True, default), or should the _cmd options be left as the
            command to run (False)
        :param include_secret: Should the result of calling any ``*_secret`` config be
            set (True, default), or should the _secret options be left as the
            path to get the secret from (False)
        :return: Dictionary, where the key is the name of the section and the content is
            the dictionary with the name of the parameter and its value.
        """
        if not display_sensitive:
            # We want to hide the sensitive values at the appropriate methods
            # since envs from cmds, secrets can be read at _include_envs method
            if not all([include_env, include_cmds, include_secret]):
                raise ValueError(
                    "If display_sensitive is false, then include_env, "
                    "include_cmds, include_secret must all be set as True"
                )

        config_sources: ConfigSourcesType = {}

        # We check sequentially all those sources and the last one we saw it in will "win"
        configs = self._get_config_sources_for_as_dict()

        self._replace_config_with_display_sources(
            config_sources,
            configs,
            self.configuration_description,
            display_source,
            raw,
            self.deprecated_options,
            include_cmds=include_cmds,
            include_env=include_env,
            include_secret=include_secret,
        )

        # add env vars and overwrite because they have priority
        if include_env:
            self._include_envs(config_sources, display_sensitive, display_source, raw)
        else:
            self._filter_by_source(config_sources, display_source, self._get_env_var_option)

        # add bash commands
        if include_cmds:
            self._include_commands(config_sources, display_sensitive, display_source, raw)
        else:
            self._filter_by_source(config_sources, display_source, self._get_cmd_option)

        # add config from secret backends
        if include_secret:
            self._include_secrets(config_sources, display_sensitive, display_source, raw)
        else:
            self._filter_by_source(config_sources, display_source, self._get_secret_option)

        if not display_sensitive:
            # This ensures the ones from config file is hidden too
            # if they are not provided through env, cmd and secret
            hidden = "< hidden >"
            for section, key in self.sensitive_config_values:
                if config_sources.get(section):
                    if config_sources[section].get(key, None):
                        if display_source:
                            source = config_sources[section][key][1]
                            config_sources[section][key] = (hidden, source)
                        else:
                            config_sources[section][key] = hidden

        return config_sources

    def _write_option_header(
        self,
        file: IO[str],
        option: str,
        extra_spacing: bool,
        include_descriptions: bool,
        include_env_vars: bool,
        include_examples: bool,
        include_sources: bool,
        section_config_description: dict[str, dict[str, Any]],
        section_to_write: str,
        sources_dict: ConfigSourcesType,
    ) -> tuple[bool, bool]:
        """
        Write header for configuration option.

        Returns tuple of (should_continue, needs_separation) where needs_separation should be
        set if the option needs additional separation to visually separate it from the next option.
        """
        option_config_description = (
            section_config_description.get("options", {}).get(option, {})
            if section_config_description
            else {}
        )
        description = option_config_description.get("description")
        needs_separation = False
        if description and include_descriptions:
            for line in description.splitlines():
                file.write(f"# {line}\n")
            needs_separation = True
        example = option_config_description.get("example")
        if example is not None and include_examples:
            if extra_spacing:
                file.write("#\n")
            example_lines = example.splitlines()
            example = "\n# ".join(example_lines)
            file.write(f"# Example: {option} = {example}\n")
            needs_separation = True
        if include_sources and sources_dict:
            sources_section = sources_dict.get(section_to_write)
            value_with_source = sources_section.get(option) if sources_section else None
            if value_with_source is None:
                file.write("#\n# Source: not defined\n")
            else:
                file.write(f"#\n# Source: {value_with_source[1]}\n")
            needs_separation = True
        if include_env_vars:
            file.write(f"#\n# Variable: AIRFLOW__{section_to_write.upper()}__{option.upper()}\n")
            if extra_spacing:
                file.write("#\n")
            needs_separation = True
        return True, needs_separation

    def is_template(self, section: str, key) -> bool:
        """
        Return whether the value is templated.

        :param section: section of the config
        :param key: key in the section
        :return: True if the value is templated
        """
        return _is_template(self.configuration_description, section, key)

    def getsection(self, section: str, team_name: str | None = None) -> ConfigOptionsDictType | None:
        """
        Return the section as a dict.

        Values are converted to int, float, bool as required.

        :param section: section from the config
        :param team_name: optional team name for team-specific configuration lookup
        """
        # Handle team-specific section lookup for config file
        config_section = f"{team_name}={section}" if team_name else section

        if not self.has_section(config_section) and not self._default_values.has_section(config_section):
            return None
        if self._default_values.has_section(config_section):
            _section: ConfigOptionsDictType = dict(self._default_values.items(config_section))
        else:
            _section = {}

        if self.has_section(config_section):
            _section.update(self.items(config_section))

        # Use section (not config_section) for env var lookup - team_name is handled by _env_var_name
        section_prefix = self._env_var_name(section, "", team_name=team_name)
        for env_var in sorted(os.environ.keys()):
            if env_var.startswith(section_prefix):
                key = env_var.replace(section_prefix, "")
                if key.endswith("_CMD"):
                    key = key[:-4]
                key = key.lower()
                _section[key] = self._get_env_var_option(section, key, team_name=team_name)

        for key, val in _section.items():
            if val is None:
                raise AirflowConfigException(
                    f"Failed to convert value automatically. "
                    f'Please check "{key}" key in "{section}" section is set.'
                )
            try:
                _section[key] = int(val)
            except ValueError:
                try:
                    _section[key] = float(val)
                except ValueError:
                    if isinstance(val, str) and val.lower() in ("t", "true"):
                        _section[key] = True
                    elif isinstance(val, str) and val.lower() in ("f", "false"):
                        _section[key] = False
        return _section

    @staticmethod
    def _write_section_header(
        file: IO[str],
        include_descriptions: bool,
        section_config_description: dict[str, str],
        section_to_write: str,
    ) -> None:
        """Write header for configuration section."""
        file.write(f"[{section_to_write}]\n")
        section_description = section_config_description.get("description")
        if section_description and include_descriptions:
            for line in section_description.splitlines():
                file.write(f"# {line}\n")
            file.write("\n")

    def _write_value(
        self,
        file: IO[str],
        option: str,
        comment_out_everything: bool,
        needs_separation: bool,
        only_defaults: bool,
        section_to_write: str,
    ):
        default_value = self.get_default_value(section_to_write, option, raw=True)
        if only_defaults:
            value = default_value
        else:
            value = self.get(section_to_write, option, fallback=default_value, raw=True)
        if value is None:
            file.write(f"# {option} = \n")
        else:
            if comment_out_everything:
                value_lines = value.splitlines()
                value = "\n# ".join(value_lines)
                file.write(f"# {option} = {value}\n")
            else:
                if "\n" in value:
                    try:
                        value = json.dumps(json.loads(value), indent=4)
                        value = value.replace(
                            "\n", "\n    "
                        )  # indent multi-line JSON to satisfy configparser format
                    except JSONDecodeError:
                        pass
                file.write(f"{option} = {value}\n")
        if needs_separation:
            file.write("\n")

    def write(  # type: ignore[override]
        self,
        file: IO[str],
        section: str | None = None,
        include_examples: bool = True,
        include_descriptions: bool = True,
        include_sources: bool = True,
        include_env_vars: bool = True,
        include_providers: bool = True,
        comment_out_everything: bool = False,
        hide_sensitive_values: bool = False,
        extra_spacing: bool = True,
        only_defaults: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Write configuration with comments and examples to a file.

        :param file: file to write to
        :param section: section of the config to write, defaults to all sections
        :param include_examples: Include examples in the output
        :param include_descriptions: Include descriptions in the output
        :param include_sources: Include the source of each config option
        :param include_env_vars: Include environment variables corresponding to each config option
        :param include_providers: Include providers configuration
        :param comment_out_everything: Comment out all values
        :param hide_sensitive_values: Include sensitive values in the output
        :param extra_spacing: Add extra spacing before examples and after variables
        :param only_defaults: Only include default values when writing the config, not the actual values
        """
        sources_dict = {}
        if include_sources:
            sources_dict = self.as_dict(display_source=True)
        with self.make_sure_configuration_loaded(with_providers=include_providers):
            for section_to_write in self.get_sections_including_defaults():
                section_config_description = self.configuration_description.get(section_to_write, {})
                if section_to_write != section and section is not None:
                    continue
                if self._default_values.has_section(section_to_write) or self.has_section(section_to_write):
                    self._write_section_header(
                        file, include_descriptions, section_config_description, section_to_write
                    )
                    for option in self.get_options_including_defaults(section_to_write):
                        should_continue, needs_separation = self._write_option_header(
                            file=file,
                            option=option,
                            extra_spacing=extra_spacing,
                            include_descriptions=include_descriptions,
                            include_env_vars=include_env_vars,
                            include_examples=include_examples,
                            include_sources=include_sources,
                            section_config_description=section_config_description,
                            section_to_write=section_to_write,
                            sources_dict=sources_dict,
                        )
                        self._write_value(
                            file=file,
                            option=option,
                            comment_out_everything=comment_out_everything,
                            needs_separation=needs_separation,
                            only_defaults=only_defaults,
                            section_to_write=section_to_write,
                        )
                    if include_descriptions and not needs_separation:
                        # extra separation between sections in case last option did not need it
                        file.write("\n")

    @contextmanager
    def make_sure_configuration_loaded(self, with_providers: bool) -> Generator[None, None, None]:
        """
        Make sure configuration is loaded with or without providers.

        This happens regardless if the provider configuration has been loaded before or not.
        Restores configuration to the state before entering the context.

        :param with_providers: whether providers should be loaded
        """
        needs_reload = False
        if with_providers:
            self._ensure_providers_config_loaded()
        else:
            needs_reload = self._ensure_providers_config_unloaded()
        yield
        if needs_reload:
            self._reload_provider_configs()

    def _ensure_providers_config_loaded(self) -> None:
        """Ensure providers configurations are loaded."""
        raise NotImplementedError("Subclasses must implement _ensure_providers_config_loaded method")

    def _ensure_providers_config_unloaded(self) -> bool:
        """Ensure providers configurations are unloaded temporarily to load core configs. Returns True if providers get unloaded."""
        raise NotImplementedError("Subclasses must implement _ensure_providers_config_unloaded method")

    def _reload_provider_configs(self) -> None:
        """Reload providers configuration."""
        raise NotImplementedError("Subclasses must implement _reload_provider_configs method")
