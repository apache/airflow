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
from collections.abc import Iterable
from configparser import ConfigParser, NoOptionError, NoSectionError
from contextlib import contextmanager
from enum import Enum
from json.decoder import JSONDecodeError
from typing import IO, Any, TypeVar, overload

from .exceptions import AirflowConfigException

log = logging.getLogger(__name__)


ConfigType = str | int | float | bool
ConfigOptionsDictType = dict[str, ConfigType]
ConfigSectionSourcesType = dict[str, str | tuple[str, str]]
ConfigSourcesType = dict[str, ConfigSectionSourcesType]
ENV_VAR_PREFIX = "AIRFLOW__"


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


class AirflowConfigParser(ConfigParser):
    """
    Base configuration parser with pure parsing logic.

    This class provides the core parsing methods that work with:
    - configuration_description: dict describing config options (set by subclasses)
    - _default_values: ConfigParser with default values (set by subclasses)
    - deprecated_options: class attribute mapping new -> old options
    - deprecated_sections: class attribute mapping new -> old sections

    Subclasses should:
    1. Set configuration_description and _default_values in __init__
    2. Override get_provider_config_fallback_defaults() if needed
    3. Override _get_config_value_from_secret_backend() if needed
    """

    # A mapping of (new section, new option) -> (old section, old option, since_version).
    # When reading new option, the old option will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old option will be used instead
    deprecated_options: dict[tuple[str, str], tuple[str, str, str]] = {
        ("dag_processor", "refresh_interval"): ("scheduler", "dag_dir_list_interval", "3.0"),
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
    }

    # A mapping of new section -> (old section, since_version).
    deprecated_sections: dict[str, tuple[str, str]] = {}

    def _raise_config_exception(self, message: str) -> None:
        """
        Raise an AirflowConfigException.

        This is a stub called by the shared parser's methods when configuration errors occur.
        Subclasses can override this to raise their own exception type.

        :param message: Exception message
        """
        raise AirflowConfigException(message)

    def __init__(self, *args, **kwargs):
        """
        Initialize the parser.

        Subclasses should call super().__init__() and then set:
        - self.configuration_description
        - self._default_values
        - self._suppress_future_warnings (default False)
        """
        super().__init__(*args, **kwargs)
        # These should be set by subclasses:
        self.configuration_description: dict[str, dict[str, Any]] | None = None
        self._default_values: ConfigParser | None = None
        self._suppress_future_warnings: bool = False

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
        if self.configuration_description is None:
            return set()
        flattened = {
            (s, k): item
            for s, s_c in self.configuration_description.items()
            for k, item in s_c.get("options", {}).items()  # type: ignore[union-attr]
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

    def get_default_value(self, section: str, key: str, fallback: Any = None, raw=False, **kwargs) -> Any:
        """
        Retrieve default value from default config parser.

        This is a stub called by the shared parser's get() method as part of the lookup chain.
        Subclasses can override this to customize how default values are retrieved.

        :param section: section of the config
        :param key: key in the section
        :param fallback: fallback value if not found
        :param raw: if True, return raw value without interpolation
        :param kwargs: additional kwargs passed to ConfigParser.get()
        :return: default value or fallback
        """
        if self._default_values is None:
            return fallback
        try:
            if raw:
                return self._default_values.get(section, key, **kwargs)
            return self._default_values.get(section, key, **kwargs)
        except (NoOptionError, NoSectionError):
            return fallback

    # TODO: Remove this from shared, after https://github.com/apache/airflow/pull/57970 is merged
    def get_provider_config_fallback_defaults(self, section: str, key: str, **kwargs) -> Any:
        """
        Get provider config fallback default values.

        This is a stub called by the shared parser's get() method as part of the lookup chain.
        Subclasses can override this to provide provider-specific fallback defaults.
        Default implementation returns None (no provider fallbacks).

        :param section: section name
        :param key: key name
        :param kwargs: additional kwargs
        :return: fallback value or None
        """
        return None

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
            raise self._raise_config_exception(
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

    # _get_secret_option is now provided by the shared base class.

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
                    raise self._raise_config_exception(
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
        team_name: str | None = None,
    ) -> str | None:
        """Get config option from environment variables."""
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
        return None

    def _get_option_from_config_file(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        kwargs: dict[str, Any],
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
    ) -> str | None:
        """Get config option from config file."""
        if super().has_option(section, key):
            return expand_env_var(super().get(section, key, **kwargs))
        if deprecated_section and deprecated_key:
            if super().has_option(deprecated_section, deprecated_key):
                if issue_warning:
                    self._warn_deprecate(section, key, deprecated_section, deprecated_key, extra_stacklevel)
                with self.suppress_future_warnings():
                    return expand_env_var(super().get(deprecated_section, deprecated_key, **kwargs))
        return None

    def _get_option_from_commands(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
    ) -> str | None:
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
        return None

    def _get_option_from_secrets(
        self,
        deprecated_key: str | None,
        deprecated_section: str | None,
        key: str,
        section: str,
        issue_warning: bool = True,
        extra_stacklevel: int = 0,
    ) -> str | None:
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
        return None

    def get(  # type: ignore[misc]
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
        Get config value with priority: env vars > config file > commands > secrets > defaults.

        This is the main method for retrieving configuration values.
        """
        section = section.lower()
        key = key.lower()
        warning_emitted = False
        deprecated_section: str | None = None
        deprecated_key: str | None = None

        if lookup_from_deprecated:
            option_description = (
                self.configuration_description.get(section, {}).get("options", {}).get(key, {})  # type: ignore[union-attr]
                if self.configuration_description
                else {}
            )
            if option_description.get("deprecated"):
                deprecation_reason = option_description.get("deprecation_reason", "")
                warnings.warn(
                    f"The '{key}' option in section {section} is deprecated. {deprecation_reason}",
                    DeprecationWarning,
                    stacklevel=2 + _extra_stacklevel,
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
                        stacklevel=2 + _extra_stacklevel,
                    )
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
                        stacklevel=2 + _extra_stacklevel,
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
        # first check environment variables
        option = self._get_environment_variables(
            deprecated_key,
            deprecated_section,
            key,
            section,
            issue_warning=not warning_emitted,
            extra_stacklevel=_extra_stacklevel,
            team_name=team_name,
        )
        if option is not None:
            return option

        # ...then the config file
        option = self._get_option_from_config_file(
            deprecated_key,
            deprecated_section,
            key,
            kwargs,
            section,
            issue_warning=not warning_emitted,
            extra_stacklevel=_extra_stacklevel,
        )
        if option is not None:
            return option

        # ...then commands
        option = self._get_option_from_commands(
            deprecated_key,
            deprecated_section,
            key,
            section,
            issue_warning=not warning_emitted,
            extra_stacklevel=_extra_stacklevel,
        )
        if option is not None:
            return option

        # ...then from secret backends
        option = self._get_option_from_secrets(
            deprecated_key,
            deprecated_section,
            key,
            section,
            issue_warning=not warning_emitted,
            extra_stacklevel=_extra_stacklevel,
        )
        if option is not None:
            return option

        # ...then the default config
        if self.get_default_value(section, key) is not None or "fallback" in kwargs:
            return expand_env_var(self.get_default_value(section, key, **kwargs))

        if self.get_provider_config_fallback_defaults(section, key) is not None:
            # no expansion needed
            return self.get_provider_config_fallback_defaults(section, key, **kwargs)

        if not suppress_warnings:
            log.warning("section/key [%s/%s] not found in config", section, key)

        raise self._raise_config_exception(f"section/key [{section}/{key}] not found in config")

    def getboolean(self, section: str, key: str, **kwargs) -> bool:  # type: ignore[override]
        """Get config value as boolean."""
        val = str(self.get(section, key, _extra_stacklevel=1, **kwargs)).lower().strip()
        if "#" in val:
            val = val.split("#")[0].strip()
        if val in ("t", "true", "1"):
            return True
        if val in ("f", "false", "0"):
            return False
        raise self._raise_config_exception(
            f'Failed to convert value to bool. Please check "{key}" key in "{section}" section. '
            f'Current value: "{val}".'
        )

    def getint(self, section: str, key: str, **kwargs) -> int:  # type: ignore[override]
        """Get config value as integer."""
        val = self.get(section, key, _extra_stacklevel=1, **kwargs)
        if val is None:
            raise self._raise_config_exception(
                f"Failed to convert value None to int. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return int(val)
        except ValueError:
            raise self._raise_config_exception(
                f'Failed to convert value to int. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getfloat(self, section: str, key: str, **kwargs) -> float:  # type: ignore[override]
        """Get config value as float."""
        val = self.get(section, key, _extra_stacklevel=1, **kwargs)
        if val is None:
            raise self._raise_config_exception(
                f"Failed to convert value None to float. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return float(val)
        except ValueError:
            raise self._raise_config_exception(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getlist(self, section: str, key: str, delimiter=",", **kwargs):
        """Get config value as list."""
        val = self.get(section, key, **kwargs)
        if val is None:
            if "fallback" in kwargs:
                return kwargs["fallback"]
            raise self._raise_config_exception(
                f"Failed to convert value None to list. "
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return [item.strip() for item in val.split(delimiter)]
        except Exception:
            raise self._raise_config_exception(
                f'Failed to parse value to a list. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    E = TypeVar("E", bound=Enum)

    def getenum(self, section: str, key: str, enum_class: type[E], **kwargs) -> E:
        """Get config value as enum."""
        val = self.get(section, key, **kwargs)
        enum_names = [enum_item.name for enum_item in enum_class]

        if val is None:
            raise self._raise_config_exception(
                f'Failed to convert value. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}" and it must be one of {", ".join(enum_names)}'
            )

        try:
            return enum_class[val]
        except KeyError:
            if "fallback" in kwargs and kwargs["fallback"] in enum_names:
                return enum_class[kwargs["fallback"]]
            raise self._raise_config_exception(
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
            from airflow.utils.module_loading import import_string

            return import_string(full_qualified_path)
        except ImportError as e:
            log.warning(e)
            raise self._raise_config_exception(
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
            raise self._raise_config_exception(f"Unable to parse [{section}] {key!r} as valid json") from e

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
                raise self._raise_config_exception(
                    f'Failed to convert value to int. Please check "{key}" key in "{section}" section. '
                    f'Current value: "{val}".'
                )

            try:
                return datetime.timedelta(seconds=int_val)
            except OverflowError as err:
                raise self._raise_config_exception(
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

    def has_section(self, section: str) -> bool:
        """
        Check if section exists in config or configuration_description.

        :param section: section name to check
        :return: True if section exists in config or configuration_description
        """
        if super().has_section(section):
            return True
        if self.configuration_description and section in self.configuration_description:
            return True
        return False

    def _update_defaults_from_string(self, config_string: str) -> None:
        """
        Update the defaults in _default_values based on values in config_string ("ini" format).

        Used for testing purposes.
        """
        parser = ConfigParser()
        parser.read_string(config_string)
        for section in parser.sections():
            if self._default_values is None:
                raise AirflowConfigException(
                    "_default_values must be set before calling _update_defaults_from_string"
                )
            if section not in self._default_values.sections():
                self._default_values.add_section(section)
            for key, value in parser.items(section):
                self._default_values.set(section, key, value)

    def get_sections_including_defaults(self) -> list[str]:
        """
        Retrieve all sections from the configuration parser, including sections defined by built-in defaults.

        :return: list of section names
        """
        sections_from_config = self.sections()
        sections_from_description = (
            list(self.configuration_description.keys()) if self.configuration_description else []
        )
        return list(dict.fromkeys(itertools.chain(sections_from_description, sections_from_config)))

    def get_options_including_defaults(self, section: str) -> list[str]:
        """
        Retrieve all possible options from the configuration parser for the section given.

        Includes options defined by built-in defaults.

        :param section: section name
        :return: list of option names for the section given
        """
        my_own_options = self.options(section) if self.has_section(section) else []
        all_options_from_defaults = (
            list(self.configuration_description.get(section, {}).get("options", {}).keys())
            if self.configuration_description
            else []
        )
        return list(dict.fromkeys(itertools.chain(all_options_from_defaults, my_own_options)))

    def has_option(self, section: str, option: str, lookup_from_deprecated: bool = True) -> bool:
        """
        Check if option is defined.

        Uses self.get() to avoid reimplementing the priority order of config variables
        (env, config, cmd, defaults).

        :param section: section to get option from
        :param option: option to get
        :param lookup_from_deprecated: If True, check if the option is defined in deprecated sections
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

        if self._default_values and self.get_default_value(section, option) is not None and remove_default:
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
    ) -> None:
        """
        Write configuration value to file.

        :param file: File to write to
        :param option: Option name
        :param comment_out_everything: If True, comment out the value
        :param needs_separation: If True, add blank line after value
        :param only_defaults: If True, write only default value, not actual value
        :param section_to_write: Section name
        """
        if self._default_values is None:
            default_value = None
        else:
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
                file.write(f"{option} = {value}\n")
        if needs_separation:
            file.write("\n")
