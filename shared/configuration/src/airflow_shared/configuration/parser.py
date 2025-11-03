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
import json
import logging
import os
import shlex
import subprocess
import sys
import warnings
from configparser import ConfigParser, NoOptionError, NoSectionError
from contextlib import contextmanager
from enum import Enum
from json.decoder import JSONDecodeError
from typing import Any, TypeVar, overload

from .exceptions import AirflowConfigException

log = logging.getLogger(__name__)

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

    def _get_config_value_from_secret_backend(self, config_key: str) -> str | None:
        """
        Get Config option values from Secret Backend.

        This is a stub called by the shared parser's _get_secret_option() method as part of the lookup chain.
        Subclasses can override this to integrate with their secrets backend system.
        Default implementation returns None (no secrets backend).

        :param config_key: the config key to retrieve
        :return: config value or None
        """
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
