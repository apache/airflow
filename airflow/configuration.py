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
import datetime
import functools
import json
import logging
import multiprocessing
import os
import pathlib
import re
import shlex
import subprocess
import sys
import warnings
from base64 import b64encode
from collections import OrderedDict

# Ignored Mypy on configparser because it thinks the configparser module has no _UNSET attribute
from configparser import _UNSET, ConfigParser, NoOptionError, NoSectionError  # type: ignore
from contextlib import suppress
from json.decoder import JSONDecodeError
from re import Pattern
from typing import IO, Any, Dict, Iterable, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

from typing_extensions import overload

from airflow.exceptions import AirflowConfigException
from airflow.secrets import DEFAULT_SECRETS_SEARCH_PATH, BaseSecretsBackend
from airflow.utils import yaml
from airflow.utils.module_loading import import_string
from airflow.utils.weight_rule import WeightRule

log = logging.getLogger(__name__)

# show Airflow's deprecation warnings
if not sys.warnoptions:
    warnings.filterwarnings(action='default', category=DeprecationWarning, module='airflow')
    warnings.filterwarnings(action='default', category=PendingDeprecationWarning, module='airflow')

_SQLITE3_VERSION_PATTERN = re.compile(r"(?P<version>^\d+(?:\.\d+)*)\D?.*$")

ConfigType = Union[str, int, float, bool]
ConfigOptionsDictType = Dict[str, ConfigType]
ConfigSectionSourcesType = Dict[str, Union[str, Tuple[str, str]]]
ConfigSourcesType = Dict[str, ConfigSectionSourcesType]

ENV_VAR_PREFIX = 'AIRFLOW__'


def _parse_sqlite_version(s: str) -> Tuple[int, ...]:
    match = _SQLITE3_VERSION_PATTERN.match(s)
    if match is None:
        return ()
    return tuple(int(p) for p in match.group("version").split("."))


@overload
def expand_env_var(env_var: None) -> None:
    ...


@overload
def expand_env_var(env_var: str) -> str:
    ...


def expand_env_var(env_var: Union[str, None]) -> Optional[Union[str, None]]:
    """
    Expands (potentially nested) env vars by repeatedly applying
    `expandvars` and `expanduser` until interpolation stops having
    any effect.
    """
    if not env_var:
        return env_var
    while True:
        interpolated = os.path.expanduser(os.path.expandvars(str(env_var)))
        if interpolated == env_var:
            return interpolated
        else:
            env_var = interpolated


def run_command(command: str) -> str:
    """Runs command and returns stdout"""
    process = subprocess.Popen(
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True
    )
    output, stderr = (stream.decode(sys.getdefaultencoding(), 'ignore') for stream in process.communicate())

    if process.returncode != 0:
        raise AirflowConfigException(
            f"Cannot execute {command}. Error code is: {process.returncode}. "
            f"Output: {output}, Stderr: {stderr}"
        )

    return output


def _get_config_value_from_secret_backend(config_key: str) -> Optional[str]:
    """Get Config option values from Secret Backend"""
    try:
        secrets_client = get_custom_secret_backend()
        if not secrets_client:
            return None
        return secrets_client.get_config(config_key)
    except Exception as e:
        raise AirflowConfigException(
            'Cannot retrieve config from alternative secrets backend. '
            'Make sure it is configured properly and that the Backend '
            'is accessible.\n'
            f'{e}'
        )


def _default_config_file_path(file_name: str) -> str:
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    return os.path.join(templates_dir, file_name)


def default_config_yaml() -> List[Dict[str, Any]]:
    """
    Read Airflow configs from YAML file

    :return: Python dictionary containing configs & their info
    """
    with open(_default_config_file_path('config.yml')) as config_file:
        return yaml.safe_load(config_file)


class AirflowConfigParser(ConfigParser):
    """Custom Airflow Configparser supporting defaults and deprecated options"""

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}_cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    # These configs can also be fetched from Secrets backend
    # following the "{section}__{name}__secret" pattern
    sensitive_config_values: Set[Tuple[str, str]] = {
        ('database', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'flower_basic_auth'),
        ('celery', 'result_backend'),
        ('atlas', 'password'),
        ('smtp', 'smtp_password'),
        ('webserver', 'secret_key'),
        # The following options are deprecated
        ('core', 'sql_alchemy_conn'),
    }

    # A mapping of (new section, new option) -> (old section, old option, since_version).
    # When reading new option, the old option will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old option will be used instead
    deprecated_options: Dict[Tuple[str, str], Tuple[str, str, str]] = {
        ('celery', 'worker_precheck'): ('core', 'worker_precheck', '2.0.0'),
        ('logging', 'base_log_folder'): ('core', 'base_log_folder', '2.0.0'),
        ('logging', 'remote_logging'): ('core', 'remote_logging', '2.0.0'),
        ('logging', 'remote_log_conn_id'): ('core', 'remote_log_conn_id', '2.0.0'),
        ('logging', 'remote_base_log_folder'): ('core', 'remote_base_log_folder', '2.0.0'),
        ('logging', 'encrypt_s3_logs'): ('core', 'encrypt_s3_logs', '2.0.0'),
        ('logging', 'logging_level'): ('core', 'logging_level', '2.0.0'),
        ('logging', 'fab_logging_level'): ('core', 'fab_logging_level', '2.0.0'),
        ('logging', 'logging_config_class'): ('core', 'logging_config_class', '2.0.0'),
        ('logging', 'colored_console_log'): ('core', 'colored_console_log', '2.0.0'),
        ('logging', 'colored_log_format'): ('core', 'colored_log_format', '2.0.0'),
        ('logging', 'colored_formatter_class'): ('core', 'colored_formatter_class', '2.0.0'),
        ('logging', 'log_format'): ('core', 'log_format', '2.0.0'),
        ('logging', 'simple_log_format'): ('core', 'simple_log_format', '2.0.0'),
        ('logging', 'task_log_prefix_template'): ('core', 'task_log_prefix_template', '2.0.0'),
        ('logging', 'log_filename_template'): ('core', 'log_filename_template', '2.0.0'),
        ('logging', 'log_processor_filename_template'): ('core', 'log_processor_filename_template', '2.0.0'),
        ('logging', 'dag_processor_manager_log_location'): (
            'core',
            'dag_processor_manager_log_location',
            '2.0.0',
        ),
        ('logging', 'task_log_reader'): ('core', 'task_log_reader', '2.0.0'),
        ('metrics', 'statsd_on'): ('scheduler', 'statsd_on', '2.0.0'),
        ('metrics', 'statsd_host'): ('scheduler', 'statsd_host', '2.0.0'),
        ('metrics', 'statsd_port'): ('scheduler', 'statsd_port', '2.0.0'),
        ('metrics', 'statsd_prefix'): ('scheduler', 'statsd_prefix', '2.0.0'),
        ('metrics', 'statsd_allow_list'): ('scheduler', 'statsd_allow_list', '2.0.0'),
        ('metrics', 'stat_name_handler'): ('scheduler', 'stat_name_handler', '2.0.0'),
        ('metrics', 'statsd_datadog_enabled'): ('scheduler', 'statsd_datadog_enabled', '2.0.0'),
        ('metrics', 'statsd_datadog_tags'): ('scheduler', 'statsd_datadog_tags', '2.0.0'),
        ('metrics', 'statsd_custom_client_path'): ('scheduler', 'statsd_custom_client_path', '2.0.0'),
        ('scheduler', 'parsing_processes'): ('scheduler', 'max_threads', '1.10.14'),
        ('scheduler', 'scheduler_idle_sleep_time'): ('scheduler', 'processor_poll_interval', '2.2.0'),
        ('operators', 'default_queue'): ('celery', 'default_queue', '2.1.0'),
        ('core', 'hide_sensitive_var_conn_fields'): ('admin', 'hide_sensitive_variable_fields', '2.1.0'),
        ('core', 'sensitive_var_conn_names'): ('admin', 'sensitive_variable_fields', '2.1.0'),
        ('core', 'default_pool_task_slot_count'): ('core', 'non_pooled_task_slot_count', '1.10.4'),
        ('core', 'max_active_tasks_per_dag'): ('core', 'dag_concurrency', '2.2.0'),
        ('logging', 'worker_log_server_port'): ('celery', 'worker_log_server_port', '2.2.0'),
        ('api', 'access_control_allow_origins'): ('api', 'access_control_allow_origin', '2.2.0'),
        ('api', 'auth_backends'): ('api', 'auth_backend', '2.3.0'),
        ('database', 'sql_alchemy_conn'): ('core', 'sql_alchemy_conn', '2.3.0'),
        ('database', 'sql_engine_encoding'): ('core', 'sql_engine_encoding', '2.3.0'),
        ('database', 'sql_engine_collation_for_ids'): ('core', 'sql_engine_collation_for_ids', '2.3.0'),
        ('database', 'sql_alchemy_pool_enabled'): ('core', 'sql_alchemy_pool_enabled', '2.3.0'),
        ('database', 'sql_alchemy_pool_size'): ('core', 'sql_alchemy_pool_size', '2.3.0'),
        ('database', 'sql_alchemy_max_overflow'): ('core', 'sql_alchemy_max_overflow', '2.3.0'),
        ('database', 'sql_alchemy_pool_recycle'): ('core', 'sql_alchemy_pool_recycle', '2.3.0'),
        ('database', 'sql_alchemy_pool_pre_ping'): ('core', 'sql_alchemy_pool_pre_ping', '2.3.0'),
        ('database', 'sql_alchemy_schema'): ('core', 'sql_alchemy_schema', '2.3.0'),
        ('database', 'sql_alchemy_connect_args'): ('core', 'sql_alchemy_connect_args', '2.3.0'),
        ('database', 'load_default_connections'): ('core', 'load_default_connections', '2.3.0'),
        ('database', 'max_db_retries'): ('core', 'max_db_retries', '2.3.0'),
    }

    # A mapping of old default values that we want to change and warn the user
    # about. Mapping of section -> setting -> { old, replace, by_version }
    deprecated_values: Dict[str, Dict[str, Tuple[Pattern, str, str]]] = {
        'core': {
            'hostname_callable': (re.compile(r':'), r'.', '2.1'),
        },
        'webserver': {
            'navbar_color': (re.compile(r'\A#007A87\Z', re.IGNORECASE), '#fff', '2.1'),
            'dag_default_view': (re.compile(r'^tree$'), 'grid', '3.0'),
        },
        'email': {
            'email_backend': (
                re.compile(r'^airflow\.contrib\.utils\.sendgrid\.send_email$'),
                r'airflow.providers.sendgrid.utils.emailer.send_email',
                '2.1',
            ),
        },
        'logging': {
            'log_filename_template': (
                re.compile(re.escape("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")),
                "XX-set-after-default-config-loaded-XX",
                '3.0',
            ),
        },
        'api': {
            'auth_backends': (
                re.compile(r'^airflow\.api\.auth\.backend\.deny_all$|^$'),
                'airflow.api.auth.backend.session',
                '3.0',
            ),
        },
        'elasticsearch': {
            'log_id_template': (
                re.compile('^' + re.escape('{dag_id}-{task_id}-{execution_date}-{try_number}') + '$'),
                '{dag_id}-{task_id}-{run_id}-{map_index}-{try_number}',
                '3.0',
            )
        },
    }

    _available_logging_levels = ['CRITICAL', 'FATAL', 'ERROR', 'WARN', 'WARNING', 'INFO', 'DEBUG']
    enums_options = {
        ("core", "default_task_weight_rule"): sorted(WeightRule.all_weight_rules()),
        ("core", "dag_ignore_file_syntax"): ["regexp", "glob"],
        ('core', 'mp_start_method'): multiprocessing.get_all_start_methods(),
        ("scheduler", "file_parsing_sort_mode"): ["modified_time", "random_seeded_by_host", "alphabetical"],
        ("logging", "logging_level"): _available_logging_levels,
        ("logging", "fab_logging_level"): _available_logging_levels,
        # celery_logging_level can be empty, which uses logging_level as fallback
        ("logging", "celery_logging_level"): _available_logging_levels + [''],
        ("webserver", "analytical_tool"): ['google_analytics', 'metarouter', 'segment', ''],
    }

    upgraded_values: Dict[Tuple[str, str], str]
    """Mapping of (section,option) to the old value that was upgraded"""

    # This method transforms option names on every read, get, or set operation.
    # This changes from the default behaviour of ConfigParser from lower-casing
    # to instead be case-preserving
    def optionxform(self, optionstr: str) -> str:
        return optionstr

    def __init__(self, default_config: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.upgraded_values = {}

        self.airflow_defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.airflow_defaults.read_string(default_config)
            # Set the upgrade value based on the current loaded default
            default = self.airflow_defaults.get('logging', 'log_filename_template', fallback=None)
            if default:
                replacement = self.deprecated_values['logging']['log_filename_template']
                self.deprecated_values['logging']['log_filename_template'] = (
                    replacement[0],
                    default,
                    replacement[2],
                )
            else:
                # In case of tests it might not exist
                with suppress(KeyError):
                    del self.deprecated_values['logging']['log_filename_template']
        else:
            with suppress(KeyError):
                del self.deprecated_values['logging']['log_filename_template']

        self.is_validated = False

    def validate(self):
        self._validate_config_dependencies()
        self._validate_enums()

        for section, replacement in self.deprecated_values.items():
            for name, info in replacement.items():
                old, new, version = info
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
                        version=version,
                    )

        self._upgrade_auth_backends()
        self._upgrade_postgres_metastore_conn()
        self.is_validated = True

    def _upgrade_auth_backends(self):
        """
        Ensure a custom auth_backends setting contains session,
        which is needed by the UI for ajax queries.
        """
        old_value = self.get("api", "auth_backends", fallback="")
        if old_value in ('airflow.api.auth.backend.default', ''):
            # handled by deprecated_values
            pass
        elif old_value.find('airflow.api.auth.backend.session') == -1:
            new_value = old_value + ",airflow.api.auth.backend.session"
            self._update_env_var(section="api", name="auth_backends", new_value=new_value)
            self.upgraded_values[("api", "auth_backends")] = old_value

            # if the old value is set via env var, we need to wipe it
            # otherwise, it'll "win" over our adjusted value
            old_env_var = self._env_var_name("api", "auth_backend")
            os.environ.pop(old_env_var, None)

            warnings.warn(
                'The auth_backends setting in [api] has had airflow.api.auth.backend.session added '
                'in the running config, which is needed by the UI. Please update your config before '
                'Apache Airflow 3.0.',
                FutureWarning,
            )

    def _upgrade_postgres_metastore_conn(self):
        """
        As of SQLAlchemy 1.4, schemes `postgres+psycopg2` and `postgres`
        must be replaced with `postgresql`.
        """
        section, key = 'database', 'sql_alchemy_conn'
        old_value = self.get(section, key)
        bad_schemes = ['postgres+psycopg2', 'postgres']
        good_scheme = 'postgresql'
        parsed = urlparse(old_value)
        if parsed.scheme in bad_schemes:
            warnings.warn(
                f"Bad scheme in Airflow configuration core > sql_alchemy_conn: `{parsed.scheme}`. "
                "As of SQLAlchemy 1.4 (adopted in Airflow 2.3) this is no longer supported.  You must "
                f"change to `{good_scheme}` before the next Airflow release.",
                FutureWarning,
            )
            self.upgraded_values[(section, key)] = old_value
            new_value = re.sub('^' + re.escape(f"{parsed.scheme}://"), f"{good_scheme}://", old_value)
            self._update_env_var(section=section, name=key, new_value=new_value)

            # if the old value is set via env var, we need to wipe it
            # otherwise, it'll "win" over our adjusted value
            old_env_var = self._env_var_name("core", key)
            os.environ.pop(old_env_var, None)

    def _validate_enums(self):
        """Validate that enum type config has an accepted value"""
        for (section_key, option_key), enum_options in self.enums_options.items():
            if self.has_option(section_key, option_key):
                value = self.get(section_key, option_key)
                if value not in enum_options:
                    raise AirflowConfigException(
                        f"`[{section_key}] {option_key}` should not be "
                        f"{value!r}. Possible values: {', '.join(enum_options)}."
                    )

    def _validate_config_dependencies(self):
        """
        Validate that config values aren't invalid given other config values
        or system-level limitations and requirements.
        """
        is_executor_without_sqlite_support = self.get("core", "executor") not in (
            'DebugExecutor',
            'SequentialExecutor',
        )
        is_sqlite = "sqlite" in self.get('database', 'sql_alchemy_conn')
        if is_sqlite and is_executor_without_sqlite_support:
            raise AirflowConfigException(f"error: cannot use sqlite with the {self.get('core', 'executor')}")
        if is_sqlite:
            import sqlite3

            from airflow.utils.docs import get_docs_url

            # Some features in storing rendered fields require sqlite version >= 3.15.0
            min_sqlite_version = (3, 15, 0)
            if _parse_sqlite_version(sqlite3.sqlite_version) < min_sqlite_version:
                min_sqlite_version_str = ".".join(str(s) for s in min_sqlite_version)
                raise AirflowConfigException(
                    f"error: sqlite C library version too old (< {min_sqlite_version_str}). "
                    f"See {get_docs_url('howto/set-up-database.html#setting-up-a-sqlite-database')}"
                )

    def _using_old_value(self, old: Pattern, current_value: str) -> bool:
        return old.search(current_value) is not None

    def _update_env_var(self, section: str, name: str, new_value: Union[str]):
        env_var = self._env_var_name(section, name)
        # Set it as an env var so that any subprocesses keep the same override!
        os.environ[env_var] = new_value

    @staticmethod
    def _create_future_warning(name: str, section: str, current_value: Any, new_value: Any, version: str):
        warnings.warn(
            f'The {name!r} setting in [{section}] has the old default value of {current_value!r}. '
            f'This value has been changed to {new_value!r} in the running config, but '
            f'please update your config before Apache Airflow {version}.',
            FutureWarning,
        )

    def _env_var_name(self, section: str, key: str) -> str:
        return f'{ENV_VAR_PREFIX}{section.upper()}__{key.upper()}'

    def _get_env_var_option(self, section: str, key: str):
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = self._env_var_name(section, key)
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])
        # alternatively AIRFLOW__{SECTION}__{KEY}_CMD (for a command)
        env_var_cmd = env_var + '_CMD'
        if env_var_cmd in os.environ:
            # if this is a valid command key...
            if (section, key) in self.sensitive_config_values:
                return run_command(os.environ[env_var_cmd])
        # alternatively AIRFLOW__{SECTION}__{KEY}_SECRET (to get from Secrets Backend)
        env_var_secret_path = env_var + '_SECRET'
        if env_var_secret_path in os.environ:
            # if this is a valid secret path...
            if (section, key) in self.sensitive_config_values:
                return _get_config_value_from_secret_backend(os.environ[env_var_secret_path])
        return None

    def _get_cmd_option(self, section: str, key: str):
        fallback_key = key + '_cmd'
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                command = super().get(section, fallback_key)
                return run_command(command)
        return None

    def _get_cmd_option_from_config_sources(
        self, config_sources: ConfigSourcesType, section: str, key: str
    ) -> Optional[str]:
        fallback_key = key + '_cmd'
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

    def _get_secret_option(self, section: str, key: str) -> Optional[str]:
        """Get Config option values from Secret Backend"""
        fallback_key = key + '_secret'
        if (section, key) in self.sensitive_config_values:
            if super().has_option(section, fallback_key):
                secrets_path = super().get(section, fallback_key)
                return _get_config_value_from_secret_backend(secrets_path)
        return None

    def _get_secret_option_from_config_sources(
        self, config_sources: ConfigSourcesType, section: str, key: str
    ) -> Optional[str]:
        fallback_key = key + '_secret'
        if (section, key) in self.sensitive_config_values:
            section_dict = config_sources.get(section)
            if section_dict is not None:
                secrets_path_value = section_dict.get(fallback_key)
                if secrets_path_value is not None:
                    if isinstance(secrets_path_value, str):
                        secrets_path = secrets_path_value
                    else:
                        secrets_path = secrets_path_value[0]
                    return _get_config_value_from_secret_backend(secrets_path)
        return None

    def get_mandatory_value(self, section: str, key: str, **kwargs) -> str:
        value = self.get(section, key, **kwargs)
        if value is None:
            raise ValueError(f"The value {section}/{key} should be set!")
        return value

    def get(self, section: str, key: str, **kwargs) -> Optional[str]:  # type: ignore[override]
        section = str(section).lower()
        key = str(key).lower()

        deprecated_section, deprecated_key, _ = self.deprecated_options.get(
            (section, key), (None, None, None)
        )

        option = self._get_environment_variables(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_config_file(deprecated_key, deprecated_section, key, kwargs, section)
        if option is not None:
            return option

        option = self._get_option_from_commands(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        option = self._get_option_from_secrets(deprecated_key, deprecated_section, key, section)
        if option is not None:
            return option

        return self._get_option_from_default_config(section, key, **kwargs)

    def _get_option_from_default_config(self, section: str, key: str, **kwargs) -> Optional[str]:
        # ...then the default config
        if self.airflow_defaults.has_option(section, key) or 'fallback' in kwargs:
            return expand_env_var(self.airflow_defaults.get(section, key, **kwargs))

        else:
            log.warning("section/key [%s/%s] not found in config", section, key)

            raise AirflowConfigException(f"section/key [{section}/{key}] not found in config")

    def _get_option_from_secrets(
        self, deprecated_key: Optional[str], deprecated_section: Optional[str], key: str, section: str
    ) -> Optional[str]:
        # ...then from secret backends
        option = self._get_secret_option(section, key)
        if option:
            return option
        if deprecated_section and deprecated_key:
            option = self._get_secret_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_commands(
        self, deprecated_key: Optional[str], deprecated_section: Optional[str], key: str, section: str
    ) -> Optional[str]:
        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_section and deprecated_key:
            option = self._get_cmd_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def _get_option_from_config_file(
        self,
        deprecated_key: Optional[str],
        deprecated_section: Optional[str],
        key: str,
        kwargs: Dict[str, Any],
        section: str,
    ) -> Optional[str]:
        # ...then the config file
        if super().has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(super().get(section, key, **kwargs))
        if deprecated_section and deprecated_key:
            if super().has_option(deprecated_section, deprecated_key):
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return expand_env_var(super().get(deprecated_section, deprecated_key, **kwargs))
        return None

    def _get_environment_variables(
        self, deprecated_key: Optional[str], deprecated_section: Optional[str], key: str, section: str
    ) -> Optional[str]:
        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        if deprecated_section and deprecated_key:
            option = self._get_env_var_option(deprecated_section, deprecated_key)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option
        return None

    def getboolean(self, section: str, key: str, **kwargs) -> bool:  # type: ignore[override]
        val = str(self.get(section, key, **kwargs)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val in ('t', 'true', '1'):
            return True
        elif val in ('f', 'false', '0'):
            return False
        else:
            raise AirflowConfigException(
                f'Failed to convert value to bool. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getint(self, section: str, key: str, **kwargs) -> int:  # type: ignore[override]
        val = self.get(section, key, **kwargs)
        if val is None:
            raise AirflowConfigException(
                f'Failed to convert value None to int. '
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
        val = self.get(section, key, **kwargs)
        if val is None:
            raise AirflowConfigException(
                f'Failed to convert value None to float. '
                f'Please check "{key}" key in "{section}" section is set.'
            )
        try:
            return float(val)
        except ValueError:
            raise AirflowConfigException(
                f'Failed to convert value to float. Please check "{key}" key in "{section}" section. '
                f'Current value: "{val}".'
            )

    def getimport(self, section: str, key: str, **kwargs) -> Any:
        """
        Reads options, imports the full qualified name, and returns the object.

        In case of failure, it throws an exception with the key and section names

        :return: The object or None, if the option is empty
        """
        full_qualified_path = conf.get(section=section, key=key, **kwargs)
        if not full_qualified_path:
            return None

        try:
            return import_string(full_qualified_path)
        except ImportError as e:
            log.error(e)
            raise AirflowConfigException(
                f'The object could not be loaded. Please check "{key}" key in "{section}" section. '
                f'Current value: "{full_qualified_path}".'
            )

    def getjson(
        self, section: str, key: str, fallback=_UNSET, **kwargs
    ) -> Union[dict, list, str, int, float, None]:
        """
        Return a config value parsed from a JSON string.

        ``fallback`` is *not* JSON parsed but used verbatim when no config value is given.
        """
        # get always returns the fallback value as a string, so for this if
        # someone gives us an object we want to keep that
        default = _UNSET
        if fallback is not _UNSET:
            default = fallback
            fallback = _UNSET

        try:
            data = self.get(section=section, key=key, fallback=fallback, **kwargs)
        except (NoSectionError, NoOptionError):
            return default

        if not data:
            return default if default is not _UNSET else None

        try:
            return json.loads(data)
        except JSONDecodeError as e:
            raise AirflowConfigException(f'Unable to parse [{section}] {key!r} as valid json') from e

    def gettimedelta(
        self, section: str, key: str, fallback: Any = None, **kwargs
    ) -> Optional[datetime.timedelta]:
        """
        Gets the config value for the given section and key, and converts it into datetime.timedelta object.
        If the key is missing, then it is considered as `None`.

        :param section: the section from the config
        :param key: the key defined in the given section
        :param fallback: fallback value when no config value is given, defaults to None
        :raises AirflowConfigException: raised because ValueError or OverflowError
        :return: datetime.timedelta(seconds=<config_value>) or None
        """
        val = self.get(section, key, fallback=fallback, **kwargs)

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
                    f'Failed to convert value to timedelta in `seconds`. '
                    f'{err}. '
                    f'Please check "{key}" key in "{section}" section. Current value: "{val}".'
                )

        return fallback

    def read(
        self,
        filenames: Union[
            str,
            bytes,
            os.PathLike,
            Iterable[Union[str, bytes, os.PathLike]],
        ],
        encoding=None,
    ):
        super().read(filenames=filenames, encoding=encoding)

    # The RawConfigParser defines "Mapping" from abc.collections is not subscriptable - so we have
    # to use Dict here.
    def read_dict(  # type: ignore[override]
        self, dictionary: Dict[str, Dict[str, Any]], source: str = '<dict>'
    ):
        super().read_dict(dictionary=dictionary, source=source)

    def has_option(self, section: str, option: str) -> bool:
        try:
            # Using self.get() to avoid reimplementing the priority order
            # of config variables (env, config, cmd, defaults)
            # UNSET to avoid logging a warning about missing values
            self.get(section, option, fallback=_UNSET)
            return True
        except (NoOptionError, NoSectionError):
            return False

    def remove_option(self, section: str, option: str, remove_default: bool = True):
        """
        Remove an option if it exists in config from a file or
        default config. If both of config have the same option, this removes
        the option in both configs unless remove_default=False.
        """
        if super().has_option(section, option):
            super().remove_option(section, option)

        if self.airflow_defaults.has_option(section, option) and remove_default:
            self.airflow_defaults.remove_option(section, option)

    def getsection(self, section: str) -> Optional[ConfigOptionsDictType]:
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.

        :param section: section from the config
        :rtype: dict
        """
        if not self.has_section(section) and not self.airflow_defaults.has_section(section):
            return None
        if self.airflow_defaults.has_section(section):
            _section: ConfigOptionsDictType = OrderedDict(self.airflow_defaults.items(section))
        else:
            _section = OrderedDict()

        if self.has_section(section):
            _section.update(OrderedDict(self.items(section)))

        section_prefix = self._env_var_name(section, '')
        for env_var in sorted(os.environ.keys()):
            if env_var.startswith(section_prefix):
                key = env_var.replace(section_prefix, '')
                if key.endswith("_CMD"):
                    key = key[:-4]
                key = key.lower()
                _section[key] = self._get_env_var_option(section, key)

        for key, val in _section.items():
            if val is None:
                raise AirflowConfigException(
                    f'Failed to convert value automatically. '
                    f'Please check "{key}" key in "{section}" section is set.'
                )
            try:
                _section[key] = int(val)
            except ValueError:
                try:
                    _section[key] = float(val)
                except ValueError:
                    if isinstance(val, str) and val.lower() in ('t', 'true'):
                        _section[key] = True
                    elif isinstance(val, str) and val.lower() in ('f', 'false'):
                        _section[key] = False
        return _section

    def write(self, fp: IO, space_around_delimiters: bool = True):  # type: ignore[override]
        # This is based on the configparser.RawConfigParser.write method code to add support for
        # reading options from environment variables.
        # Various type ignores below deal with less-than-perfect RawConfigParser superclass typing
        if space_around_delimiters:
            delimiter = f" {self._delimiters[0]} "  # type: ignore[attr-defined]
        else:
            delimiter = self._delimiters[0]  # type: ignore[attr-defined]
        if self._defaults:  # type: ignore
            self._write_section(  # type: ignore[attr-defined]
                fp, self.default_section, self._defaults.items(), delimiter  # type: ignore[attr-defined]
            )
        for section in self._sections:  # type: ignore[attr-defined]
            item_section: ConfigOptionsDictType = self.getsection(section)  # type: ignore[assignment]
            self._write_section(fp, section, item_section.items(), delimiter)  # type: ignore[attr-defined]

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
        Returns the current configuration as an OrderedDict of OrderedDicts.

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
        :param include_cmds: Should the result of calling any *_cmd config be
            set (True, default), or should the _cmd options be left as the
            command to run (False)
        :param include_secret: Should the result of calling any *_secret config be
            set (True, default), or should the _secret options be left as the
            path to get the secret from (False)
        :rtype: Dict[str, Dict[str, str]]
        :return: Dictionary, where the key is the name of the section and the content is
            the dictionary with the name of the parameter and its value.
        """
        config_sources: ConfigSourcesType = {}
        configs = [
            ('default', self.airflow_defaults),
            ('airflow.cfg', self),
        ]

        self._replace_config_with_display_sources(
            config_sources,
            configs,
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

        return config_sources

    def _include_secrets(
        self,
        config_sources: ConfigSourcesType,
        display_sensitive: bool,
        display_source: bool,
        raw: bool,
    ):
        for (section, key) in self.sensitive_config_values:
            value: Optional[str] = self._get_secret_option_from_config_sources(config_sources, section, key)
            if value:
                if not display_sensitive:
                    value = '< hidden >'
                if display_source:
                    opt: Union[str, Tuple[str, str]] = (value, 'secret')
                elif raw:
                    opt = value.replace('%', '%%')
                else:
                    opt = value
                config_sources.setdefault(section, OrderedDict()).update({key: opt})
                del config_sources[section][key + '_secret']

    def _include_commands(
        self,
        config_sources: ConfigSourcesType,
        display_sensitive: bool,
        display_source: bool,
        raw: bool,
    ):
        for (section, key) in self.sensitive_config_values:
            opt = self._get_cmd_option_from_config_sources(config_sources, section, key)
            if not opt:
                continue
            opt_to_set: Union[str, Tuple[str, str], None] = opt
            if not display_sensitive:
                opt_to_set = '< hidden >'
            if display_source:
                opt_to_set = (str(opt_to_set), 'cmd')
            elif raw:
                opt_to_set = str(opt_to_set).replace('%', '%%')
            if opt_to_set is not None:
                dict_to_update: Dict[str, Union[str, Tuple[str, str]]] = {key: opt_to_set}
                config_sources.setdefault(section, OrderedDict()).update(dict_to_update)
                del config_sources[section][key + '_cmd']

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
                _, section, key = env_var.split('__', 2)
                opt = self._get_env_var_option(section, key)
            except ValueError:
                continue
            if opt is None:
                log.warning("Ignoring unknown env var '%s'", env_var)
                continue
            if not display_sensitive and env_var != self._env_var_name('core', 'unit_test_mode'):
                opt = '< hidden >'
            elif raw:
                opt = opt.replace('%', '%%')
            if display_source:
                opt = (opt, 'env var')

            section = section.lower()
            # if we lower key for kubernetes_environment_variables section,
            # then we won't be able to set any Airflow environment
            # variables. Airflow only parse environment variables starts
            # with AIRFLOW_. Therefore, we need to make it a special case.
            if section != 'kubernetes_environment_variables':
                key = key.lower()
            config_sources.setdefault(section, OrderedDict()).update({key: opt})

    def _filter_by_source(
        self,
        config_sources: ConfigSourcesType,
        display_source: bool,
        getter_func,
    ):
        """
        Deletes default configs from current configuration (an OrderedDict of
        OrderedDicts) if it would conflict with special sensitive_config_values.

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
        :rtype: None
        :return: None, the given config_sources is filtered if necessary,
            otherwise untouched.
        """
        for (section, key) in self.sensitive_config_values:
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
            if not self.airflow_defaults.has_option(section, key):
                continue
            # Check to see if bare setting is the same as defaults
            if display_source:
                # when display_source = true, we know that the config_sources contains tuple
                opt, source = config_sources[section][key]  # type: ignore
            else:
                opt = config_sources[section][key]
            if opt == self.airflow_defaults.get(section, key):
                del config_sources[section][key]

    @staticmethod
    def _replace_config_with_display_sources(
        config_sources: ConfigSourcesType,
        configs: Iterable[Tuple[str, ConfigParser]],
        display_source: bool,
        raw: bool,
        deprecated_options: Dict[Tuple[str, str], Tuple[str, str, str]],
        include_env: bool,
        include_cmds: bool,
        include_secret: bool,
    ):
        for (source_name, config) in configs:
            for section in config.sections():
                AirflowConfigParser._replace_section_config_with_display_sources(
                    config,
                    config_sources,
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
    def _deprecated_value_is_set_in_config(
        deprecated_section: str,
        deprecated_key: str,
        configs: Iterable[Tuple[str, ConfigParser]],
    ) -> bool:
        for config_type, config in configs:
            if config_type == 'default':
                continue
            try:
                deprecated_section_array = config.items(section=deprecated_section, raw=True)
                for (key_candidate, _) in deprecated_section_array:
                    if key_candidate == deprecated_key:
                        return True
            except NoSectionError:
                pass
        return False

    @staticmethod
    def _deprecated_variable_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f'{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}')
            is not None
        )

    @staticmethod
    def _deprecated_command_is_set_in_config(
        deprecated_section: str, deprecated_key: str, configs: Iterable[Tuple[str, ConfigParser]]
    ) -> bool:
        return AirflowConfigParser._deprecated_value_is_set_in_config(
            deprecated_section=deprecated_section, deprecated_key=deprecated_key + "_cmd", configs=configs
        )

    @staticmethod
    def _deprecated_variable_command_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f'{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}_CMD')
            is not None
        )

    @staticmethod
    def _deprecated_secret_is_set_in_config(
        deprecated_section: str, deprecated_key: str, configs: Iterable[Tuple[str, ConfigParser]]
    ) -> bool:
        return AirflowConfigParser._deprecated_value_is_set_in_config(
            deprecated_section=deprecated_section, deprecated_key=deprecated_key + "_secret", configs=configs
        )

    @staticmethod
    def _deprecated_variable_secret_is_set(deprecated_section: str, deprecated_key: str) -> bool:
        return (
            os.environ.get(f'{ENV_VAR_PREFIX}{deprecated_section.upper()}__{deprecated_key.upper()}_SECRET')
            is not None
        )

    @staticmethod
    def _replace_section_config_with_display_sources(
        config: ConfigParser,
        config_sources: ConfigSourcesType,
        display_source: bool,
        raw: bool,
        section: str,
        source_name: str,
        deprecated_options: Dict[Tuple[str, str], Tuple[str, str, str]],
        configs: Iterable[Tuple[str, ConfigParser]],
        include_env: bool,
        include_cmds: bool,
        include_secret: bool,
    ):
        sect = config_sources.setdefault(section, OrderedDict())
        for (k, val) in config.items(section=section, raw=raw):
            deprecated_section, deprecated_key, _ = deprecated_options.get((section, k), (None, None, None))
            if deprecated_section and deprecated_key:
                if source_name == 'default':
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
                sect[k] = (val, source_name)
            else:
                sect[k] = val

    def load_test_config(self):
        """
        Load the unit test configuration.

        Note: this is not reversible.
        """
        # remove all sections, falling back to defaults
        for section in self.sections():
            self.remove_section(section)

        # then read test config

        path = _default_config_file_path('default_test.cfg')
        log.info("Reading default test configuration from %s", path)
        self.read_string(_parameterized_config_from_template('default_test.cfg'))
        # then read any "custom" test settings
        log.info("Reading test configuration from %s", TEST_CONFIG_FILE)
        self.read(TEST_CONFIG_FILE)

    @staticmethod
    def _warn_deprecate(section: str, key: str, deprecated_section: str, deprecated_name: str):
        if section == deprecated_section:
            warnings.warn(
                f'The {deprecated_name} option in [{section}] has been renamed to {key} - '
                f'the old setting has been used, but please update your config.',
                DeprecationWarning,
                stacklevel=3,
            )
        else:
            warnings.warn(
                f'The {deprecated_name} option in [{deprecated_section}] has been moved to the {key} option '
                f'in [{section}] - the old setting has been used, but please update your config.',
                DeprecationWarning,
                stacklevel=3,
            )

    def __getstate__(self):
        return {
            name: getattr(self, name)
            for name in [
                '_sections',
                'is_validated',
                'airflow_defaults',
            ]
        }

    def __setstate__(self, state):
        self.__init__()
        config = state.pop('_sections')
        self.read_dict(config)
        self.__dict__.update(state)


def get_airflow_home() -> str:
    """Get path to Airflow Home"""
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))


def get_airflow_config(airflow_home) -> str:
    """Get Path to airflow.cfg path"""
    airflow_config_var = os.environ.get('AIRFLOW_CONFIG')
    if airflow_config_var is None:
        return os.path.join(airflow_home, 'airflow.cfg')
    return expand_env_var(airflow_config_var)


def _parameterized_config_from_template(filename) -> str:
    TEMPLATE_START = '# ----------------------- TEMPLATE BEGINS HERE -----------------------\n'

    path = _default_config_file_path(filename)
    with open(path) as fh:
        for line in fh:
            if line != TEMPLATE_START:
                continue
            return parameterized_config(fh.read().strip())
    raise RuntimeError(f"Template marker not found in {path!r}")


def parameterized_config(template) -> str:
    """
    Generates a configuration from the provided template + variables defined in
    current scope

    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)


def get_airflow_test_config(airflow_home) -> str:
    """Get path to unittests.cfg"""
    if 'AIRFLOW_TEST_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'unittests.cfg')
    # It will never return None
    return expand_env_var(os.environ['AIRFLOW_TEST_CONFIG'])  # type: ignore[return-value]


def _generate_fernet_key() -> str:
    from cryptography.fernet import Fernet

    return Fernet.generate_key().decode()


def initialize_config() -> AirflowConfigParser:
    """
    Load the Airflow config files.

    Called for you automatically as part of the Airflow boot process.
    """
    global FERNET_KEY, AIRFLOW_HOME

    default_config = _parameterized_config_from_template('default_airflow.cfg')

    local_conf = AirflowConfigParser(default_config=default_config)

    if local_conf.getboolean('core', 'unit_test_mode'):
        # Load test config only
        if not os.path.isfile(TEST_CONFIG_FILE):
            from cryptography.fernet import Fernet

            log.info('Creating new Airflow config file for unit tests in: %s', TEST_CONFIG_FILE)
            pathlib.Path(AIRFLOW_HOME).mkdir(parents=True, exist_ok=True)

            FERNET_KEY = Fernet.generate_key().decode()

            with open(TEST_CONFIG_FILE, 'w') as file:
                cfg = _parameterized_config_from_template('default_test.cfg')
                file.write(cfg)

        local_conf.load_test_config()
    else:
        # Load normal config
        if not os.path.isfile(AIRFLOW_CONFIG):
            from cryptography.fernet import Fernet

            log.info('Creating new Airflow config file in: %s', AIRFLOW_CONFIG)
            pathlib.Path(AIRFLOW_HOME).mkdir(parents=True, exist_ok=True)

            FERNET_KEY = Fernet.generate_key().decode()

            with open(AIRFLOW_CONFIG, 'w') as file:
                file.write(default_config)

        log.info("Reading the config from %s", AIRFLOW_CONFIG)

        local_conf.read(AIRFLOW_CONFIG)

        if local_conf.has_option('core', 'AIRFLOW_HOME'):
            msg = (
                'Specifying both AIRFLOW_HOME environment variable and airflow_home '
                'in the config file is deprecated. Please use only the AIRFLOW_HOME '
                'environment variable and remove the config file entry.'
            )
            if 'AIRFLOW_HOME' in os.environ:
                warnings.warn(msg, category=DeprecationWarning)
            elif local_conf.get('core', 'airflow_home') == AIRFLOW_HOME:
                warnings.warn(
                    'Specifying airflow_home in the config file is deprecated. As you '
                    'have left it at the default value you should remove the setting '
                    'from your airflow.cfg and suffer no change in behaviour.',
                    category=DeprecationWarning,
                )
            else:
                # there
                AIRFLOW_HOME = local_conf.get('core', 'airflow_home')  # type: ignore[assignment]
                warnings.warn(msg, category=DeprecationWarning)

        # They _might_ have set unit_test_mode in the airflow.cfg, we still
        # want to respect that and then load the unittests.cfg
        if local_conf.getboolean('core', 'unit_test_mode'):
            local_conf.load_test_config()

    # Make it no longer a proxy variable, just set it to an actual string
    global WEBSERVER_CONFIG
    WEBSERVER_CONFIG = AIRFLOW_HOME + '/webserver_config.py'

    if not os.path.isfile(WEBSERVER_CONFIG):
        import shutil

        log.info('Creating new FAB webserver config file in: %s', WEBSERVER_CONFIG)
        shutil.copy(_default_config_file_path('default_webserver_config.py'), WEBSERVER_CONFIG)
    return local_conf


# Historical convenience functions to access config entries
def load_test_config():
    """Historical load_test_config"""
    warnings.warn(
        "Accessing configuration method 'load_test_config' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.load_test_config'",
        DeprecationWarning,
        stacklevel=2,
    )
    conf.load_test_config()


def get(*args, **kwargs) -> Optional[ConfigType]:
    """Historical get"""
    warnings.warn(
        "Accessing configuration method 'get' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.get'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.get(*args, **kwargs)


def getboolean(*args, **kwargs) -> bool:
    """Historical getboolean"""
    warnings.warn(
        "Accessing configuration method 'getboolean' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getboolean'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getboolean(*args, **kwargs)


def getfloat(*args, **kwargs) -> float:
    """Historical getfloat"""
    warnings.warn(
        "Accessing configuration method 'getfloat' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getfloat'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getfloat(*args, **kwargs)


def getint(*args, **kwargs) -> int:
    """Historical getint"""
    warnings.warn(
        "Accessing configuration method 'getint' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getint'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getint(*args, **kwargs)


def getsection(*args, **kwargs) -> Optional[ConfigOptionsDictType]:
    """Historical getsection"""
    warnings.warn(
        "Accessing configuration method 'getsection' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.getsection'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.getsection(*args, **kwargs)


def has_option(*args, **kwargs) -> bool:
    """Historical has_option"""
    warnings.warn(
        "Accessing configuration method 'has_option' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.has_option'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.has_option(*args, **kwargs)


def remove_option(*args, **kwargs) -> bool:
    """Historical remove_option"""
    warnings.warn(
        "Accessing configuration method 'remove_option' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.remove_option'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.remove_option(*args, **kwargs)


def as_dict(*args, **kwargs) -> ConfigSourcesType:
    """Historical as_dict"""
    warnings.warn(
        "Accessing configuration method 'as_dict' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.as_dict'",
        DeprecationWarning,
        stacklevel=2,
    )
    return conf.as_dict(*args, **kwargs)


def set(*args, **kwargs) -> None:
    """Historical set"""
    warnings.warn(
        "Accessing configuration method 'set' directly from the configuration module is "
        "deprecated. Please access the configuration from the 'configuration.conf' object via "
        "'conf.set'",
        DeprecationWarning,
        stacklevel=2,
    )
    conf.set(*args, **kwargs)


def ensure_secrets_loaded() -> List[BaseSecretsBackend]:
    """
    Ensure that all secrets backends are loaded.
    If the secrets_backend_list contains only 2 default backends, reload it.
    """
    # Check if the secrets_backend_list contains only 2 default backends
    if len(secrets_backend_list) == 2:
        return initialize_secrets_backends()
    return secrets_backend_list


def get_custom_secret_backend() -> Optional[BaseSecretsBackend]:
    """Get Secret Backend if defined in airflow.cfg"""
    secrets_backend_cls = conf.getimport(section='secrets', key='backend')

    if secrets_backend_cls:
        try:
            backends: Any = conf.get(section='secrets', key='backend_kwargs', fallback='{}')
            alternative_secrets_config_dict = json.loads(backends)
        except JSONDecodeError:
            alternative_secrets_config_dict = {}

        return secrets_backend_cls(**alternative_secrets_config_dict)
    return None


def initialize_secrets_backends() -> List[BaseSecretsBackend]:
    """
    * import secrets backend classes
    * instantiate them and return them in a list
    """
    backend_list = []

    custom_secret_backend = get_custom_secret_backend()

    if custom_secret_backend is not None:
        backend_list.append(custom_secret_backend)

    for class_name in DEFAULT_SECRETS_SEARCH_PATH:
        secrets_backend_cls = import_string(class_name)
        backend_list.append(secrets_backend_cls())

    return backend_list


@functools.lru_cache(maxsize=None)
def _DEFAULT_CONFIG() -> str:
    path = _default_config_file_path('default_airflow.cfg')
    with open(path) as fh:
        return fh.read()


@functools.lru_cache(maxsize=None)
def _TEST_CONFIG() -> str:
    path = _default_config_file_path('default_test.cfg')
    with open(path) as fh:
        return fh.read()


_deprecated = {
    'DEFAULT_CONFIG': _DEFAULT_CONFIG,
    'TEST_CONFIG': _TEST_CONFIG,
    'TEST_CONFIG_FILE_PATH': functools.partial(_default_config_file_path, 'default_test.cfg'),
    'DEFAULT_CONFIG_FILE_PATH': functools.partial(_default_config_file_path, 'default_airflow.cfg'),
}


def __getattr__(name):
    if name in _deprecated:
        warnings.warn(
            f"{__name__}.{name} is deprecated and will be removed in future",
            DeprecationWarning,
            stacklevel=2,
        )
        return _deprecated[name]()
    raise AttributeError(f"module {__name__} has no attribute {name}")


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "$AIRFLOW_HOME/airflow.cfg" respectively as defaults.
AIRFLOW_HOME = get_airflow_home()
AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)


# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'tests', 'dags'
)
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')

# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'tests', 'plugins'
)
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, 'plugins')


TEST_CONFIG_FILE = get_airflow_test_config(AIRFLOW_HOME)

SECRET_KEY = b64encode(os.urandom(16)).decode('utf-8')
FERNET_KEY = ''  # Set only if needed when generating a new file
WEBSERVER_CONFIG = ''  # Set by initialize_config

conf = initialize_config()
secrets_backend_list = initialize_secrets_backends()
conf.validate()
