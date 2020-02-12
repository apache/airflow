
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

import copy
import logging
import os
import pathlib
import shlex
import subprocess
import sys
import warnings
from base64 import b64encode
from collections import OrderedDict
# Ignored Mypy on configparser because it thinks the configparser module has no _UNSET attribute
from configparser import _UNSET, ConfigParser, NoOptionError, NoSectionError  # type: ignore
from typing import Dict, Tuple

import yaml
from cryptography.fernet import Fernet
from zope.deprecation import deprecated

from airflow.exceptions import AirflowConfigException

log = logging.getLogger(__name__)

# show Airflow's deprecation warnings
warnings.filterwarnings(
    action='default', category=DeprecationWarning, module='airflow')
warnings.filterwarnings(
    action='default', category=PendingDeprecationWarning, module='airflow')


def expand_env_var(env_var):
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


def run_command(command):
    """
    Runs command and returns stdout
    """
    process = subprocess.Popen(
        shlex.split(command),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True)
    output, stderr = [stream.decode(sys.getdefaultencoding(), 'ignore')
                      for stream in process.communicate()]

    if process.returncode != 0:
        raise AirflowConfigException(
            "Cannot execute {}. Error code is: {}. Output: {}, Stderr: {}"
            .format(command, process.returncode, output, stderr)
        )

    return output


def _read_default_config_file(file_name: str) -> Tuple[str, str]:
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    file_path = os.path.join(templates_dir, file_name)
    with open(file_path, encoding='utf-8') as config_file:
        return config_file.read(), file_path


DEFAULT_CONFIG, DEFAULT_CONFIG_FILE_PATH = _read_default_config_file('default_airflow.cfg')
TEST_CONFIG, TEST_CONFIG_FILE_PATH = _read_default_config_file('default_test.cfg')


def default_config_yaml() -> dict:
    """
    Read Airflow configs from YAML file

    :return: Python dictionary containing configs & their info
    """
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    file_path = os.path.join(templates_dir, "config.yml")

    with open(file_path) as config_file:
        return yaml.safe_load(config_file)


class AirflowConfigParser(ConfigParser):

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    as_command_stdout = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'flower_basic_auth'),
        ('celery', 'result_backend'),
        ('atlas', 'password'),
        ('smtp', 'smtp_password'),
        ('ldap', 'bind_password'),
        ('kubernetes', 'git_password'),
    }

    # A mapping of (new option -> old option). where option is a tuple of section name and key.
    # When reading new option, the old option will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old option will be used instead
    deprecated_options = {
        ('elasticsearch', 'host'): ('elasticsearch', 'elasticsearch_host'),
        ('elasticsearch', 'log_id_template'): ('elasticsearch', 'elasticsearch_log_id_template'),
        ('elasticsearch', 'end_of_log_mark'): ('elasticsearch', 'elasticsearch_end_of_log_mark'),
        ('elasticsearch', 'frontend'): ('elasticsearch', 'elasticsearch_frontend'),
        ('elasticsearch', 'write_stdout'): ('elasticsearch', 'elasticsearch_write_stdout'),
        ('elasticsearch', 'json_format'): ('elasticsearch', 'elasticsearch_json_format'),
        ('elasticsearch', 'json_fields'): ('elasticsearch', 'elasticsearch_json_fields'),
        ('logging', 'base_log_folder'): ('core', 'base_log_folder'),
        ('logging', 'remote_logging'): ('core', 'remote_logging'),
        ('logging', 'remote_log_conn_id'): ('core', 'remote_log_conn_id'),
        ('logging', 'remote_base_log_folder'): ('core', 'remote_base_log_folder'),
        ('logging', 'encrypt_s3_logs'): ('core', 'encrypt_s3_logs'),
        ('logging', 'logging_level'): ('core', 'logging_level'),
        ('logging', 'fab_logging_level'): ('core', 'fab_logging_level'),
        ('logging', 'logging_config_class'): ('core', 'logging_config_class'),
        ('logging', 'colored_console_log'): ('core', 'colored_console_log'),
        ('logging', 'colored_log_format'): ('core', 'colored_log_format'),
        ('logging', 'colored_formatter_class'): ('core', 'colored_formatter_class'),
        ('logging', 'log_format'): ('core', 'log_format'),
        ('logging', 'simple_log_format'): ('core', 'simple_log_format'),
        ('logging', 'task_log_prefix_template'): ('core', 'task_log_prefix_template'),
        ('logging', 'log_filename_template'): ('core', 'log_filename_template'),
        ('logging', 'log_processor_filename_template'): ('core', 'log_processor_filename_template'),
        ('logging', 'dag_processor_manager_log_location'): ('core', 'dag_processor_manager_log_location'),
        ('logging', 'task_log_reader'): ('core', 'task_log_reader'),
    }

    # A mapping of old default values that we want to change and warn the user
    # about. Mapping of section -> setting -> { old, replace, by_version }
    deprecated_values = {
        'core': {
            'task_runner': ('BashTaskRunner', 'StandardTaskRunner', '2.0'),
        },
    }

    # This method transforms option names on every read, get, or set operation.
    # This changes from the default behaviour of ConfigParser from lowercasing
    # to instead be case-preserving
    def optionxform(self, optionstr: str) -> str:
        return optionstr

    def __init__(self, default_config=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.airflow_defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.airflow_defaults.read_string(default_config)

        self.is_validated = False

    def _validate(self):
        if (
                self.get("core", "executor") not in ('DebugExecutor', 'SequentialExecutor') and
                "sqlite" in self.get('core', 'sql_alchemy_conn')):
            raise AirflowConfigException(
                "error: cannot use sqlite with the {}".format(
                    self.get('core', 'executor')))

        for section, replacement in self.deprecated_values.items():
            for name, info in replacement.items():
                old, new, version = info
                if self.get(section, name, fallback=None) == old:
                    # Make sure the env var option is removed, otherwise it
                    # would be read and used instead of the value we set
                    env_var = self._env_var_name(section, name)
                    os.environ.pop(env_var, None)

                    self.set(section, name, new)
                    warnings.warn(
                        'The {name} setting in [{section}] has the old default value '
                        'of {old!r}. This value has been changed to {new!r} in the '
                        'running config, but please update your config before Apache '
                        'Airflow {version}.'.format(
                            name=name, section=section, old=old, new=new, version=version
                        ),
                        FutureWarning
                    )

        self.is_validated = True

    @staticmethod
    def _env_var_name(section, key):
        return 'AIRFLOW__{S}__{K}'.format(S=section.upper(), K=key.upper())

    def _get_env_var_option(self, section, key):
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = self._env_var_name(section, key)
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])
        # alternatively AIRFLOW__{SECTION}__{KEY}_CMD (for a command)
        env_var_cmd = env_var + '_CMD'
        if env_var_cmd in os.environ:
            # if this is a valid command key...
            if (section, key) in self.as_command_stdout:
                return run_command(os.environ[env_var_cmd])

    def _get_cmd_option(self, section, key):
        fallback_key = key + '_cmd'
        # if this is a valid command key...
        if (section, key) in self.as_command_stdout:
            if super().has_option(section, fallback_key):
                command = super().get(section, fallback_key)
                return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        deprecated_section, deprecated_key = self.deprecated_options.get((section, key), (None, None))

        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        if deprecated_section:
            option = self._get_env_var_option(deprecated_section, deprecated_key)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option

        # ...then the config file
        if super().has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(
                super().get(section, key, **kwargs))
        if deprecated_section:
            if super().has_option(deprecated_section, deprecated_key):
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return expand_env_var(super().get(
                    deprecated_section,
                    deprecated_key,
                    **kwargs
                ))

        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_section:
            option = self._get_cmd_option(deprecated_section, deprecated_key)
            if option:
                self._warn_deprecate(section, key, deprecated_section, deprecated_key)
                return option

        # ...then the default config
        if self.airflow_defaults.has_option(section, key) or 'fallback' in kwargs:
            return expand_env_var(
                self.airflow_defaults.get(section, key, **kwargs))

        else:
            log.warning(
                "section/key [%s/%s] not found in config", section, key
            )

            raise AirflowConfigException(
                "section/key [{section}/{key}] not found "
                "in config".format(section=section, key=key))

    def getboolean(self, section, key, **kwargs):
        val = str(self.get(section, key, **kwargs)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val in ('t', 'true', '1'):
            return True
        elif val in ('f', 'false', '0'):
            return False
        else:
            raise ValueError(
                'The value for configuration option "{}:{}" is not a '
                'boolean (received "{}").'.format(section, key, val))

    def getint(self, section, key, **kwargs):
        return int(self.get(section, key, **kwargs))

    def getfloat(self, section, key, **kwargs):
        return float(self.get(section, key, **kwargs))

    def read(self, filenames, **kwargs):
        super().read(filenames, **kwargs)
        self._validate()

    def read_dict(self, *args, **kwargs):
        super().read_dict(*args, **kwargs)
        self._validate()

    def has_option(self, section, option):
        try:
            # Using self.get() to avoid reimplementing the priority order
            # of config variables (env, config, cmd, defaults)
            # UNSET to avoid logging a warning about missing values
            self.get(section, option, fallback=_UNSET)
            return True
        except (NoOptionError, NoSectionError):
            return False

    def remove_option(self, section, option, remove_default=True):
        """
        Remove an option if it exists in config from a file or
        default config. If both of config have the same option, this removes
        the option in both configs unless remove_default=False.
        """
        if super().has_option(section, option):
            super().remove_option(section, option)

        if self.airflow_defaults.has_option(section, option) and remove_default:
            self.airflow_defaults.remove_option(section, option)

    def getsection(self, section):
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.

        :param section: section from the config
        :rtype: dict
        """
        if (section not in self._sections and
                section not in self.airflow_defaults._sections):
            return None

        _section = copy.deepcopy(self.airflow_defaults._sections[section])

        if section in self._sections:
            _section.update(copy.deepcopy(self._sections[section]))

        section_prefix = 'AIRFLOW__{S}__'.format(S=section.upper())
        for env_var in sorted(os.environ.keys()):
            if env_var.startswith(section_prefix):
                key = env_var.replace(section_prefix, '').lower()
                _section[key] = self._get_env_var_option(section, key)

        for key, val in _section.items():
            try:
                val = int(val)
            except ValueError:
                try:
                    val = float(val)
                except ValueError:
                    if val.lower() in ('t', 'true'):
                        val = True
                    elif val.lower() in ('f', 'false'):
                        val = False
            _section[key] = val
        return _section

    def as_dict(
            self, display_source=False, display_sensitive=False, raw=False,
            include_env=True, include_cmds=True) -> Dict[str, Dict[str, str]]:
        """
        Returns the current configuration as an OrderedDict of OrderedDicts.

        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg', 'default', 'env var', or 'cmd'.
        :type display_source: bool
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :type display_sensitive: bool
        :param raw: Should the values be output as interpolated values, or the
            "raw" form that can be fed back in to ConfigParser
        :type raw: bool
        :param include_env: Should the value of configuration from AIRFLOW__
            environment variables be included or not
        :type include_env: bool
        :param include_cmds: Should the result of calling any *_cmd config be
            set (True, default), or should the _cmd options be left as the
            command to run (False)
        :type include_cmds: bool
        :rtype: Dict[str, Dict[str, str]]
        :return: Dictionary, where the key is the name of the section and the content is
            the dictionary with the name of the parameter and its value.
        """
        cfg: Dict[str, Dict[str, str]] = {}
        configs = [
            ('default', self.airflow_defaults),
            ('airflow.cfg', self),
        ]

        for (source_name, config) in configs:
            for section in config.sections():
                sect = cfg.setdefault(section, OrderedDict())
                for (k, val) in config.items(section=section, raw=raw):
                    if display_source:
                        val = (val, source_name)
                    sect[k] = val

        # add env vars and overwrite because they have priority
        if include_env:
            for ev in [ev for ev in os.environ if ev.startswith('AIRFLOW__')]:
                try:
                    _, section, key = ev.split('__', 2)
                    opt = self._get_env_var_option(section, key)
                except ValueError:
                    continue
                if not display_sensitive and ev != 'AIRFLOW__CORE__UNIT_TEST_MODE':
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
                cfg.setdefault(section, OrderedDict()).update({key: opt})

        # add bash commands
        if include_cmds:
            for (section, key) in self.as_command_stdout:
                opt = self._get_cmd_option(section, key)
                if opt:
                    if not display_sensitive:
                        opt = '< hidden >'
                    if display_source:
                        opt = (opt, 'cmd')
                    elif raw:
                        opt = opt.replace('%', '%%')
                    cfg.setdefault(section, OrderedDict()).update({key: opt})
                    del cfg[section][key + '_cmd']

        return cfg

    def load_test_config(self):
        """
        Load the unit test configuration.

        Note: this is not reversible.
        """
        # override any custom settings with defaults
        log.info("Overriding settings with defaults from %s", DEFAULT_CONFIG_FILE_PATH)
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        # then read test config
        log.info("Reading default test configuration from %s", TEST_CONFIG_FILE_PATH)
        self.read_string(parameterized_config(TEST_CONFIG))
        # then read any "custom" test settings
        log.info("Reading test configuration from %s", TEST_CONFIG_FILE)
        self.read(TEST_CONFIG_FILE)

    def _warn_deprecate(self, section, key, deprecated_section, deprecated_name):
        if section == deprecated_section:
            warnings.warn(
                'The {old} option in [{section}] has been renamed to {new} - the old '
                'setting has been used, but please update your config.'.format(
                    old=deprecated_name,
                    new=key,
                    section=section,
                ),
                DeprecationWarning,
                stacklevel=3,
            )
        else:
            warnings.warn(
                'The {old_key} option in [{old_section}] has been moved to the {new_key} option in '
                '[{new_section}] - the old setting has been used, but please update your config.'.format(
                    old_section=deprecated_section,
                    old_key=deprecated_name,
                    new_key=key,
                    new_section=section,
                ),
                DeprecationWarning,
                stacklevel=3,
            )


def get_airflow_home():
    return expand_env_var(os.environ.get('AIRFLOW_HOME', '~/airflow'))


def get_airflow_config(airflow_home):
    if 'AIRFLOW_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'airflow.cfg')
    return expand_env_var(os.environ['AIRFLOW_CONFIG'])


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "$AIRFLOW_HOME/airflow.cfg" respectively as defaults.

AIRFLOW_HOME = get_airflow_home()
AIRFLOW_CONFIG = get_airflow_config(AIRFLOW_HOME)
pathlib.Path(AIRFLOW_HOME).mkdir(parents=True, exist_ok=True)


# Set up dags folder for unit tests
# this directory won't exist if users install via pip
_TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'tests',
    'dags')
if os.path.exists(_TEST_DAGS_FOLDER):
    TEST_DAGS_FOLDER = _TEST_DAGS_FOLDER
else:
    TEST_DAGS_FOLDER = os.path.join(AIRFLOW_HOME, 'dags')

# Set up plugins folder for unit tests
_TEST_PLUGINS_FOLDER = os.path.join(
    os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
    'tests',
    'plugins')
if os.path.exists(_TEST_PLUGINS_FOLDER):
    TEST_PLUGINS_FOLDER = _TEST_PLUGINS_FOLDER
else:
    TEST_PLUGINS_FOLDER = os.path.join(AIRFLOW_HOME, 'plugins')


def parameterized_config(template):
    """
    Generates a configuration from the provided template + variables defined in
    current scope

    :param template: a config content templated with {{variables}}
    """
    all_vars = {k: v for d in [globals(), locals()] for k, v in d.items()}
    return template.format(**all_vars)


def get_airflow_test_config(airflow_home):
    if 'AIRFLOW_TEST_CONFIG' not in os.environ:
        return os.path.join(airflow_home, 'unittests.cfg')
    return expand_env_var(os.environ['AIRFLOW_TEST_CONFIG'])


TEST_CONFIG_FILE = get_airflow_test_config(AIRFLOW_HOME)

# only generate a Fernet key if we need to create a new config file
if not os.path.isfile(TEST_CONFIG_FILE) or not os.path.isfile(AIRFLOW_CONFIG):
    FERNET_KEY = Fernet.generate_key().decode()
else:
    FERNET_KEY = ''

SECRET_KEY = b64encode(os.urandom(16)).decode('utf-8')

TEMPLATE_START = (
    '# ----------------------- TEMPLATE BEGINS HERE -----------------------')
if not os.path.isfile(TEST_CONFIG_FILE):
    log.info(
        'Creating new Airflow config file for unit tests in: %s', TEST_CONFIG_FILE
    )
    with open(TEST_CONFIG_FILE, 'w') as file:
        cfg = parameterized_config(TEST_CONFIG)
        file.write(cfg.split(TEMPLATE_START)[-1].strip())
if not os.path.isfile(AIRFLOW_CONFIG):
    log.info(
        'Creating new Airflow config file in: %s',
        AIRFLOW_CONFIG
    )
    with open(AIRFLOW_CONFIG, 'w') as file:
        cfg = parameterized_config(DEFAULT_CONFIG)
        cfg = cfg.split(TEMPLATE_START)[-1].strip()
        file.write(cfg)

log.info("Reading the config from %s", AIRFLOW_CONFIG)

conf = AirflowConfigParser(default_config=parameterized_config(DEFAULT_CONFIG))

conf.read(AIRFLOW_CONFIG)

if conf.has_option('core', 'AIRFLOW_HOME'):
    msg = (
        'Specifying both AIRFLOW_HOME environment variable and airflow_home '
        'in the config file is deprecated. Please use only the AIRFLOW_HOME '
        'environment variable and remove the config file entry.'
    )
    if 'AIRFLOW_HOME' in os.environ:
        warnings.warn(msg, category=DeprecationWarning)
    elif conf.get('core', 'airflow_home') == AIRFLOW_HOME:
        warnings.warn(
            'Specifying airflow_home in the config file is deprecated. As you '
            'have left it at the default value you should remove the setting '
            'from your airflow.cfg and suffer no change in behaviour.',
            category=DeprecationWarning,
        )
    else:
        AIRFLOW_HOME = conf.get('core', 'airflow_home')
        warnings.warn(msg, category=DeprecationWarning)


WEBSERVER_CONFIG = AIRFLOW_HOME + '/webserver_config.py'

if not os.path.isfile(WEBSERVER_CONFIG):
    log.info('Creating new FAB webserver config file in: %s', WEBSERVER_CONFIG)
    DEFAULT_WEBSERVER_CONFIG, _ = _read_default_config_file('default_webserver_config.py')
    with open(WEBSERVER_CONFIG, 'w') as file:
        file.write(DEFAULT_WEBSERVER_CONFIG)

if conf.getboolean('core', 'unit_test_mode'):
    conf.load_test_config()

# Historical convenience functions to access config entries

load_test_config = conf.load_test_config
get = conf.get
getboolean = conf.getboolean
getfloat = conf.getfloat
getint = conf.getint
getsection = conf.getsection
has_option = conf.has_option
remove_option = conf.remove_option
as_dict = conf.as_dict
set = conf.set  # noqa

for func in [load_test_config, get, getboolean, getfloat, getint, has_option,
             remove_option, as_dict, set]:
    deprecated(
        func.__name__,
        "Accessing configuration method '{f.__name__}' directly from "
        "the configuration module is deprecated. Please access the "
        "configuration from the 'configuration.conf' object via "
        "'conf.{f.__name__}'".format(f=func))
