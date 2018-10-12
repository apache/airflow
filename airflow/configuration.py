# -*- coding: utf-8 -*-
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
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from base64 import b64encode
from builtins import str
from collections import OrderedDict
import copy
import errno
from future import standard_library
import os
import shlex
import six
from six import iteritems
import subprocess
import sys
import warnings

from backports.configparser import ConfigParser
from zope.deprecation import deprecated

from airflow.exceptions import AirflowConfigException
from airflow.utils.log.logging_mixin import LoggingMixin

standard_library.install_aliases()

log = LoggingMixin().log

# show Airflow's deprecation warnings
warnings.filterwarnings(
    action='default', category=DeprecationWarning, module='airflow')
warnings.filterwarnings(
    action='default', category=PendingDeprecationWarning, module='airflow')


def generate_fernet_key():
    try:
        from cryptography.fernet import Fernet
    except ImportError:
        pass
    try:
        key = Fernet.generate_key().decode()
    except NameError:
        key = "cryptography_not_found_storing_passwords_in_plain_text"
    return key


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


def _read_default_config_file(file_name):
    templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
    file_path = os.path.join(templates_dir, file_name)
    if six.PY2:
        with open(file_path) as f:
            config = f.read()
            return config.decode('utf-8')
    else:
        with open(file_path, encoding='utf-8') as f:
            return f.read()


DEFAULT_CONFIG = _read_default_config_file('default_airflow.cfg')
TEST_CONFIG = _read_default_config_file('default_test.cfg')


class AirflowConfigParser(ConfigParser):

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    as_command_stdout = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'result_backend'),
        # Todo: remove this in Airflow 1.11
        ('celery', 'celery_result_backend'),
        ('atlas', 'password'),
        ('smtp', 'smtp_password'),
        ('ldap', 'bind_password'),
        ('kubernetes', 'git_password'),
    }

    # A two-level mapping of (section -> new_name -> old_name). When reading
    # new_name, the old_name will be checked to see if it exists. If it does a
    # DeprecationWarning will be issued and the old name will be used instead
    deprecated_options = {
        'celery': {
            # Remove these keys in Airflow 1.11
            'worker_concurrency': 'celeryd_concurrency',
            'result_backend': 'celery_result_backend',
            'broker_url': 'celery_broker_url',
            'ssl_active': 'celery_ssl_active',
            'ssl_cert': 'celery_ssl_cert',
            'ssl_key': 'celery_ssl_key',
        }
    }
    deprecation_format_string = (
        'The {old} option in [{section}] has been renamed to {new} - the old '
        'setting has been used, but please update your config.'
    )

    def __init__(self, default_config=None, *args, **kwargs):
        super(AirflowConfigParser, self).__init__(*args, **kwargs)

        self.defaults = ConfigParser(*args, **kwargs)
        if default_config is not None:
            self.defaults.read_string(default_config)

        self.is_validated = False

    def _validate(self):
        if (
                self.get("core", "executor") != 'SequentialExecutor' and
                "sqlite" in self.get('core', 'sql_alchemy_conn')):
            raise AirflowConfigException(
                "error: cannot use sqlite with the {}".format(
                    self.get('core', 'executor')))

        elif (
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode") not in ['user', 'ldapgroup']
        ):
            raise AirflowConfigException(
                "error: owner_mode option should be either "
                "'user' or 'ldapgroup' when filtering by owner is set")

        elif (
            self.getboolean("webserver", "authenticate") and
            self.get("webserver", "owner_mode").lower() == 'ldapgroup' and
            self.get("webserver", "auth_backend") != (
                'airflow.contrib.auth.backends.ldap_auth')
        ):
            raise AirflowConfigException(
                "error: attempt at using ldapgroup "
                "filtering without using the Ldap backend")

        self.is_validated = True

    def _get_env_var_option(self, section, key):
        # must have format AIRFLOW__{SECTION}__{KEY} (note double underscore)
        env_var = 'AIRFLOW__{S}__{K}'.format(S=section.upper(), K=key.upper())
        if env_var in os.environ:
            return expand_env_var(os.environ[env_var])

    def _get_cmd_option(self, section, key):
        fallback_key = key + '_cmd'
        # if this is a valid command key...
        if (section, key) in self.as_command_stdout:
            if super(AirflowConfigParser, self) \
                    .has_option(section, fallback_key):
                command = super(AirflowConfigParser, self) \
                    .get(section, fallback_key)
                return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        deprecated_name = self.deprecated_options.get(section, {}).get(key, None)

        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option
        if deprecated_name:
            option = self._get_env_var_option(section, deprecated_name)
            if option is not None:
                self._warn_deprecate(section, key, deprecated_name)
                return option

        # ...then the config file
        if super(AirflowConfigParser, self).has_option(section, key):
            # Use the parent's methods to get the actual config here to be able to
            # separate the config from default config.
            return expand_env_var(
                super(AirflowConfigParser, self).get(section, key, **kwargs))
        if deprecated_name:
            if super(AirflowConfigParser, self).has_option(section, deprecated_name):
                self._warn_deprecate(section, key, deprecated_name)
                return expand_env_var(super(AirflowConfigParser, self).get(
                    section,
                    deprecated_name,
                    **kwargs
                ))

        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option
        if deprecated_name:
            option = self._get_cmd_option(section, deprecated_name)
            if option:
                self._warn_deprecate(section, key, deprecated_name)
                return option

        # ...then the default config
        if self.defaults.has_option(section, key):
            return expand_env_var(
                self.defaults.get(section, key, **kwargs))

        else:
            log.warning(
                "section/key [{section}/{key}] not found in config".format(**locals())
            )

            raise AirflowConfigException(
                "section/key [{section}/{key}] not found "
                "in config".format(**locals()))

    def getboolean(self, section, key):
        val = str(self.get(section, key)).lower().strip()
        if '#' in val:
            val = val.split('#')[0].strip()
        if val.lower() in ('t', 'true', '1'):
            return True
        elif val.lower() in ('f', 'false', '0'):
            return False
        else:
            raise AirflowConfigException(
                'The value for configuration option "{}:{}" is not a '
                'boolean (received "{}").'.format(section, key, val))

    def getint(self, section, key):
        return int(self.get(section, key))

    def getfloat(self, section, key):
        return float(self.get(section, key))

    def read(self, filenames):
        super(AirflowConfigParser, self).read(filenames)
        self._validate()

    def read_dict(self, *args, **kwargs):
        super(AirflowConfigParser, self).read_dict(*args, **kwargs)
        self._validate()

    def has_option(self, section, option):
        try:
            # Using self.get() to avoid reimplementing the priority order
            # of config variables (env, config, cmd, defaults)
            self.get(section, option)
            return True
        except AirflowConfigException:
            return False

    def remove_option(self, section, option, remove_default=True):
        """
        Remove an option if it exists in config from a file or
        default config. If both of config have the same option, this removes
        the option in both configs unless remove_default=False.
        """
        if super(AirflowConfigParser, self).has_option(section, option):
            super(AirflowConfigParser, self).remove_option(section, option)

        if self.defaults.has_option(section, option) and remove_default:
            self.defaults.remove_option(section, option)

    def getsection(self, section):
        """
        Returns the section as a dict. Values are converted to int, float, bool
        as required.
        :param section: section from the config
        :return: dict
        """
        if section not in self._sections and section not in self.defaults._sections:
            return None

        _section = copy.deepcopy(self.defaults._sections[section])

        if section in self._sections:
            _section.update(copy.deepcopy(self._sections[section]))

        for key, val in iteritems(_section):
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

    def as_dict(self, display_source=False, display_sensitive=False):
        """
        Returns the current configuration as an OrderedDict of OrderedDicts.
        :param display_source: If False, the option value is returned. If True,
            a tuple of (option_value, source) is returned. Source is either
            'airflow.cfg' or 'default'.
        :type display_source: bool
        :param display_sensitive: If True, the values of options set by env
            vars and bash commands will be displayed. If False, those options
            are shown as '< hidden >'
        :type display_sensitive: bool
        """
        cfg = copy.deepcopy(self.defaults._sections)
        cfg.update(copy.deepcopy(self._sections))

        # remove __name__ (affects Python 2 only)
        for options in cfg.values():
            options.pop('__name__', None)

        # add source
        if display_source:
            for section in cfg:
                for k, v in cfg[section].items():
                    cfg[section][k] = (v, 'airflow config')

        # add env vars and overwrite because they have priority
        for ev in [ev for ev in os.environ if ev.startswith('AIRFLOW__')]:
            try:
                _, section, key = ev.split('__')
                opt = self._get_env_var_option(section, key)
            except ValueError:
                opt = None
            if opt:
                if (
                    not display_sensitive and
                        ev != 'AIRFLOW__CORE__UNIT_TEST_MODE'):
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'env var')
                cfg.setdefault(section.lower(), OrderedDict()).update(
                    {key.lower(): opt})

        # add bash commands
        for (section, key) in self.as_command_stdout:
            opt = self._get_cmd_option(section, key)
            if opt:
                if not display_sensitive:
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'bash cmd')
                cfg.setdefault(section, OrderedDict()).update({key: opt})

        return cfg

    def load_test_config(self):
        """
        Load the unit test configuration.

        Note: this is not reversible.
        """
        # override any custom settings with defaults
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        # then read test config
        self.read_string(parameterized_config(TEST_CONFIG))
        # then read any "custom" test settings
        self.read(TEST_CONFIG_FILE)

    def _warn_deprecate(self, section, key, deprecated_name):
        warnings.warn(
            self.deprecation_format_string.format(
                old=deprecated_name,
                new=key,
                section=section,
            ),
            DeprecationWarning,
            stacklevel=3,
        )


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise AirflowConfigException(
                'Error creating {}: {}'.format(path, exc.strerror))


# Setting AIRFLOW_HOME and AIRFLOW_CONFIG from environment variables, using
# "~/airflow" and "~/airflow/airflow.cfg" respectively as defaults.

if 'AIRFLOW_HOME' not in os.environ:
    AIRFLOW_HOME = expand_env_var('~/airflow')
else:
    AIRFLOW_HOME = expand_env_var(os.environ['AIRFLOW_HOME'])

mkdir_p(AIRFLOW_HOME)

if 'AIRFLOW_CONFIG' not in os.environ:
    if os.path.isfile(expand_env_var('~/airflow.cfg')):
        AIRFLOW_CONFIG = expand_env_var('~/airflow.cfg')
    else:
        AIRFLOW_CONFIG = AIRFLOW_HOME + '/airflow.cfg'
else:
    AIRFLOW_CONFIG = expand_env_var(os.environ['AIRFLOW_CONFIG'])

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


TEST_CONFIG_FILE = AIRFLOW_HOME + '/unittests.cfg'

# only generate a Fernet key if we need to create a new config file
if not os.path.isfile(TEST_CONFIG_FILE) or not os.path.isfile(AIRFLOW_CONFIG):
    FERNET_KEY = generate_fernet_key()
else:
    FERNET_KEY = ''

SECRET_KEY = b64encode(os.urandom(16)).decode('utf-8')

TEMPLATE_START = (
    '# ----------------------- TEMPLATE BEGINS HERE -----------------------')
if not os.path.isfile(TEST_CONFIG_FILE):
    log.info(
        'Creating new Airflow config file for unit tests in: %s', TEST_CONFIG_FILE
    )
    with open(TEST_CONFIG_FILE, 'w') as f:
        cfg = parameterized_config(TEST_CONFIG)
        f.write(cfg.split(TEMPLATE_START)[-1].strip())
if not os.path.isfile(AIRFLOW_CONFIG):
    log.info(
        'Creating new Airflow config file in: %s',
        AIRFLOW_CONFIG
    )
    with open(AIRFLOW_CONFIG, 'w') as f:
        cfg = parameterized_config(DEFAULT_CONFIG)
        cfg = cfg.split(TEMPLATE_START)[-1].strip()
        if six.PY2:
            cfg = cfg.encode('utf8')
        f.write(cfg)

log.info("Reading the config from %s", AIRFLOW_CONFIG)

conf = AirflowConfigParser(default_config=parameterized_config(DEFAULT_CONFIG))

conf.read(AIRFLOW_CONFIG)


if conf.getboolean('webserver', 'rbac'):
    DEFAULT_WEBSERVER_CONFIG = _read_default_config_file('default_webserver_config.py')

    WEBSERVER_CONFIG = AIRFLOW_HOME + '/webserver_config.py'

    if not os.path.isfile(WEBSERVER_CONFIG):
        log.info('Creating new FAB webserver config file in: %s', WEBSERVER_CONFIG)
        with open(WEBSERVER_CONFIG, 'w') as f:
            f.write(DEFAULT_WEBSERVER_CONFIG)

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
set = conf.set # noqa

for func in [load_test_config, get, getboolean, getfloat, getint, has_option,
             remove_option, as_dict, set]:
    deprecated(
        func,
        "Accessing configuration method '{f.__name__}' directly from "
        "the configuration module is deprecated. Please access the "
        "configuration from the 'configuration.conf' object via "
        "'conf.{f.__name__}'".format(f=func))
