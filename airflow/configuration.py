# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import copy
import errno
import logging
import os
import six
import subprocess
import warnings
import shlex
import sys

from future import standard_library
standard_library.install_aliases()

from builtins import str
from collections import OrderedDict
from six.moves import configparser

from airflow.exceptions import AirflowConfigException

# show Airflow's deprecation warnings
warnings.filterwarnings(
    action='default', category=DeprecationWarning, module='airflow')
warnings.filterwarnings(
    action='default', category=PendingDeprecationWarning, module='airflow')

if six.PY3:
    ConfigParser = configparser.ConfigParser
else:
    ConfigParser = configparser.SafeConfigParser


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
        shlex.split(command), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, stderr = [stream.decode(sys.getdefaultencoding(), 'ignore')
                      for stream in process.communicate()]

    if process.returncode != 0:
        raise AirflowConfigException(
            "Cannot execute {}. Error code is: {}. Output: {}, Stderr: {}"
            .format(command, process.returncode, output, stderr)
        )

    return output

_templates_dir = os.path.join(os.path.dirname(__file__), 'config_templates')
with open(os.path.join(_templates_dir, 'default_airflow.cfg')) as f:
    DEFAULT_CONFIG = f.read()
with open(os.path.join(_templates_dir, 'default_test.cfg')) as f:
    TEST_CONFIG = f.read()


class AirflowConfigParser(ConfigParser):

    # These configuration elements can be fetched as the stdout of commands
    # following the "{section}__{name}__cmd" pattern, the idea behind this
    # is to not store password on boxes in text files.
    as_command_stdout = {
        ('core', 'sql_alchemy_conn'),
        ('core', 'fernet_key'),
        ('celery', 'broker_url'),
        ('celery', 'celery_result_backend')
    }

    def __init__(self, *args, **kwargs):
        ConfigParser.__init__(self, *args, **kwargs)
        self.read_string(parameterized_config(DEFAULT_CONFIG))
        self.is_validated = False

    def read_string(self, string, source='<string>'):
        """
        Read configuration from a string.

        A backwards-compatible version of the ConfigParser.read_string()
        method that was introduced in Python 3.
        """
        # Python 3 added read_string() method
        if six.PY3:
            ConfigParser.read_string(self, string, source=source)
        # Python 2 requires StringIO buffer
        else:
            import StringIO
            self.readfp(StringIO.StringIO(string))

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
        if (section, key) in AirflowConfigParser.as_command_stdout:
            # if the original key is present, return it no matter what
            if self.has_option(section, key):
                return ConfigParser.get(self, section, key)
            # otherwise, execute the fallback key
            elif self.has_option(section, fallback_key):
                command = self.get(section, fallback_key)
                return run_command(command)

    def get(self, section, key, **kwargs):
        section = str(section).lower()
        key = str(key).lower()

        # first check environment variables
        option = self._get_env_var_option(section, key)
        if option is not None:
            return option

        # ...then the config file
        if self.has_option(section, key):
            return expand_env_var(
                ConfigParser.get(self, section, key, **kwargs))

        # ...then commands
        option = self._get_cmd_option(section, key)
        if option:
            return option

        else:
            logging.warning("section/key [{section}/{key}] not found "
                            "in config".format(**locals()))

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
        ConfigParser.read(self, filenames)
        self._validate()

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
        cfg = copy.deepcopy(self._sections)

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
                        not display_sensitive
                        and ev != 'AIRFLOW__CORE__UNIT_TEST_MODE'):
                    opt = '< hidden >'
                if display_source:
                    opt = (opt, 'env var')
                cfg.setdefault(section.lower(), OrderedDict()).update(
                    {key.lower(): opt})

        # add bash commands
        for (section, key) in AirflowConfigParser.as_command_stdout:
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


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise AirflowConfigException('Had trouble creating a directory')


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

TEMPLATE_START = (
    '# ----------------------- TEMPLATE BEGINS HERE -----------------------')
if not os.path.isfile(TEST_CONFIG_FILE):
    logging.info(
        'Creating new Airflow config file for unit tests in: {}'.format(
            TEST_CONFIG_FILE))
    with open(TEST_CONFIG_FILE, 'w') as f:
        cfg = parameterized_config(TEST_CONFIG)
        f.write(cfg.split(TEMPLATE_START)[-1].strip())
if not os.path.isfile(AIRFLOW_CONFIG):
    logging.info('Creating new Airflow config file in: {}'.format(
        AIRFLOW_CONFIG))
    with open(AIRFLOW_CONFIG, 'w') as f:
        cfg = parameterized_config(DEFAULT_CONFIG)
        f.write(cfg.split(TEMPLATE_START)[-1].strip())

logging.info("Reading the config from " + AIRFLOW_CONFIG)

conf = AirflowConfigParser()
conf.read(AIRFLOW_CONFIG)


def load_test_config():
    """
    Load the unit test configuration.

    Note: this is not reversible.
    """
    conf.load_test_config()

if conf.getboolean('core', 'unit_test_mode'):
    load_test_config()


def get(section, key, **kwargs):
    return conf.get(section, key, **kwargs)


def getboolean(section, key):
    return conf.getboolean(section, key)


def getfloat(section, key):
    return conf.getfloat(section, key)


def getint(section, key):
    return conf.getint(section, key)


def has_option(section, key):
    return conf.has_option(section, key)


def remove_option(section, option):
    return conf.remove_option(section, option)


def as_dict(display_source=False, display_sensitive=False):
    return conf.as_dict(
        display_source=display_source, display_sensitive=display_sensitive)
as_dict.__doc__ = conf.as_dict.__doc__


def set(section, option, value):  # noqa
    return conf.set(section, option, value)
