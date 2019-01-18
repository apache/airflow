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
import os
import six
import sys
import tempfile
from mock import patch, mock

from airflow import configuration as conf
from airflow.configuration import mkdir_p
from airflow.exceptions import AirflowConfigException


if six.PY2:
    # Need `assertWarns` back-ported from unittest2
    import unittest2 as unittest
else:
    import unittest

SETTINGS_FILE_VALID = """
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
        'task': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        },
    },
    'loggers': {
        'airflow': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        },
        'airflow.task': {
            'handlers': ['task'],
            'level': 'INFO',
            'propagate': False,
        },
    }
}
"""

SETTINGS_FILE_INVALID = """
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - %%(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'airflow': {
            'handlers': ['file.handler'], # this handler does not exists
            'level': 'INFO',
            'propagate': False
        }
    }
}
"""

SETTINGS_FILE_EMPTY = """
# Other settings here
"""

SETTINGS_DEFAULT_NAME = 'custom_airflow_local_settings'


class settings_context(object):
    """
    Sets a settings file and puts it in the Python classpath

    :param content:
          The content of the settings file
    """

    def __init__(self, content, dir=None, name='LOGGING_CONFIG'):
        self.content = content
        self.settings_root = tempfile.mkdtemp()
        filename = "{}.py".format(SETTINGS_DEFAULT_NAME)

        if dir:
            # Replace slashes by dots
            self.module = dir.replace('/', '.') + '.' + SETTINGS_DEFAULT_NAME + '.' + name

            # Create the directory structure
            dir_path = os.path.join(self.settings_root, dir)
            mkdir_p(dir_path)

            # Add the __init__ for the directories
            # This is required for Python 2.7
            basedir = self.settings_root
            for part in dir.split('/'):
                open(os.path.join(basedir, '__init__.py'), 'w').close()
                basedir = os.path.join(basedir, part)
            open(os.path.join(basedir, '__init__.py'), 'w').close()

            self.settings_file = os.path.join(dir_path, filename)
        else:
            self.module = SETTINGS_DEFAULT_NAME + '.' + name
            self.settings_file = os.path.join(self.settings_root, filename)

    def __enter__(self):
        with open(self.settings_file, 'w') as handle:
            handle.writelines(self.content)
        sys.path.append(self.settings_root)
        conf.set(
            'core',
            'logging_config_class',
            self.module
        )
        return self.settings_file

    def __exit__(self, *exc_info):
        # shutil.rmtree(self.settings_root)
        # Reset config
        conf.set('core', 'logging_config_class', '')
        sys.path.remove(self.settings_root)


class TestLoggingSettings(unittest.TestCase):
    # Make sure that the configure_logging is not cached
    def setUp(self):
        self.old_modules = dict(sys.modules)

    def tearDown(self):
        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for m in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[m]

    # When we try to load an invalid config file, we expect an error
    def test_loading_invalid_local_settings(self):
        from airflow.logging_config import configure_logging, log
        with settings_context(SETTINGS_FILE_INVALID):
            with patch.object(log, 'warning') as mock_info:
                # Load config
                with self.assertRaises(ValueError):
                    configure_logging()

                mock_info.assert_called_with(
                    'Unable to load the config, contains a configuration error.'
                )

    def test_loading_valid_complex_local_settings(self):
        # Test what happens when the config is somewhere in a subfolder
        module_structure = 'etc.airflow.config'
        dir_structure = module_structure.replace('.', '/')
        with settings_context(SETTINGS_FILE_VALID, dir_structure):
            from airflow.logging_config import configure_logging, log
            with patch.object(log, 'info') as mock_info:
                configure_logging()
                mock_info.assert_called_with(
                    'Successfully imported user-defined logging config from %s',
                    'etc.airflow.config.{}.LOGGING_CONFIG'.format(
                        SETTINGS_DEFAULT_NAME
                    )
                )

    # When we try to load a valid config
    def test_loading_valid_local_settings(self):
        with settings_context(SETTINGS_FILE_VALID):
            from airflow.logging_config import configure_logging, log
            with patch.object(log, 'info') as mock_info:
                configure_logging()
                mock_info.assert_called_with(
                    'Successfully imported user-defined logging config from %s',
                    '{}.LOGGING_CONFIG'.format(
                        SETTINGS_DEFAULT_NAME
                    )
                )

    # When we load an empty file, it should go to default
    def test_loading_no_local_settings(self):
        with settings_context(SETTINGS_FILE_EMPTY):
            from airflow.logging_config import configure_logging
            with self.assertRaises(ImportError):
                configure_logging()

    # When the key is not available in the configuration
    def test_when_the_config_key_does_not_exists(self):
        from airflow import logging_config
        conf_get = conf.get

        def side_effect(*args):
            if args[1] == 'logging_config_class':
                raise AirflowConfigException
            else:
                return conf_get(*args)

        logging_config.conf.get = mock.Mock(side_effect=side_effect)

        with patch.object(logging_config.log, 'debug') as mock_debug:
            logging_config.configure_logging()
            mock_debug.assert_any_call(
                'Could not find key logging_config_class in config'
            )

    # Just default
    def test_loading_local_settings_without_logging_config(self):
        from airflow.logging_config import configure_logging, log
        with patch.object(log, 'debug') as mock_info:
            configure_logging()
            mock_info.assert_called_with(
                'Unable to load custom logging, using default config instead'
            )

    def test_1_9_config(self):
        from airflow.logging_config import configure_logging
        conf.set('core', 'task_log_reader', 'file.task')
        try:
            with self.assertWarnsRegex(DeprecationWarning, r'file.task'):
                configure_logging()
                self.assertEqual(conf.get('core', 'task_log_reader'), 'task')
        finally:
            conf.remove_option('core', 'task_log_reader', remove_default=False)


if __name__ == '__main__':
    unittest.main()
