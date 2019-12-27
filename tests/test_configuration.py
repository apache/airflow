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
import unittest
import warnings
from collections import OrderedDict
from unittest import mock

from airflow import configuration
from airflow.configuration import (
    AirflowConfigParser, conf, expand_env_var, get_airflow_config, get_airflow_home, parameterized_config,
)
from tests.test_utils.config import conf_vars
from tests.test_utils.reset_warning_registry import reset_warning_registry


@unittest.mock.patch.dict('os.environ', {
    'AIRFLOW__TESTSECTION__TESTKEY': 'testvalue',
    'AIRFLOW__TESTSECTION__TESTPERCENT': 'with%percent',
    'AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD': 'echo -n "OK"',
    'AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD': 'echo -n "NOT OK"'
})
class TestConf(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        conf.set('core', 'percent', 'with%%inside')

    def test_airflow_home_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_HOME' in os.environ:
                del os.environ['AIRFLOW_HOME']
            self.assertEqual(
                get_airflow_home(),
                expand_env_var('~/airflow'))

    def test_airflow_home_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_HOME='/path/to/airflow'):
            self.assertEqual(
                get_airflow_home(),
                '/path/to/airflow')

    def test_airflow_config_default(self):
        with unittest.mock.patch.dict('os.environ'):
            if 'AIRFLOW_CONFIG' in os.environ:
                del os.environ['AIRFLOW_CONFIG']
            self.assertEqual(
                get_airflow_config('/home/airflow'),
                expand_env_var('/home/airflow/airflow.cfg'))

    def test_airflow_config_override(self):
        with unittest.mock.patch.dict('os.environ', AIRFLOW_CONFIG='/path/to/airflow/airflow.cfg'):
            self.assertEqual(
                get_airflow_config('/home//airflow'),
                '/path/to/airflow/airflow.cfg')

    def test_case_sensitivity(self):
        # section and key are case insensitive for get method
        # note: this is not the case for as_dict method
        self.assertEqual(conf.get("core", "percent"), "with%inside")
        self.assertEqual(conf.get("core", "PERCENT"), "with%inside")
        self.assertEqual(conf.get("CORE", "PERCENT"), "with%inside")

    def test_env_var_config(self):
        opt = conf.get('testsection', 'testkey')
        self.assertEqual(opt, 'testvalue')

        opt = conf.get('testsection', 'testpercent')
        self.assertEqual(opt, 'with%percent')

        self.assertTrue(conf.has_option('testsection', 'testkey'))

        with unittest.mock.patch.dict(
            'os.environ',
            AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
        ):
            opt = conf.get('kubernetes_environment_variables', 'AIRFLOW__TESTSECTION__TESTKEY')
            self.assertEqual(opt, 'nested')

    @mock.patch.dict(
        'os.environ',
        AIRFLOW__KUBERNETES_ENVIRONMENT_VARIABLES__AIRFLOW__TESTSECTION__TESTKEY='nested'
    )
    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        self.assertEqual(cfg_dict['core']['unit_test_mode'], 'True')

        self.assertEqual(cfg_dict['core']['percent'], 'with%inside')

        # test env vars
        self.assertEqual(cfg_dict['testsection']['testkey'], '< hidden >')
        self.assertEqual(
            cfg_dict['kubernetes_environment_variables']['AIRFLOW__TESTSECTION__TESTKEY'],
            '< hidden >')

    def test_conf_as_dict_source(self):
        # test display_source
        cfg_dict = conf.as_dict(display_source=True)
        self.assertEqual(
            cfg_dict['core']['load_examples'][1], 'airflow.cfg')
        self.assertEqual(
            cfg_dict['testsection']['testkey'], ('< hidden >', 'env var'))

    def test_conf_as_dict_sensitive(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True)
        self.assertEqual(cfg_dict['testsection']['testkey'], 'testvalue')
        self.assertEqual(cfg_dict['testsection']['testpercent'], 'with%percent')

        # test display_source and display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True, display_source=True)
        self.assertEqual(
            cfg_dict['testsection']['testkey'], ('testvalue', 'env var'))

    def test_conf_as_dict_raw(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(raw=True, display_sensitive=True)
        self.assertEqual(cfg_dict['testsection']['testkey'], 'testvalue')

        # Values with '%' in them should be escaped
        self.assertEqual(cfg_dict['testsection']['testpercent'], 'with%%percent')
        self.assertEqual(cfg_dict['core']['percent'], 'with%%inside')

    def test_conf_as_dict_exclude_env(self):
        # test display_sensitive
        cfg_dict = conf.as_dict(include_env=False, display_sensitive=True)

        # Since testsection is only created from env vars, it shouldn't be
        # present at all if we don't ask for env vars to be included.
        self.assertNotIn('testsection', cfg_dict)

    def test_command_precedence(self):
        test_config = '''[test]
key1 = hello
key2_cmd = printf cmd_result
key3 = airflow
key4_cmd = printf key4_result
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow

[another]
key6 = value6
'''

        test_conf = AirflowConfigParser(
            default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)
        test_conf.as_command_stdout = test_conf.as_command_stdout | {
            ('test', 'key2'),
            ('test', 'key4'),
        }
        self.assertEqual('hello', test_conf.get('test', 'key1'))
        self.assertEqual('cmd_result', test_conf.get('test', 'key2'))
        self.assertEqual('airflow', test_conf.get('test', 'key3'))
        self.assertEqual('key4_result', test_conf.get('test', 'key4'))
        self.assertEqual('value6', test_conf.get('another', 'key6'))

        self.assertEqual('hello', test_conf.get('test', 'key1', fallback='fb'))
        self.assertEqual('value6', test_conf.get('another', 'key6', fallback='fb'))
        self.assertEqual('fb', test_conf.get('another', 'key7', fallback='fb'))
        self.assertEqual(True, test_conf.getboolean('another', 'key8_boolean', fallback='True'))
        self.assertEqual(10, test_conf.getint('another', 'key8_int', fallback='10'))
        self.assertEqual(1.0, test_conf.getfloat('another', 'key8_float', fallback='1'))

        self.assertTrue(test_conf.has_option('test', 'key1'))
        self.assertTrue(test_conf.has_option('test', 'key2'))
        self.assertTrue(test_conf.has_option('test', 'key3'))
        self.assertTrue(test_conf.has_option('test', 'key4'))
        self.assertFalse(test_conf.has_option('test', 'key5'))
        self.assertTrue(test_conf.has_option('another', 'key6'))

        cfg_dict = test_conf.as_dict(display_sensitive=True)
        self.assertEqual('cmd_result', cfg_dict['test']['key2'])
        self.assertNotIn('key2_cmd', cfg_dict['test'])

        # If we exclude _cmds then we should still see the commands to run, not
        # their values
        cfg_dict = test_conf.as_dict(include_cmds=False, display_sensitive=True)
        self.assertNotIn('key4', cfg_dict['test'])
        self.assertEqual('printf key4_result', cfg_dict['test']['key4_cmd'])

    def test_getboolean(self):
        """Test AirflowConfigParser.getboolean"""
        test_config = """
[type_validation]
key1 = non_bool_value

[true]
key2 = t
key3 = true
key4 = 1

[false]
key5 = f
key6 = false
key7 = 0

[inline-comment]
key8 = true #123
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with self.assertRaises(ValueError):
            test_conf.getboolean('type_validation', 'key1')
        self.assertTrue(isinstance(test_conf.getboolean('true', 'key3'), bool))
        self.assertEqual(True, test_conf.getboolean('true', 'key2'))
        self.assertEqual(True, test_conf.getboolean('true', 'key3'))
        self.assertEqual(True, test_conf.getboolean('true', 'key4'))
        self.assertEqual(False, test_conf.getboolean('false', 'key5'))
        self.assertEqual(False, test_conf.getboolean('false', 'key6'))
        self.assertEqual(False, test_conf.getboolean('false', 'key7'))
        self.assertEqual(True, test_conf.getboolean('inline-comment', 'key8'))

    def test_getint(self):
        """Test AirflowConfigParser.getint"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with self.assertRaises(ValueError):
            test_conf.getint('invalid', 'key1')
        self.assertTrue(isinstance(test_conf.getint('valid', 'key2'), int))
        self.assertEqual(1, test_conf.getint('valid', 'key2'))

    def test_getfloat(self):
        """Test AirflowConfigParser.getfloat"""
        test_config = """
[invalid]
key1 = str

[valid]
key2 = 1.23
"""
        test_conf = AirflowConfigParser(default_config=test_config)
        with self.assertRaises(ValueError):
            test_conf.getfloat('invalid', 'key1')
        self.assertTrue(isinstance(test_conf.getfloat('valid', 'key2'), float))
        self.assertEqual(1.23, test_conf.getfloat('valid', 'key2'))

    def test_has_option(self):
        test_config = '''[test]
key1 = value1
'''
        test_conf = AirflowConfigParser()
        test_conf.read_string(test_config)
        self.assertTrue(test_conf.has_option('test', 'key1'))
        self.assertFalse(test_conf.has_option('test', 'key_not_exists'))
        self.assertFalse(test_conf.has_option('section_not_exists', 'key1'))

    def test_remove_option(self):
        test_config = '''[test]
key1 = hello
key2 = airflow
'''
        test_config_default = '''[test]
key1 = awesome
key2 = airflow
'''

        test_conf = AirflowConfigParser(
            default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        self.assertEqual('hello', test_conf.get('test', 'key1'))
        test_conf.remove_option('test', 'key1', remove_default=False)
        self.assertEqual('awesome', test_conf.get('test', 'key1'))

        test_conf.remove_option('test', 'key2')
        self.assertFalse(test_conf.has_option('test', 'key2'))

    def test_getsection(self):
        test_config = '''
[test]
key1 = hello
'''
        test_config_default = '''
[test]
key1 = awesome
key2 = airflow

[testsection]
key3 = value3
'''
        test_conf = AirflowConfigParser(
            default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        self.assertEqual(
            OrderedDict([('key1', 'hello'), ('key2', 'airflow')]),
            test_conf.getsection('test')
        )
        self.assertEqual(
            OrderedDict([
                ('key3', 'value3'),
                ('testkey', 'testvalue'),
                ('testpercent', 'with%percent')]),
            test_conf.getsection('testsection')
        )

    def test_kubernetes_environment_variables_section(self):
        test_config = '''
[kubernetes_environment_variables]
key1 = hello
AIRFLOW_HOME = /root/airflow
'''
        test_config_default = '''
[kubernetes_environment_variables]
'''
        test_conf = AirflowConfigParser(
            default_config=parameterized_config(test_config_default))
        test_conf.read_string(test_config)

        self.assertEqual(
            OrderedDict([('key1', 'hello'), ('AIRFLOW_HOME', '/root/airflow')]),
            test_conf.getsection('kubernetes_environment_variables')
        )

    def test_broker_transport_options(self):
        section_dict = conf.getsection("celery_broker_transport_options")
        self.assertTrue(isinstance(section_dict['visibility_timeout'], int))
        self.assertTrue(isinstance(section_dict['_test_only_bool'], bool))
        self.assertTrue(isinstance(section_dict['_test_only_float'], float))
        self.assertTrue(isinstance(section_dict['_test_only_string'], str))

    @conf_vars({
        ("celery", "worker_concurrency"): None,
        ("celery", "celeryd_concurrency"): None,
    })
    def test_deprecated_options(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options = {
            ('celery', 'worker_concurrency'): ('celery', 'celeryd_concurrency'),
        }

        # Remove it so we are sure we use the right setting
        conf.remove_option('celery', 'worker_concurrency')

        with self.assertWarns(DeprecationWarning):
            with mock.patch.dict('os.environ', AIRFLOW__CELERY__CELERYD_CONCURRENCY="99"):
                self.assertEqual(conf.getint('celery', 'worker_concurrency'), 99)

        with self.assertWarns(DeprecationWarning):
            conf.set('celery', 'celeryd_concurrency', '99')
            self.assertEqual(conf.getint('celery', 'worker_concurrency'), 99)
            conf.remove_option('celery', 'celeryd_concurrency')

    @conf_vars({
        ('logging', 'logging_level'): None,
        ('core', 'logging_level'): None,
    })
    def test_deprecated_options_with_new_section(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options = {
            ('logging', 'logging_level'): ('core', 'logging_level'),
        }

        # Remove it so we are sure we use the right setting
        conf.remove_option('core', 'logging_level')
        conf.remove_option('logging', 'logging_level')

        with self.assertWarns(DeprecationWarning):
            with mock.patch.dict('os.environ', AIRFLOW__CORE__LOGGING_LEVEL="VALUE"):
                self.assertEqual(conf.get('logging', 'logging_level'), "VALUE")

        with self.assertWarns(DeprecationWarning):
            conf.set('core', 'logging_level', 'VALUE')
            self.assertEqual(conf.get('logging', 'logging_level'), "VALUE")
            conf.remove_option('core', 'logging_level')

    @conf_vars({
        ("celery", "result_backend"): None,
        ("celery", "celery_result_backend"): None,
        ("celery", "celery_result_backend_cmd"): None,
    })
    def test_deprecated_options_cmd(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options[('celery', "result_backend")] = ('celery', 'celery_result_backend')
        conf.as_command_stdout.add(('celery', 'celery_result_backend'))

        conf.remove_option('celery', 'result_backend')
        conf.set('celery', 'celery_result_backend_cmd', '/bin/echo 99')

        with self.assertWarns(DeprecationWarning):
            tmp = None
            if 'AIRFLOW__CELERY__RESULT_BACKEND' in os.environ:
                tmp = os.environ.pop('AIRFLOW__CELERY__RESULT_BACKEND')
            self.assertEqual(conf.getint('celery', 'result_backend'), 99)
            if tmp:
                os.environ['AIRFLOW__CELERY__RESULT_BACKEND'] = tmp

    def test_deprecated_values(self):
        def make_config():
            test_conf = AirflowConfigParser(default_config='')
            # Guarantee we have a deprecated setting, so we test the deprecation
            # lookup even if we remove this explicit fallback
            test_conf.deprecated_values = {
                'core': {
                    'task_runner': ('BashTaskRunner', 'StandardTaskRunner', '2.0'),
                },
            }
            test_conf.read_dict({
                'core': {
                    'executor': 'SequentialExecutor',
                    'task_runner': 'BashTaskRunner',
                    'sql_alchemy_conn': 'sqlite://',
                },
            })
            return test_conf

        with self.assertWarns(FutureWarning):
            test_conf = make_config()
            self.assertEqual(test_conf.get('core', 'task_runner'), 'StandardTaskRunner')

        with self.assertWarns(FutureWarning):
            with unittest.mock.patch.dict('os.environ', AIRFLOW__CORE__TASK_RUNNER='BashTaskRunner'):
                test_conf = make_config()

                self.assertEqual(test_conf.get('core', 'task_runner'), 'StandardTaskRunner')
        with reset_warning_registry():
            with warnings.catch_warnings(record=True) as warning:
                with unittest.mock.patch.dict('os.environ', AIRFLOW__CORE__TASK_RUNNER='NotBashTaskRunner'):
                    test_conf = make_config()

                    self.assertEqual(test_conf.get('core', 'task_runner'), 'NotBashTaskRunner')

                    self.assertListEqual([], warning)

    def test_deprecated_funcs(self):
        for func in ['load_test_config', 'get', 'getboolean', 'getfloat', 'getint', 'has_option',
                     'remove_option', 'as_dict', 'set']:
            with mock.patch('airflow.configuration.{}'.format(func)):
                with self.assertWarns(DeprecationWarning):
                    getattr(configuration, func)()

    def test_command_from_env(self):
        test_cmdenv_config = '''[testcmdenv]
itsacommand = NOT OK
notacommand = OK
'''
        test_cmdenv_conf = AirflowConfigParser()
        test_cmdenv_conf.read_string(test_cmdenv_config)
        test_cmdenv_conf.as_command_stdout.add(('testcmdenv', 'itsacommand'))
        with unittest.mock.patch.dict('os.environ'):
            # AIRFLOW__TESTCMDENV__ITSACOMMAND_CMD maps to ('testcmdenv', 'itsacommand') in
            # as_command_stdout and therefore should return 'OK' from the environment variable's
            # echo command, and must not return 'NOT OK' from the configuration
            self.assertEqual(test_cmdenv_conf.get('testcmdenv', 'itsacommand'), 'OK')
            # AIRFLOW__TESTCMDENV__NOTACOMMAND_CMD maps to no entry in as_command_stdout and therefore
            # the option should return 'OK' from the configuration, and must not return 'NOT OK' from
            # the environement variable's echo command
            self.assertEqual(test_cmdenv_conf.get('testcmdenv', 'notacommand'), 'OK')
