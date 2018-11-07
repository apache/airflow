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

from __future__ import print_function
from __future__ import unicode_literals

import os
from collections import OrderedDict

import six

from airflow import configuration
from airflow.configuration import conf, AirflowConfigParser, parameterized_config

if six.PY2:
    # Need `assertWarns` back-ported from unittest2
    import unittest2 as unittest
else:
    import unittest


class ConfTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ['AIRFLOW__TESTSECTION__TESTKEY'] = 'testvalue'
        os.environ['AIRFLOW__TESTSECTION__TESTPERCENT'] = 'with%percent'
        configuration.load_test_config()
        conf.set('core', 'percent', 'with%%inside')

    @classmethod
    def tearDownClass(cls):
        del os.environ['AIRFLOW__TESTSECTION__TESTKEY']
        del os.environ['AIRFLOW__TESTSECTION__TESTPERCENT']

    def test_env_var_config(self):
        opt = conf.get('testsection', 'testkey')
        self.assertEqual(opt, 'testvalue')

        opt = conf.get('testsection', 'testpercent')
        self.assertEqual(opt, 'with%percent')

    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        self.assertEqual(cfg_dict['core']['unit_test_mode'], 'True')

        self.assertEqual(cfg_dict['core']['percent'], 'with%inside')

        # test env vars
        self.assertEqual(cfg_dict['testsection']['testkey'], '< hidden >')

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

    def test_command_config(self):
        TEST_CONFIG = '''[test]
key1 = hello
key2_cmd = printf cmd_result
key3 = airflow
key4_cmd = printf key4_result
'''
        TEST_CONFIG_DEFAULT = '''[test]
key1 = awesome
key2 = airflow

[another]
key6 = value6
'''

        test_conf = AirflowConfigParser(
            default_config=parameterized_config(TEST_CONFIG_DEFAULT))
        test_conf.read_string(TEST_CONFIG)
        test_conf.as_command_stdout = test_conf.as_command_stdout | {
            ('test', 'key2'),
            ('test', 'key4'),
        }
        self.assertEqual('hello', test_conf.get('test', 'key1'))
        self.assertEqual('cmd_result', test_conf.get('test', 'key2'))
        self.assertEqual('airflow', test_conf.get('test', 'key3'))
        self.assertEqual('key4_result', test_conf.get('test', 'key4'))
        self.assertEqual('value6', test_conf.get('another', 'key6'))

        self.assertTrue(test_conf.has_option('test', 'key1'))
        self.assertTrue(test_conf.has_option('test', 'key2'))
        self.assertTrue(test_conf.has_option('test', 'key3'))
        self.assertTrue(test_conf.has_option('test', 'key4'))
        self.assertFalse(test_conf.has_option('test', 'key5'))
        self.assertTrue(test_conf.has_option('another', 'key6'))

        cfg_dict = test_conf.as_dict(display_sensitive=True)
        self.assertEqual('cmd_result', cfg_dict['test']['key2'])
        self.assertNotIn('key2_cmd', cfg_dict['test'])

    def test_remove_option(self):
        TEST_CONFIG = '''[test]
key1 = hello
key2 = airflow
'''
        TEST_CONFIG_DEFAULT = '''[test]
key1 = awesome
key2 = airflow
'''

        test_conf = AirflowConfigParser(
            default_config=parameterized_config(TEST_CONFIG_DEFAULT))
        test_conf.read_string(TEST_CONFIG)

        self.assertEqual('hello', test_conf.get('test', 'key1'))
        test_conf.remove_option('test', 'key1', remove_default=False)
        self.assertEqual('awesome', test_conf.get('test', 'key1'))

        test_conf.remove_option('test', 'key2')
        self.assertFalse(test_conf.has_option('test', 'key2'))

    def test_getsection(self):
        TEST_CONFIG = '''
[test]
key1 = hello
'''
        TEST_CONFIG_DEFAULT = '''
[test]
key1 = awesome
key2 = airflow

[another]
key3 = value3
'''
        test_conf = AirflowConfigParser(
            default_config=parameterized_config(TEST_CONFIG_DEFAULT))
        test_conf.read_string(TEST_CONFIG)

        self.assertEqual(
            OrderedDict([('key1', 'hello'), ('key2', 'airflow')]),
            test_conf.getsection('test')
        )
        self.assertEqual(
            OrderedDict([('key3', 'value3')]),
            test_conf.getsection('another')
        )

    def test_broker_transport_options(self):
        section_dict = conf.getsection("celery_broker_transport_options")
        self.assertTrue(isinstance(section_dict['visibility_timeout'], int))

        self.assertTrue(isinstance(section_dict['_test_only_bool'], bool))

        self.assertTrue(isinstance(section_dict['_test_only_float'], float))

        self.assertTrue(isinstance(section_dict['_test_only_string'], six.string_types))

    def test_deprecated_options(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options['celery'] = {
            'worker_concurrency': 'celeryd_concurrency',
        }

        # Remove it so we are sure we use the right setting
        conf.remove_option('celery', 'worker_concurrency')

        with self.assertWarns(DeprecationWarning):
            os.environ['AIRFLOW__CELERY__CELERYD_CONCURRENCY'] = '99'
            self.assertEquals(conf.getint('celery', 'worker_concurrency'), 99)
            os.environ.pop('AIRFLOW__CELERY__CELERYD_CONCURRENCY')

        with self.assertWarns(DeprecationWarning):
            conf.set('celery', 'celeryd_concurrency', '99')
            self.assertEquals(conf.getint('celery', 'worker_concurrency'), 99)
            conf.remove_option('celery', 'celeryd_concurrency')

    def test_deprecated_options_cmd(self):
        # Guarantee we have a deprecated setting, so we test the deprecation
        # lookup even if we remove this explicit fallback
        conf.deprecated_options['celery'] = {'result_backend': 'celery_result_backend'}
        conf.as_command_stdout.add(('celery', 'celery_result_backend'))

        conf.remove_option('celery', 'result_backend')
        conf.set('celery', 'celery_result_backend_cmd', '/bin/echo 99')

        with self.assertWarns(DeprecationWarning):
            tmp = None
            if 'AIRFLOW__CELERY__RESULT_BACKEND' in os.environ:
                tmp = os.environ.pop('AIRFLOW__CELERY__RESULT_BACKEND')
            self.assertEquals(conf.getint('celery', 'result_backend'), 99)
            if tmp:
                os.environ['AIRFLOW__CELERY__RESULT_BACKEND'] = tmp
