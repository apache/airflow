# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import unittest

import six

from airflow import configuration
from airflow.configuration import conf


class ConfTest(unittest.TestCase):

    def setup(self):
        configuration.load_test_config()

    def test_env_var_config(self):
        opt = conf.get('testsection', 'testkey')
        self.assertEqual(opt, 'testvalue')

    def test_conf_as_dict(self):
        cfg_dict = conf.as_dict()

        # test that configs are picked up
        self.assertEqual(cfg_dict['core']['unit_test_mode'], 'True')

        # test env vars
        self.assertEqual(cfg_dict['testsection']['testkey'], '< hidden >')

        # test display_source
        cfg_dict = conf.as_dict(display_source=True)
        self.assertEqual(
            cfg_dict['core']['load_examples'][1], 'airflow config')
        self.assertEqual(
            cfg_dict['testsection']['testkey'], ('< hidden >', 'env var'))

        # test display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True)
        self.assertEqual(cfg_dict['testsection']['testkey'], 'testvalue')

        # test display_source and display_sensitive
        cfg_dict = conf.as_dict(display_sensitive=True, display_source=True)
        self.assertEqual(
            cfg_dict['testsection']['testkey'], ('testvalue', 'env var'))

    def test_broker_transport_options(self):
        section_dict = conf.getsection("celery_broker_transport_options")
        self.assertTrue(isinstance(section_dict['visibility_timeout'], int))

        self.assertTrue(isinstance(section_dict['_test_only_bool'], bool))

        self.assertTrue(isinstance(section_dict['_test_only_float'], float))

        self.assertTrue(isinstance(section_dict['_test_only_string'], six.string_types))
