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

import sys
import unittest

from airflow import configuration


class CeleryConfTest(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()

    def tearDown(self):
        # We need to un-import the module as it reads the config at import time. :(
        del sys.modules['airflow.executors.celery_executor']

    def test_broker_transport_options_default(self):
        from airflow.executors.celery_executor import CeleryConfig

        self.assertEqual(CeleryConfig.BROKER_TRANSPORT_OPTIONS, {})

    def test_broker_transport_options_set(self):
        configuration.set('celery', 'BROKER_TRANSPORT_OPTIONS', '{"visibility_timeout": 10000}')
        from airflow.executors.celery_executor import CeleryConfig

        self.assertEqual(CeleryConfig.BROKER_TRANSPORT_OPTIONS, {'visibility_timeout': 10000})
