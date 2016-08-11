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

"""
Checks that existing loggers are preserved after importing airflow (settings).
"""

import logging
import unittest

from airflow import settings


class ConfigureLoggingTestCase(unittest.TestCase):
    def setUp(self):
        self.logger = logging.getLogger()
        self.handler = logging.NullHandler()

        self.existing_handlers = self.logger.handlers
        self.logger.handlers = []

    def tearDown(self):
        self.logger.handlers = self.existing_handlers

    def test(self):
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(self.handler)

        self.assertEqual(self.logger.handlers, [self.handler])

        settings.configure_logging()

        # Make sure our log settings are preserved.
        self.assertEqual(self.logger.level, logging.DEBUG)
        self.assertIn(self.handler, self.logger.handlers)

        # Make sure default StreamHandler got added.
        self.assertIn(settings.handler, self.logger.handlers)
