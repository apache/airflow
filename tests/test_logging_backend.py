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

import unittest

from airflow import configuration
from airflow.logging_backend import cached_logging_backend, _get_logging_backend
from airflow.logging_backends.base_logging_backend import BaseLoggingBackend

class LoggingBackendTest(unittest.TestCase):

    def test_invalid_logging_backend(self):
        invalid_schema = "invalid_schema://localhost:1234"
        invalid_url_format = "invalid_url_format"
        self.assertRaises(ValueError, _get_logging_backend, invalid_schema)
        self.assertRaises(ValueError, _get_logging_backend, invalid_url_format)

    def test_default_test_configuration(self):
        configuration.load_test_config()
        logging_backend = cached_logging_backend()
        self.assertIsNone(logging_backend)
