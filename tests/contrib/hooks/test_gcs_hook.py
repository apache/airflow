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

from airflow.exceptions import AirflowException
import airflow.contrib.hooks.gcs_hook as gcs_hook


class TestGCSHookHelperFunctions(unittest.TestCase):

    def test_parse_gcs_url(self):
        """
        Test GCS url parsing
        """

        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/path/to/blob'),
            ('bucket', 'path/to/blob'))

        # invalid URI
        self.assertRaises(
            AirflowException,
            gcs_hook._parse_gcs_url,
            'gs:/bucket/path/to/blob')

        # trailing slash
        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/path/to/blob/'),
            ('bucket', 'path/to/blob'))

        # bucket only
        self.assertEqual(
            gcs_hook._parse_gcs_url('gs://bucket/'),
            ('bucket', ''))
