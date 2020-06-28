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

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from airflow import settings
from airflow.utils.file import find_path_from_directory


class TestIgnorePluginFile(unittest.TestCase):
    """
    Test that the .airflowignore work and whether the file is properly ignored.
    """

    def setUp(self):
        """
        Make tmp folder and set base path
        """
        self.test_dir = tempfile.mkdtemp()
        self.test_file = os.path.join(self.test_dir, 'test_file.txt')
        self.plugin_folder_path = os.path.join(self.test_dir, 'test_ignore')
        shutil.copytree('test_ignore', self.plugin_folder_path)
        self.mock_plugins_folder = patch.object(
            settings, 'PLUGINS_FOLDER', return_value=self.plugin_folder_path
        )

    def tearDown(self):
        """
        Delete tmp folder
        """
        shutil.rmtree(self.test_dir)

    def test_find_not_should_ignore_path(self):
        """
        Test that the .airflowignore work and whether the file is properly ignored.
        """

        detected_files = set()
        should_ignore_files = {
            'test_notload.py',
            'test_notload_sub.py',
            'test_noneload_sub1.py',
            'test_shouldignore.py'
        }
        should_not_ignore_files = {
            'test_load.py',
            'test_load_sub1.py'
        }
        ignore_list_file = ".airflowignore"
        for file_path in find_path_from_directory(self.plugin_folder_path, ignore_list_file):
            if not os.path.isfile(file_path):
                continue
            _, file_ext = os.path.splitext(os.path.split(file_path)[-1])
            if file_ext != '.py':
                continue
            detected_files.add(os.path.basename(file_path))
        self.assertEqual(detected_files, should_not_ignore_files)
        for path in detected_files:
            self.assertNotIn(path, should_ignore_files)
