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

import shutil
import os
import unittest

from airflow.utils.log.remote_file_task_handler import RemoteFileTaskHandler
from datetime import datetime
from datetime import timedelta


class TestRemoteFileTaskHandler(unittest.TestCase):
    def setUp(self):
        super(TestRemoteFileTaskHandler, self).setUp()
        self.base_log_folder = "/tmp/log_test"
        self.remote_base_log_folder = "/tmp/log_test2"
        self.filename_template = "output.log"

    def test_remote_close(self):
        rfth = RemoteFileTaskHandler(self.base_log_folder,
                                     self.remote_base_log_folder,
                                     self.filename_template)
        rfth.log_relative_path = self.filename_template
        full_path = rfth._init_file()

        with open(full_path, 'w') as fp:
            fp.write('testing')

        rfth.close()

        secondary_path = os.path.join(self.remote_base_log_folder, self.filename_template)
        self.assertTrue(os.path.exists(secondary_path))

        with open(secondary_path) as fp:
            line = fp.read()
            self.assertEqual(line, 'testing')

    def tearDown(self):
        shutil.rmtree(self.base_log_folder, ignore_errors=True)
        shutil.rmtree(self.remote_base_log_folder, ignore_errors=True)
