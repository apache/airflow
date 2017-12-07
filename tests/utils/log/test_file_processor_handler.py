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

from airflow.utils.log.file_processor_handler import FileProcessorHandler
from airflow.utils import timezone
from datetime import timedelta
from freezegun import freeze_time


class TestFileProcessorHandler(unittest.TestCase):
    def setUp(self):
        super(TestFileProcessorHandler, self).setUp()
        self.base_log_folder = "/tmp/log_test"
        self.filename = "{filename}"
        self.filename_template = "{{ filename }}.log"
        self.dag_dir = "/dags"

    def test_non_template(self):
        date = timezone.utcnow().strftime("%Y-%m-%d")
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder,
                                       filename_template=self.filename)
        handler.dag_dir = self.dag_dir

        path = os.path.join(self.base_log_folder, "latest")
        self.assertTrue(os.path.islink(path))
        self.assertEqual(os.path.basename(os.readlink(path)), date)

        handler.set_context(filename=os.path.join(self.dag_dir, "logfile"))
        self.assertTrue(os.path.exists(os.path.join(path, "logfile")))

    def test_template(self):
        date = timezone.utcnow().strftime("%Y-%m-%d")
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder,
                                       filename_template=self.filename_template)
        handler.dag_dir = self.dag_dir

        path = os.path.join(self.base_log_folder, "latest")
        self.assertTrue(os.path.islink(path))
        self.assertEqual(os.path.basename(os.readlink(path)), date)

        handler.set_context(filename=os.path.join(self.dag_dir, "logfile"))
        self.assertTrue(os.path.exists(os.path.join(path, "logfile.log")))

    def test_symlink_latest_log_directory(self):
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder,
                                       filename_template=self.filename)
        handler.dag_dir = self.dag_dir

        date1 = (timezone.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        date2 = (timezone.utcnow() + timedelta(days=2)).strftime("%Y-%m-%d")

        p1 = os.path.join(self.base_log_folder, date1, "log1")
        p2 = os.path.join(self.base_log_folder, date1, "log2")

        if os.path.exists(p1):
            os.remove(p1)
        if os.path.exists(p2):
            os.remove(p2)

        link = os.path.join(self.base_log_folder, "latest")

        with freeze_time(date1):
            handler.set_context(filename=os.path.join(self.dag_dir, "log1"))
            self.assertTrue(os.path.islink(link))
            self.assertEqual(os.path.basename(os.readlink(link)), date1)
            self.assertTrue(os.path.exists(os.path.join(link, "log1")))

        with freeze_time(date2):
            handler.set_context(filename=os.path.join(self.dag_dir, "log2"))
            self.assertTrue(os.path.islink(link))
            self.assertEqual(os.path.basename(os.readlink(link)), date2)
            self.assertTrue(os.path.exists(os.path.join(link, "log2")))

    def tearDown(self):
        shutil.rmtree(self.base_log_folder, ignore_errors=True)
