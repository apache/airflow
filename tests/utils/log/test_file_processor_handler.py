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
from __future__ import annotations

import os
import shutil
from datetime import timedelta
from pathlib import Path

import time_machine

from airflow.utils import timezone
from airflow.utils.log.file_processor_handler import FileProcessorHandler


class TestFileProcessorHandler:
    def setup_method(self):
        self.base_log_folder = "/tmp/log_test"
        self.filename = "{filename}"
        self.filename_template = "{{ filename }}.log"
        self.dag_dir = "/dags"

    def test_non_template(self):
        date = timezone.utcnow().strftime("%Y-%m-%d")
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder, filename_template=self.filename)
        handler.dag_dir = self.dag_dir

        path = os.path.join(self.base_log_folder, "latest")
        assert os.path.islink(path)
        assert os.path.basename(os.readlink(path)) == date

        handler.set_context(filename=os.path.join(self.dag_dir, "logfile"))
        assert os.path.exists(os.path.join(path, "logfile"))

    def test_template(self):
        date = timezone.utcnow().strftime("%Y-%m-%d")
        handler = FileProcessorHandler(
            base_log_folder=self.base_log_folder, filename_template=self.filename_template
        )
        handler.dag_dir = self.dag_dir

        path = os.path.join(self.base_log_folder, "latest")
        assert os.path.islink(path)
        assert os.path.basename(os.readlink(path)) == date

        handler.set_context(filename=os.path.join(self.dag_dir, "logfile"))
        assert os.path.exists(os.path.join(path, "logfile.log"))

    def test_symlink_latest_log_directory(self):
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder, filename_template=self.filename)
        handler.dag_dir = self.dag_dir

        date1 = (timezone.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")
        date2 = (timezone.utcnow() + timedelta(days=2)).strftime("%Y-%m-%d")

        path1 = Path(self.base_log_folder, date1, "log1")
        path2 = Path(self.base_log_folder, date1, "log2")

        path1.unlink(missing_ok=True)
        path2.unlink(missing_ok=True)

        link = Path(self.base_log_folder, "latest")

        with time_machine.travel(date1, tick=False):
            handler.set_context(filename=os.path.join(self.dag_dir, "log1"))
            assert link.is_symlink()
            assert link.readlink().name == date1
            assert (link / "log1").exists()

        with time_machine.travel(date2, tick=False):
            handler.set_context(filename=os.path.join(self.dag_dir, "log2"))
            assert link.is_symlink()
            assert link.readlink().name == date2
            assert (link / "log2").exists()

    def test_symlink_latest_log_directory_exists(self):
        handler = FileProcessorHandler(base_log_folder=self.base_log_folder, filename_template=self.filename)
        handler.dag_dir = self.dag_dir

        date1 = (timezone.utcnow() + timedelta(days=1)).strftime("%Y-%m-%d")

        path1 = Path(self.base_log_folder, date1, "log1")
        path1.unlink(missing_ok=True)

        link = Path(self.base_log_folder, "latest")
        link.rmdir(missing_ok=True)
        link.mkdir(parents=True, exist_ok=True)

        with time_machine.travel(date1, tick=False):
            handler.set_context(filename=os.path.join(self.dag_dir, "log1"))

    def teardown_method(self):
        shutil.rmtree(self.base_log_folder, ignore_errors=True)
