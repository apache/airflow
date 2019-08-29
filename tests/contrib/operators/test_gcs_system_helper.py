#!/usr/bin/env python
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
import os

from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

PATH_TO_FILE = os.environ.get("GCS_UPLOAD_FILE_PATH", "test_n9c8347r.txt")
PATH_TO_SAVED_FILE = os.environ.get("PATH_TO_SAVED_FILE", "test_download_n9c8347r.txt")
BUCKET = os.environ.get("GCP_GCS_BUCKET", "test-gcs-23e-5v5143")


class GcsSystemTestHelper(LoggingCommandExecutor):
    @staticmethod
    def create_test_file():
        # Create test file for upload
        with open(PATH_TO_FILE, "w+") as file:
            file.writelines(["This is a test file"])

    @staticmethod
    def remove_test_files():
        os.remove(PATH_TO_FILE)
        os.remove(PATH_TO_SAVED_FILE)

    def remove_bucket(self):
        self.execute_cmd(["gsutil", "rm", "-r", "gs://{bucket}".format(bucket=BUCKET)])
