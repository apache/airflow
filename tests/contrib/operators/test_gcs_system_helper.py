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

BUCKET_1 = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket")
BUCKET_2 = os.environ.get("GCP_GCS_BUCKET_1", "test-gcs-example-bucket-2")

PATH_TO_UPLOAD_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.txt")
PATH_TO_SAVED_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-download.txt")


class GcsSystemTestHelper(LoggingCommandExecutor):
    @staticmethod
    def create_test_file():
        # Create test file for upload
        with open(PATH_TO_UPLOAD_FILE, "w+") as file:
            file.writelines(["This is a test file"])

    @staticmethod
    def remove_test_files():
        os.remove(PATH_TO_UPLOAD_FILE)
        os.remove(PATH_TO_SAVED_FILE)

    def remove_bucket(self):
        self.execute_cmd(["gsutil", "rm", "-r", "gs://{bucket}".format(bucket=BUCKET_1)])
        self.execute_cmd(["gsutil", "rm", "-r", "gs://{bucket}".format(bucket=BUCKET_2)])
