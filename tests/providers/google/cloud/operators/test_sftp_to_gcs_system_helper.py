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
"""
Helpers to perform system tests for the Google Cloud Storage service.
"""
import argparse
import os
import shutil

from airflow.providers.google.cloud.example_dags.example_sftp_to_gcs import (
    BUCKET_SRC, DIR, OBJECT_SRC_1, OBJECT_SRC_2, OBJECT_SRC_3, SUBDIR, TMP_PATH,
)
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY, GcpAuthenticator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")


class SFTPtoGcsTestHelper(LoggingCommandExecutor):
    """
    Helper class to perform system tests for the Google Cloud Storage service.
    """

    files_and_dirs = [
        (OBJECT_SRC_1, os.path.join(TMP_PATH, DIR)),
        (OBJECT_SRC_2, os.path.join(TMP_PATH, DIR)),
        (OBJECT_SRC_3, os.path.join(TMP_PATH, DIR)),
        (OBJECT_SRC_1, os.path.join(TMP_PATH, DIR, SUBDIR)),
        (OBJECT_SRC_2, os.path.join(TMP_PATH, DIR, SUBDIR)),
        (OBJECT_SRC_3, os.path.join(TMP_PATH, DIR, SUBDIR)),
    ]

    buckets = [BUCKET_SRC]

    def create_buckets(self):
        """Create a bucket in Google Cloud Storage service with sample content."""

        # 1. Create buckets
        for bucket in self.buckets:
            self.execute_cmd(["gsutil", "mb", "gs://{}".format(bucket)])

    def create_temp_files(self):
        for filename, dir_path in self.files_and_dirs:
            self._create_temp_file(filename, dir_path)

    def delete_temp_files(self):
        for filename, dir_path in self.files_and_dirs:
            self._delete_temp_file(filename, dir_path)
            self._delete_temp_dir(dir_path)

    def delete_buckets(self):
        """Delete bucket in Google Cloud Storage service"""
        self.execute_cmd(["gsutil", "rm", "gs://{}/**".format(BUCKET_SRC)])
        self.execute_cmd(["gsutil", "rb", "gs://{}".format(BUCKET_SRC)])

    @staticmethod
    def _create_temp_file(filename, dir_path="/tmp"):
        os.makedirs(dir_path, exist_ok=True)

        full_path = os.path.join(dir_path, filename)
        with open(full_path, "wb") as f:
            f.write(os.urandom(1 * 1024 * 1024))

    @staticmethod
    def _delete_temp_file(filename, dir_path):
        full_path = os.path.join(dir_path, filename)
        try:
            os.remove(full_path)
        except FileNotFoundError:
            pass
        if dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)

    @staticmethod
    def _delete_temp_dir(dir_path):
        if dir_path != "/tmp":
            shutil.rmtree(dir_path, ignore_errors=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create  bucket for system tests.")
    parser.add_argument(
        "--action",
        dest="action",
        required=True,
        choices=(
            "create-buckets",
            "delete-buckets",
            "before-tests",
            "after-tests",
            "create-files",
            "delete-files",
        ),
    )
    action = parser.parse_args().action

    helper = SFTPtoGcsTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_GCS_KEY)
    helper.log.info("Starting action: %s", action)

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == "before-tests":
            helper.create_buckets()
            helper.create_temp_files()
        elif action == "after-tests":
            helper.delete_buckets()
            helper.delete_temp_files()
        elif action == "create-buckets":
            helper.create_buckets()
        elif action == "delete-buckets":
            helper.delete_buckets()
        elif action == "create-files":
            helper.create_temp_files()
        elif action == "delete-files":
            helper.delete_temp_files()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info("Finishing action: %s", action)
