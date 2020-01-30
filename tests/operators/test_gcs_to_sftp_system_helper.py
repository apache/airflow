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
from itertools import product

from airflow.providers.google.cloud.example_dags.example_gcs_to_sftp import (
    BUCKET_SRC, OBJECT_SRC_1, OBJECT_SRC_2,
)
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY, GcpAuthenticator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")


class GcsToSFTPTestHelper(LoggingCommandExecutor):
    """
    Helper class to perform system tests for the Google Cloud Storage service.
    """

    def create_buckets(self):
        """Create a bucket in Google Cloud Storage service with sample content."""

        # 1. Create buckets
        self.execute_cmd(["gsutil", "mb", "gs://{}".format(BUCKET_SRC)])

        # 2. Prepare files
        for bucket_src, object_source in product(
            (
                BUCKET_SRC,
                "{}/subdir-1".format(BUCKET_SRC),
                "{}/subdir-2".format(BUCKET_SRC),
            ),
            (OBJECT_SRC_1, OBJECT_SRC_2),
        ):
            source_path = "gs://{}/{}".format(bucket_src, object_source)
            self.execute_cmd(
                [
                    "bash",
                    "-c",
                    "cat /dev/urandom | head -c $((1 * 1024 * 1024)) | gsutil cp - {}".format(
                        source_path
                    ),
                ]
            )

    def delete_buckets(self):
        """Delete bucket in Google Cloud Storage service"""
        self.execute_cmd(["gsutil", "rm", "gs://{}/**".format(BUCKET_SRC)])
        self.execute_cmd(["gsutil", "rb", "gs://{}".format(BUCKET_SRC)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create  bucket for system tests.")
    parser.add_argument(
        "--action",
        dest="action",
        required=True,
        choices=("create-buckets", "delete-buckets", "before-tests", "after-tests"),
    )
    action = parser.parse_args().action

    helper = GcsToSFTPTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_GCS_KEY)
    helper.log.info("Starting action: %s", action)

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == "before-tests":
            pass
        elif action == "after-tests":
            pass
        elif action == "create-buckets":
            helper.create_buckets()
        elif action == "delete-buckets":
            helper.delete_buckets()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info("Finishing action: %s", action)
