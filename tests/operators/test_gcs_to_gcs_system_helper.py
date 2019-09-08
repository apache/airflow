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
Helpers to perform system tests for the Google Cloud Build service.
"""
import argparse
import os

from airflow.example_dags.example_gcs_to_gcs import (
    BUCKET_1_SRC,
    BUCKET_1_DST,
    BUCKET_2_SRC,
    BUCKET_2_DST,
    BUCKET_3_SRC,
    BUCKET_3_DST,
)
from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_GCS_KEY
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

GCP_REPOSITORY_NAME = os.environ.get("GCP_CLOUD_BUILD_REPOSITORY_NAME", "repository-name")


class GcsToGcsTestHelper(LoggingCommandExecutor):
    """
    Helper class to perform system tests for the Google Cloud Build service.
    """

    def create_buckets(self):
        """Create a buckets in Google Cloud Storage service with sample content."""

        # 1. Create bucket
        for name in [BUCKET_1_SRC, BUCKET_1_DST, BUCKET_2_SRC, BUCKET_2_DST, BUCKET_3_SRC, BUCKET_3_DST]:
            self.execute_cmd(["gsutil", "mb", "gs://{}".format(name)])

        # 2. Prepare parents
        first_parent = "gs://{}/parent-1.bin".format(BUCKET_1_SRC)
        second_parent = "gs://{}/parent-2.bin".format(BUCKET_1_SRC)

        self.execute_cmd(
            [
                "bash",
                "-c",
                "cat /dev/urandom | head -c $((1 * 1024 * 1024)) | gsutil cp - {}".format(first_parent),
            ]
        )

        self.execute_cmd(
            [
                "bash",
                "-c",
                "cat /dev/urandom | head -c $((1 * 1024 * 1024)) | gsutil cp - {}".format(second_parent),
            ]
        )

        self.execute_cmd(["gsutil", "cp", first_parent, "gs://{}/file.bin".format(BUCKET_1_SRC)])
        self.execute_cmd(["gsutil", "cp", first_parent, "gs://{}/subdir/file.bin".format(BUCKET_1_SRC)])

        self.execute_cmd(["gsutil", "cp", first_parent, "gs://{}/file.bin".format(BUCKET_2_SRC)])
        self.execute_cmd(["gsutil", "cp", first_parent, "gs://{}/subdir/file.bin".format(BUCKET_2_SRC)])
        self.execute_cmd(["gsutil", "cp", second_parent, "gs://{}/file.bin".format(BUCKET_2_DST)])
        self.execute_cmd(["gsutil", "cp", second_parent, "gs://{}/subdir/file.bin".format(BUCKET_2_DST)])
        self.execute_cmd(["gsutil", "cp", second_parent, "gs://{}/file.bin".format(BUCKET_3_DST)])
        self.execute_cmd(["gsutil", "cp", second_parent, "gs://{}/subdir/file.bin".format(BUCKET_3_DST)])

        self.execute_cmd(["gsutil", "rm", first_parent])
        self.execute_cmd(["gsutil", "rm", second_parent])

    def delete_buckets(self):
        """Delete buckets in Google Cloud Storage service"""

        for name in [BUCKET_1_SRC, BUCKET_1_DST, BUCKET_2_SRC, BUCKET_2_DST, BUCKET_3_SRC, BUCKET_3_DST]:
            self.execute_cmd(["gsutil", "rb", "gs://{}".format(name)])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create  buckets for system tests.")
    parser.add_argument(
        "--action",
        dest="action",
        required=True,
        choices=("create-buckets", "delete-buckets", "before-tests", "after-tests"),
    )
    action = parser.parse_args().action

    helper = GcsToGcsTestHelper()
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
