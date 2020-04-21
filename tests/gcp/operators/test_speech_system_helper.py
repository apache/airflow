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

import argparse
import os

from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from tests.gcp.utils.gcp_authenticator import GCP_GCS_KEY, GcpAuthenticator

SERVICE_EMAIL_FORMAT = "project-%s@storage-transfer-service.iam.gserviceaccount.com"

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
TARGET_BUCKET_NAME = os.environ.get("GCP_SPEECH_TEST_BUCKET", "gcp-speech-test-bucket")


class GCPTextToSpeechTestHelper(LoggingCommandExecutor):
    def create_target_bucket(self):
        self.execute_cmd(["gsutil", "mb", "-p", GCP_PROJECT_ID, "gs://%s/" % TARGET_BUCKET_NAME])

    def delete_target_bucket(self):
        self.execute_cmd(["gsutil", "rm", "-r", "gs://%s/" % TARGET_BUCKET_NAME], True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create and delete bucket for system tests.")
    parser.add_argument(
        "--action",
        dest="action",
        required=True,
        choices=("create-target-bucket", "delete-target-bucket", "before-tests", "after-tests"),
    )
    action = parser.parse_args().action

    helper = GCPTextToSpeechTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_GCS_KEY)
    helper.log.info("Starting action: {}".format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == "before-tests":
            helper.create_target_bucket()
        elif action == "after-tests":
            helper.delete_target_bucket()
        elif action == "create-target-bucket":
            helper.create_target_bucket()
        elif action == "delete-target-bucket":
            helper.delete_target_bucket()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info("Finishing action: {}".format(action))
