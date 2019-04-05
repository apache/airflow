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
import subprocess

from googleapiclient._auth import default_credentials, with_scopes

from tests.contrib.utils.base_gcp_system_test_case import RetrieveVariables
from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_GCS_TRANSFER_KEY
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor
from googleapiclient import discovery


retrieve_variables = RetrieveVariables()

SERVICE_EMAIL_FORMAT = "project-%s@storage-transfer-service.iam.gserviceaccount.com"

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_TRANSFER_SOURCE_AWS_BUCKET = os.environ.get('GCP_TRANSFER_SOURCE_AWS_BUCKET', 'instance-bucket-test-2')
GCP_TRANSFER_FIRST_TARGET_BUCKET = os.environ.get(
    'GCP_TRANSFER_FIRST_TARGET_BUCKET', 'gcp-transfer-first-target'
)
GCP_TRANSFER_SECOND_TARGET_BUCKET = os.environ.get(
    'GCP_TRANSFER_SECOND_TARGET_BUCKET', 'gcp-transfer-second-target'
)

# 100 MB
TEST_FILE_SIZE = 1 * 1024 * 1024
TEST_FILE_NO = 30000
# Total: 30000 * 1 MB = 30GB


class GCPTransferTestHelper(LoggingCommandExecutor):
    def create_s3_bucket(self):
        self.execute_cmd(["aws", "s3", "mb", "s3://%s" % GCP_TRANSFER_SOURCE_AWS_BUCKET])

        self.execute_cmd(
            [
                "bash",
                "-c",
                "cat /dev/urandom | head -c %s | aws s3 cp - "
                "s3://%s/file-from-aws.bin" % (TEST_FILE_SIZE, GCP_TRANSFER_SOURCE_AWS_BUCKET),
            ]
        )

        self.execute_cmd(
            [
                "bash",
                "-c",
                "seq 1 %s | xargs -n 1 -P 16 -I {} "
                "aws s3 cp  s3://%s/file-from-aws.bin s3://%s/file-from-aws-{}.bin"
                % (TEST_FILE_NO, GCP_TRANSFER_SOURCE_AWS_BUCKET, GCP_TRANSFER_SOURCE_AWS_BUCKET),
            ]
        )

    def delete_s3_bucket(self):
        self.execute_cmd(["aws", "s3", "rb", "s3://%s" % GCP_TRANSFER_SOURCE_AWS_BUCKET, "--force"])

    def create_gcs_buckets(self):
        self.execute_cmd(
            [
                'gsutil',
                'mb',
                "-p",
                GCP_PROJECT_ID,
                "-c",
                "regional",
                "-l",
                "europe-north1",
                "gs://%s/" % GCP_TRANSFER_FIRST_TARGET_BUCKET,
            ]
        )

        self.execute_cmd(
            [
                'gsutil',
                'mb',
                "-p",
                GCP_PROJECT_ID,
                "-c",
                "regional",
                "-l",
                "asia-east1",
                "gs://%s/" % GCP_TRANSFER_SECOND_TARGET_BUCKET,
            ]
        )

        project_number = (
            subprocess.check_output(
                ['gcloud', 'projects', 'describe', GCP_PROJECT_ID, '--format', 'value(projectNumber)']
            )
            .decode("utf-8")
            .strip()
        )

        account_email = SERVICE_EMAIL_FORMAT % project_number

        self.execute_cmd(
            [
                "gsutil",
                "iam",
                "ch",
                "serviceAccount:%s:admin" % account_email,
                "gs://%s" % GCP_TRANSFER_SECOND_TARGET_BUCKET,
            ]
        )

        self.execute_cmd(
            [
                "gsutil",
                "iam",
                "ch",
                "serviceAccount:%s:admin" % account_email,
                "gs://%s" % GCP_TRANSFER_FIRST_TARGET_BUCKET,
            ]
        )

    def delete_gcs_buckets(self):
        self.execute_cmd(['gsutil', 'rm', "-r", "gs://%s/" % GCP_TRANSFER_FIRST_TARGET_BUCKET, "&"], True)

        self.execute_cmd(['gsutil', 'rm', "-r", "gs://%s/" % GCP_TRANSFER_SECOND_TARGET_BUCKET, "&"], True)

    @staticmethod
    def _get_transfer_service_account():
        credentials = default_credentials()
        credentials = with_scopes(credentials, scopes=['https://www.googleapis.com/auth/cloud-platform'])
        service = discovery.build('storagetransfer', 'v1', cache_discovery=False, credentials=credentials)

        request = service.googleServiceAccounts().get(projectId=GCP_PROJECT_ID)
        return request.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create and delete buckets for system tests.')
    parser.add_argument(
        '--action',
        dest='action',
        required=True,
        choices=(
            'create-s3-bucket',
            'delete-s3-bucket',
            'create-gcs-buckets',
            'delete-gcs-buckets',
            'before-tests',
            'after-tests',
        ),
    )
    action = parser.parse_args().action

    helper = GCPTransferTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_GCS_TRANSFER_KEY)
    helper.log.info('Starting action: {}'.format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'create-s3-bucket':
            helper.create_s3_bucket()
        elif action == 'delete-s3-bucket':
            helper.delete_s3_bucket()
        elif action == 'create-gcs-buckets':
            helper.create_gcs_buckets()
        elif action == 'delete-gcs-buckets':
            helper.delete_gcs_buckets()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
