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

from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_COMPUTE_KEY
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCT_SOURCE_GCS_BUCKET_NAME = os.environ.get('GCT_SOURCE_BUCKET_NAME',
                                        'instance-bucket-test-1')
GCT_TARGET_GCS_BUCKET_NAME = os.environ.get('GCT_SOURCE_BUCKET_NAME',
                                        'instance-bucket-test-2')
GCT_SOURCE_AWS_BUCKET_NAME = os.environ.get('GCT_SOURCE_AWS_BUCKET_NAME',
                                            'instance-bucket-test-2')


class GCPComputeTestHelper(LoggingCommandExecutor):

    def create_s3_bucket(self):
        self.execute_cmd([
            "aws", "s3api",
            "create-bucket", "--bucket", GCT_SOURCE_AWS_BUCKET_NAME
        ])

    def delete_s3_bucket(self):
        self.execute_cmd([
            "aws", "s3api",
            "delete-bucket", "--bucket", GCT_SOURCE_AWS_BUCKET_NAME
        ])

    def create_gcs_buckets(self):
        self.execute_cmd([
            'gsutil', 'mb', "gs://%s/" % GCT_TARGET_GCS_BUCKET_NAME,
            "-p", GCP_PROJECT_ID,
        ])

        self.execute_cmd([
            'gsutil', 'mb', "gs://%s/" % GCT_SOURCE_GCS_BUCKET_NAME,
            "-p", GCP_PROJECT_ID,
        ])

    def delete_gcs_buckets(self):
        self.execute_cmd([
            'gsutil', 'rm', "-r", "gs://%s/" % GCT_TARGET_GCS_BUCKET_NAME
        ])

        self.execute_cmd([
            'gsutil', 'rm', "-r", "gs://%s/" % GCT_SOURCE_AWS_BUCKET_NAME,
        ])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create and delete buckets for system tests.')
    parser.add_argument('--action', dest='action', required=True,
                        choices=('create_s3_bucket', 'delete_s3_bucket',
                                 'create_gcs_buckets', 'delete_gcs_buckets',
                                 'before-tests', 'after-tests'))
    action = parser.parse_args().action

    helper = GCPComputeTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_COMPUTE_KEY)
    helper.log.info('Starting action: {}'.format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'create-s3_bucket':
            helper.create_s3_bucket()
        elif action == 'create-s3-bucket':
            helper.delete_s3_bucket()
        elif action == 'delete-s3-bucket':
            helper.create_gcs_buckets()
        elif action == 'delete-gcs-buckets':
            helper.delete_gcs_buckets()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
