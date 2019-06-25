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

import argparse
from tempfile import NamedTemporaryFile

from tests.contrib.utils.gcp_authenticator import GcpAuthenticator, GCP_AI_KEY
from tests.contrib.utils.logging_command_executor import LoggingCommandExecutor

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_BUCKET_NAME = os.environ.get('GCP_VISION_BUCKET_NAME', 'vision-bucket-system-test')
GCP_REFERENCE_IMAGE_URL = os.environ.get('GCP_VISION_REFERENCE_IMAGE_URL', 'gs://bucket-name/image.png')
GCP_ANNOTATE_IMAGE_URL = os.environ.get('GCP_VISION_ANNOTATE_IMAGE_URL', 'gs://bucket-name/image.png')
GCP_VIDEO_SOURCE_URL = os.environ.get('GCP_VISION_SOURCE_IMAGE_URL', "http://google.com/image.jpg")


class GCPVisionTestHelper(LoggingCommandExecutor):
    def create_bucket(self):
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
                "gs://%s/" % GCP_BUCKET_NAME,
            ]
        )

        with NamedTemporaryFile(suffix=".png") as file:
            self.execute_cmd(["curl", "-s", GCP_VIDEO_SOURCE_URL, "-o", file.name])
            self.execute_cmd(['gsutil', 'cp', file.name, GCP_REFERENCE_IMAGE_URL])
            self.execute_cmd(['gsutil', 'cp', file.name, GCP_ANNOTATE_IMAGE_URL])

    def delete_bucket(self):
        self.execute_cmd(['gsutil', 'rm', '-r', "gs://%s/" % GCP_BUCKET_NAME])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create and remove a bucket for system tests.')
    parser.add_argument(
        '--action',
        dest='action',
        required=True,
        choices=('create-bucket', 'delete-bucket', 'before-tests', 'after-tests'),
    )
    action = parser.parse_args().action

    helper = GCPVisionTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_AI_KEY)
    helper.log.info('Starting action: {}'.format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'create-bucket':
            helper.create_bucket()
        elif action == 'delete-bucket':
            helper.delete_bucket()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
