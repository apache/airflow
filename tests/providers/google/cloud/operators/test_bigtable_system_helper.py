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
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_SPANNER_KEY, GcpAuthenticator

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
CBT_INSTANCE = os.environ.get('CBT_INSTANCE_ID', 'testinstance')


class GCPBigtableTestHelper(LoggingCommandExecutor):

    def delete_instance(self):
        self.execute_cmd([
            'gcloud', 'bigtable', '--project', GCP_PROJECT_ID,
            '--quiet', '--verbosity=none',
            'instances', 'delete', CBT_INSTANCE
        ])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create or delete spanner instances for system tests.')
    parser.add_argument('--action', dest='action', required=True,
                        choices=('delete-instance',
                                 'before-tests', 'after-tests'))
    action = parser.parse_args().action

    helper = GCPBigtableTestHelper()
    gcp_authenticator = GcpAuthenticator(GCP_SPANNER_KEY)
    helper.log.info('Starting action: {}'.format(action))

    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            pass
        elif action == 'after-tests':
            pass
        elif action == 'delete-instance':
            helper.delete_instance()
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
