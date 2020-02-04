#!/usr/bin/env python
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

from tests.providers.google.cloud.operators.test_cloud_sql_system_helper import CloudSqlQueryTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUDSQL_KEY, GcpAuthenticator

QUERY_SUFFIX = "_QUERY"

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Create or delete Cloud SQL instances for system tests.')
    parser.add_argument('--action', required=True,
                        choices=('create', 'delete', 'setup-instances',
                                 'before-tests', 'after-tests'))
    action = parser.parse_args().action

    helper = CloudSqlQueryTestHelper()
    gcp_authenticator = GcpAuthenticator(gcp_key=GCP_CLOUDSQL_KEY)
    helper.log.info('Starting action: {}'.format(action))
    gcp_authenticator.gcp_store_authentication()
    try:
        gcp_authenticator.gcp_authenticate()
        if action == 'before-tests':
            helper.create_instances(instance_suffix=QUERY_SUFFIX)
            helper.setup_instances(instance_suffix=QUERY_SUFFIX)
        elif action == 'after-tests':
            helper.delete_instances(instance_suffix=QUERY_SUFFIX)
        elif action == 'create':
            helper.create_instances(instance_suffix=QUERY_SUFFIX)
        elif action == 'delete':
            helper.delete_instances(instance_suffix=QUERY_SUFFIX)
        elif action == 'setup-instances':
            helper.setup_instances(instance_suffix=QUERY_SUFFIX)
        else:
            raise Exception("Unknown action: {}".format(action))
    finally:
        gcp_authenticator.gcp_restore_authentication()

    helper.log.info('Finishing action: {}'.format(action))
