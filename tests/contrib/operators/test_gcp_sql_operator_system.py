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
import unittest

from airflow import AirflowException
from tests.contrib.utils.base_gcp_system_test_case import \
    SKIP_TEST_WARNING, DagGcpSystemTestCase
from tests.contrib.operators.test_gcp_sql_operator_system_helper import \
    CloudSqlQueryTestHelper
from tests.contrib.utils.gcp_authenticator import GCP_CLOUDSQL_KEY

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')

SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@unittest.skipIf(DagGcpSystemTestCase.skip_check(GCP_CLOUDSQL_KEY), SKIP_TEST_WARNING)
class CloudSqlExampleDagsIntegrationTest(DagGcpSystemTestCase):
    def __init__(self, method_name='runTest'):
        super(CloudSqlExampleDagsIntegrationTest, self).__init__(
            method_name,
            dag_id='example_gcp_sql',
            gcp_key=GCP_CLOUDSQL_KEY)

    def tearDown(self):
        # Delete instances just in case the test failed and did not cleanup after itself
        self.gcp_authenticator.gcp_authenticate()
        try:
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-failover-replica")
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-read-replica")
            SQL_QUERY_TEST_HELPER.delete_instances()
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="2")
            SQL_QUERY_TEST_HELPER.delete_service_account_acls()
        finally:
            self.gcp_authenticator.gcp_revoke_authentication()
        super(CloudSqlExampleDagsIntegrationTest, self).tearDown()

    def test_run_example_dag_cloudsql(self):
        try:
            self._run_dag()
        except AirflowException as e:
            self.log.warning(
                "In case you see 'The instance or operation is not in an appropriate "
                "state to handle the request' error - you "
                "can remove '.random' file from airflow folder and re-run "
                "the test. This will generate random name of the database for next run "
                "(the problem is that Cloud SQL keeps names of deleted instances in "
                "short-term cache).")
            raise e
