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
import random
import string
import time

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSqlProxyRunner
from tests.providers.google.cloud.operators.test_cloud_sql_system_helper import (
    QUERY_SUFFIX,
    TEARDOWN_LOCK_FILE,
    TEARDOWN_LOCK_FILE_QUERY,
    CloudSqlQueryTestHelper,
)
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUDSQL_KEY, GcpAuthenticator
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, GoogleSystemTest, provide_gcp_context

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')

SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_CLOUDSQL_KEY)
class CloudSqlExampleDagsIntegrationTest(GoogleSystemTest):
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def tearDown(self):
        if os.path.exists(TEARDOWN_LOCK_FILE):
            self.log.info("Skip deleting instances as they were created manually (helps to iterate on tests)")
        else:
            # Delete instances just in case the test failed and did not cleanup after itself
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-failover-replica")
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-read-replica")
            SQL_QUERY_TEST_HELPER.delete_instances()
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="2")
            SQL_QUERY_TEST_HELPER.delete_service_account_acls()
        super().tearDown()

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def test_run_example_dag_cloudsql(self):
        try:
            self.run_dag('example_gcp_sql', CLOUD_DAG_FOLDER)
        except AirflowException as e:
            self.log.warning(
                "In case you see 'The instance or operation is not in an appropriate "
                "state to handle the request' error - you "
                "can remove 'random.txt' file from /files/airflow-breeze-config/ folder and restart "
                "breeze environment. This will generate random name of the database for next run "
                "(the problem is that Cloud SQL keeps names of deleted instances in "
                "short-term cache)."
            )
            raise e


@pytest.mark.system("google.cloud")
@pytest.mark.credential_file(GCP_CLOUDSQL_KEY)
class CloudSqlProxySystemTest(GoogleSystemTest):
    @classmethod
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def setUpClass(cls):
        SQL_QUERY_TEST_HELPER.set_ip_addresses_in_env()
        if os.path.exists(TEARDOWN_LOCK_FILE_QUERY):
            print(
                "Skip creating and setting up instances as they were created manually "
                "(helps to iterate on tests)"
            )
        else:
            helper = CloudSqlQueryTestHelper()
            gcp_authenticator = GcpAuthenticator(gcp_key=GCP_CLOUDSQL_KEY)
            gcp_authenticator.gcp_store_authentication()
            try:
                gcp_authenticator.gcp_authenticate()
                helper.create_instances(instance_suffix=QUERY_SUFFIX)
                helper.setup_instances(instance_suffix=QUERY_SUFFIX)
            finally:
                gcp_authenticator.gcp_restore_authentication()

    @classmethod
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def tearDownClass(cls):
        if os.path.exists(TEARDOWN_LOCK_FILE_QUERY):
            print("Skip deleting instances as they were created manually (helps to iterate on tests)")
        else:
            helper = CloudSqlQueryTestHelper()
            gcp_authenticator = GcpAuthenticator(gcp_key=GCP_CLOUDSQL_KEY)
            gcp_authenticator.gcp_authenticate()
            helper.delete_instances(instance_suffix=QUERY_SUFFIX)

    @staticmethod
    def generate_unique_path():
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))

    def test_start_proxy_fail_no_parameters(self):
        runner = CloudSqlProxyRunner(
            path_prefix='/tmp/' + self.generate_unique_path(),
            project_id=GCP_PROJECT_ID,
            instance_specification='a',
        )
        with self.assertRaises(AirflowException) as cm:
            runner.start_proxy()
        err = cm.exception
        self.assertIn("The cloud_sql_proxy finished early", str(err))
        with self.assertRaises(AirflowException) as cm:
            runner.start_proxy()
        err = cm.exception
        self.assertIn("The cloud_sql_proxy finished early", str(err))
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances(self):
        runner = CloudSqlProxyRunner(
            path_prefix='/tmp/' + self.generate_unique_path(),
            project_id=GCP_PROJECT_ID,
            instance_specification='',
        )
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def test_start_proxy_with_all_instances_generated_credential_file(self):
        runner = CloudSqlProxyRunner(
            path_prefix='/tmp/' + self.generate_unique_path(),
            project_id=GCP_PROJECT_ID,
            instance_specification='',
        )
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances_specific_version(self):
        runner = CloudSqlProxyRunner(
            path_prefix='/tmp/' + self.generate_unique_path(),
            project_id=GCP_PROJECT_ID,
            instance_specification='',
            sql_proxy_version='v1.13',
        )
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)
        self.assertEqual(runner.get_proxy_version(), "1.13")

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def test_run_example_dag_cloudsql_query(self):
        self.run_dag('example_gcp_sql_query', CLOUD_DAG_FOLDER)
