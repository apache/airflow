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

from airflow import AirflowException
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSqlProxyRunner
from tests.providers.google.cloud.operators.test_cloud_sql_system_helper import CloudSqlQueryTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUDSQL_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')

SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@skip_gcp_system(GCP_CLOUDSQL_KEY)
class CloudSqlProxySystemTest(SystemTest):
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def setUp(self):
        super().setUp()
        SQL_QUERY_TEST_HELPER.check_if_instances_are_up(instance_suffix="_QUERY")

    @staticmethod
    def generate_unique_path():
        return ''.join(
            random.choice(string.ascii_letters + string.digits) for _ in range(8))

    def test_start_proxy_fail_no_parameters(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + self.generate_unique_path(),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='a')
        with self.assertRaises(AirflowException) as cm:
            runner.start_proxy()
        err = cm.exception
        self.assertIn("invalid instance name", str(err))
        with self.assertRaises(AirflowException) as cm:
            runner.start_proxy()
        err = cm.exception
        self.assertIn("invalid instance name", str(err))
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + self.generate_unique_path(),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='')
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def test_start_proxy_with_all_instances_generated_credential_file(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + self.generate_unique_path(),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='')
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances_specific_version(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + self.generate_unique_path(),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='',
                                     sql_proxy_version='v1.13')
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)
        self.assertEqual(runner.get_proxy_version(), "1.13")


@skip_gcp_system(GCP_CLOUDSQL_KEY)
class CloudSqlQueryExampleDagsSystemTest(SystemTest):
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def setUp(self):
        super().setUp()
        SQL_QUERY_TEST_HELPER.check_if_instances_are_up(instance_suffix="_QUERY")
        SQL_QUERY_TEST_HELPER.setup_instances(instance_suffix="_QUERY")

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def test_run_example_dag_cloudsql_query(self):
        self.run_dag('example_gcp_sql_query', CLOUD_DAG_FOLDER)
