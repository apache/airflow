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

from airflow import AirflowException
from tests.providers.google.cloud.operators.test_cloud_sql_system_helper import CloudSqlQueryTestHelper
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_CLOUDSQL_KEY
from tests.test_utils.gcp_system_helpers import CLOUD_DAG_FOLDER, provide_gcp_context, skip_gcp_system
from tests.test_utils.system_tests_class import SystemTest

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')

SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@skip_gcp_system(GCP_CLOUDSQL_KEY, require_local_executor=True)
class CloudSqlExampleDagsIntegrationTest(SystemTest):
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def tearDown(self):
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
                "can remove '.random' file from airflow folder and re-run "
                "the test. This will generate random name of the database for next run "
                "(the problem is that Cloud SQL keeps names of deleted instances in "
                "short-term cache).")
            raise e
