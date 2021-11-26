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
import uuid

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
CLOUD_SQL_BUCKET_NAME = os.environ.get('CLOUD_SQL_BUCKET_NAME', 'INVALID BUCKET NAME')

SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@pytest.fixture(scope='class')
def env_patch():
    """
    A convenient fixture for environment variables patching.
    All modifications will be undone after the requesting test class has finished.
    """
    from _pytest.monkeypatch import MonkeyPatch

    mpatch = MonkeyPatch()
    yield mpatch
    mpatch.undo()


@pytest.fixture(scope='module')
def default_instances():
    """
    Collect list of environment variables default values
    """
    mysql_instance_name = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME', 'test-mysql') + '%s'
    mysql_instance_name2 = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME2', 'test-mysql2') + '%s'
    mysql_instance_name_query = mysql_instance_name + QUERY_SUFFIX
    postgres_instance_name = os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME', 'test-postgres') + '%s'
    postgres_instance_name2 = os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME2', 'test-postgres2') + '%s'
    postgres_instance_name_query = postgres_instance_name + QUERY_SUFFIX
    instances = [
        ('GCSQL_MYSQL_INSTANCE_NAME', mysql_instance_name),
        ('GCSQL_MYSQL_INSTANCE_NAME2', mysql_instance_name2),
        ('GCSQL_MYSQL_INSTANCE_NAME_QUERY', mysql_instance_name_query),
        ('GCSQL_POSTGRES_INSTANCE_NAME', postgres_instance_name),
        ('GCSQL_POSTGRES_INSTANCE_NAME2', postgres_instance_name2),
        ('GCSQL_POSTGRES_INSTANCE_NAME_QUERY', postgres_instance_name_query),
    ]
    return instances


@pytest.fixture(scope='class', autouse=True)
def set_unique_postfix(env_patch, default_instances):
    """
    Generate a unique postfix and add it to an instance name to avoid 409 HTTP error
    in case of the instance name was already used during last week
    """
    unique_postfix = f'-{uuid.uuid4().hex[:5]}'  # generate 5 digits unique postfix
    for instance in default_instances:
        env_variable, value = instance
        env_patch.setenv(env_variable, value % unique_postfix)


@pytest.fixture
def set_mysql_ip(monkeypatch):
    """Set ip address of MySQL instance to GCSQL_MYSQL_PUBLIC_IP env variable"""
    with open(os.environ["GCSQL_MYSQL_PUBLIC_IP_FILE"]) as file:
        env, ip_address = file.read().split("=")
        monkeypatch.setenv(env, ip_address)


@pytest.fixture
def set_postgres_ip(monkeypatch):
    """Set ip address of Postgres instance to GCSQL_POSTGRES_PUBLIC_IP env variable"""
    with open(os.environ["GCSQL_POSTGRES_PUBLIC_IP_FILE"]) as file:
        env, ip_address = file.read().split("=")
        monkeypatch.setenv(env, ip_address)


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.credential_file(GCP_CLOUDSQL_KEY)
class CloudSqlExampleDagsIntegrationTest(GoogleSystemTest):
    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def setUp(self):
        super().setUp()
        # 1. Create Fine-grained bucket to provide ACLs
        self.log.info(f'Creating {CLOUD_SQL_BUCKET_NAME} bucket...')
        self.execute_cmd(["gsutil", "mb", "-b", "off", f"gs://{CLOUD_SQL_BUCKET_NAME}"])

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    def tearDown(self):
        if os.path.exists(TEARDOWN_LOCK_FILE):
            self.log.info("Skip deleting instances as they were created manually (helps to iterate on tests)")
        else:
            # 1. Delete instances just in case the test failed and did not cleanup after itself
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-failover-replica")
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="-read-replica")
            SQL_QUERY_TEST_HELPER.delete_instances()
            SQL_QUERY_TEST_HELPER.delete_instances(instance_suffix="2")
            SQL_QUERY_TEST_HELPER.delete_service_account_acls()

            # 2. Delete bucket
            self.log.info(f'Deleting {CLOUD_SQL_BUCKET_NAME} bucket...')
            self.execute_cmd(["gsutil", "rm", "-r", f"gs://{CLOUD_SQL_BUCKET_NAME}"])
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

    def setUp(self):
        super().setUp()

    def tearDown(self):
        super().tearDown()

    @staticmethod
    def generate_unique_path():
        return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(8))

    def test_start_proxy_fail_no_parameters(self):
        runner = CloudSqlProxyRunner(
            path_prefix='/tmp/' + self.generate_unique_path(),
            project_id=GCP_PROJECT_ID,
            instance_specification='a',
        )
        with pytest.raises(AirflowException) as ctx:
            runner.start_proxy()
        err = ctx.value
        assert "The cloud_sql_proxy finished early" in str(err)
        with pytest.raises(AirflowException) as ctx:
            runner.start_proxy()
        err = ctx.value
        assert "The cloud_sql_proxy finished early" in str(err)
        assert runner.sql_proxy_process is None

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
        assert runner.sql_proxy_process is None

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
        assert runner.sql_proxy_process is None

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
        assert runner.sql_proxy_process is None
        assert runner.get_proxy_version() == "1.13"

    @provide_gcp_context(GCP_CLOUDSQL_KEY)
    @pytest.mark.usefixtures('set_mysql_ip', 'set_postgres_ip')
    def test_run_example_dag_cloudsql_query(self):
        self.run_dag('example_gcp_sql_query', CLOUD_DAG_FOLDER)
