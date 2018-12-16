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

import time

from uuid import uuid1

from airflow import AirflowException
from airflow.contrib.hooks.gcp_sql_hook import CloudSqlProxyRunner
from tests.contrib.operators.test_gcp_base import BaseGcpIntegrationTestCase, \
    GCP_CLOUDSQL_KEY
from tests.contrib.operators.test_gcp_sql_query_operator_helper import \
    CloudSqlQueryTestHelper

try:
    # noinspection PyProtectedMember
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')
INSTANCE_NAME = os.environ.get('INSTANCE_NAME', 'test-name')
DB_NAME = os.environ.get('DB_NAME', 'db1')


SKIP_CLOUDSQL_QUERY_WARNING = """
    This test is skipped from automated runs intentionally
    as creating databases in Google Cloud SQL takes a very
    long time. You can still set GCP_ENABLE_CLOUDSQL_QUERY_TEST 
    environment variable to 'True' and then you should be able to
    run it manually after you create the database
    Creating the database can be done by running this python
    file as python program with --action=create flag
    (you should remember to delete the database with --action=delete flag)
"""
GCP_ENABLE_CLOUDSQL_QUERY_TEST = os.environ.get('GCP_ENABLE_CLOUDSQL_QUERY_TEST')

if GCP_ENABLE_CLOUDSQL_QUERY_TEST == 'True':
    skip_cloudsql_query_test = False
else:
    skip_cloudsql_query_test = True


@unittest.skipIf(skip_cloudsql_query_test, SKIP_CLOUDSQL_QUERY_WARNING)
class CloudSqlProxyIntegrationTest(BaseGcpIntegrationTestCase):
    def __init__(self, method_name='runTest'):
        super(CloudSqlProxyIntegrationTest, self).__init__(
            method_name,
            dag_id='example_gcp_sql_query',
            gcp_key='gcp_cloudsql.json')

    def test_start_proxy_fail_no_parameters(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + str(uuid1()),
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
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + str(uuid1()),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='')
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances_generated_credential_file(self):
        self.update_connection_with_dictionary()
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + str(uuid1()),
                                     project_id=GCP_PROJECT_ID,
                                     instance_specification='')
        try:
            runner.start_proxy()
            time.sleep(1)
        finally:
            runner.stop_proxy()
        self.assertIsNone(runner.sql_proxy_process)

    def test_start_proxy_with_all_instances_specific_version(self):
        runner = CloudSqlProxyRunner(path_prefix='/tmp/' + str(uuid1()),
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


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'example-project')
GCP_LOCATION = os.environ.get('GCP_LOCATION', 'europe-west1')

ITEST_POSTGRES_INSTANCE_NAME = os.environ.get('GCSQL_POSTGRES_INSTANCE_NAME',
                                              'testpostgres')
ITEST_MYSQL_INSTANCE_NAME = os.environ.get('GCSQL_MYSQL_INSTANCE_NAME',
                                           'testmysql')
GCSQL_POSTGRES_SERVER_CA_FILE = os.environ.get('GCSQL_POSTGRES_SERVER_CA_FILE',
                                               ".key/postgres-server-ca.pem")
GCSQL_POSTGRES_CLIENT_CERT_FILE = os.environ.get('GCSQL_POSTGRES_CLIENT_CERT_FILE',
                                                 ".key/postgres-client-cert.pem")
GCSQL_POSTGRES_CLIENT_KEY_FILE = os.environ.get('GCSQL_POSTGRES_CLIENT_KEY_FILE',
                                                ".key/postgres-client-key.pem")
GCSQL_POSTGRES_PUBLIC_IP_FILE = os.environ.get('GCSQL_POSTGRES_PUBLIC_IP_FILE',
                                               ".key/postgres-ip.env")
GCSQL_POSTGRES_USER = os.environ.get('GCSQL_POSTGRES_USER', 'postgres_user')
GCSQL_POSTGRES_DATABASE_NAME = os.environ.get('GCSQL_POSTGRES_DATABASE_NAME',
                                              'postgresdb')
GCSQL_MYSQL_CLIENT_CERT_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_CERT_FILE',
                                              ".key/mysql-client-cert.pem")
GCSQL_MYSQL_CLIENT_KEY_FILE = os.environ.get('GCSQL_MYSQL_CLIENT_KEY_FILE',
                                             ".key/mysql-client-key.pem")
GCSQL_MYSQL_SERVER_CA_FILE = os.environ.get('GCSQL_MYSQL_SERVER_CA_FILE',
                                            ".key/mysql-server-ca.pem")
GCSQL_MYSQL_PUBLIC_IP_FILE = os.environ.get('GCSQL_MYSQL_PUBLIC_IP_FILE',
                                            ".key/mysql-ip.env")
GCSQL_MYSQL_USER = os.environ.get('GCSQL_MYSQL_USER', 'mysql_user')
GCSQL_MYSQL_DATABASE_NAME = os.environ.get('GCSQL_MYSQL_DATABASE_NAME', 'mysqldb')
DB_VERSION_MYSQL = 'MYSQL_5_7'
DV_VERSION_POSTGRES = 'POSTGRES_9_6'


@unittest.skipIf(skip_cloudsql_query_test, SKIP_CLOUDSQL_QUERY_WARNING)
class CloudSqlQueryExampleDagsIntegrationTest(BaseGcpIntegrationTestCase):

    def __init__(self, method_name='runTest'):
        self.helper = CloudSqlQueryTestHelper()
        self.helper.set_ip_addresses_in_env()
        super(CloudSqlQueryExampleDagsIntegrationTest, self).__init__(
            method_name,
            dag_id='example_gcp_sql_query',
            gcp_key=GCP_CLOUDSQL_KEY)

    def test_run_example_dag_cloudsql_query(self):
        self._run_dag()
