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
from os.path import dirname
import random
import string
import unittest

import time

from airflow import AirflowException
from airflow.contrib.hooks.gcp_sql_hook import CloudSqlProxyRunner
from tests.contrib.utils.base_gcp_system_test_case import BaseGcpSystemTestCase, \
    DagGcpSystemTestCase
from tests.contrib.operators.test_gcp_sql_operator_system_helper import \
    CloudSqlQueryTestHelper
from tests.contrib.utils.gcp_authenticator import GCP_CLOUDSQL_KEY

GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-id')

SKIP_CLOUDSQL_QUERY_WARNING = """
    This test is skipped from automated runs intentionally
    as creating databases in Google Cloud SQL takes a very
    long time. You can still set GCP_ENABLE_CLOUDSQL_QUERY_TEST
    environment variable to 'True' and then you should be able to
    run it manually after you create the database
    Creating the database can be done by running
    `{}/test_gcp_sql_operatorquery_system_helper.py \
--action=before-tests`
    (you should remember to delete the database with --action=after-tests flag)
""".format(dirname(__file__))

GCP_ENABLE_CLOUDSQL_QUERY_TEST = os.environ.get('GCP_ENABLE_CLOUDSQL_QUERY_TEST')

if GCP_ENABLE_CLOUDSQL_QUERY_TEST == 'True':
    enable_cloudsql_query_test = True
else:
    enable_cloudsql_query_test = False


SQL_QUERY_TEST_HELPER = CloudSqlQueryTestHelper()


@unittest.skipIf(not enable_cloudsql_query_test, SKIP_CLOUDSQL_QUERY_WARNING)
class CloudSqlProxySystemTest(BaseGcpSystemTestCase):
    def __init__(self, method_name='runTest'):
        super().__init__(
            method_name,
            gcp_key='gcp_cloudsql.json')

    def setUp(self):
        super().setUp()
        self.gcp_authenticator.gcp_authenticate()
        SQL_QUERY_TEST_HELPER.check_if_instances_are_up(instance_suffix="_QUERY")
        self.gcp_authenticator.gcp_revoke_authentication()

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

    def test_start_proxy_with_all_instances_generated_credential_file(self):
        self.gcp_authenticator.set_dictionary_in_airflow_connection()
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


@unittest.skipIf(not enable_cloudsql_query_test, SKIP_CLOUDSQL_QUERY_WARNING)
class CloudSqlQueryExampleDagsSystemTest(DagGcpSystemTestCase):

    def __init__(self, method_name='runTest'):
        super().__init__(
            method_name,
            dag_id='example_gcp_sql_query',
            gcp_key=GCP_CLOUDSQL_KEY)

    def setUp(self):
        super().setUp()
        self.gcp_authenticator.gcp_authenticate()
        SQL_QUERY_TEST_HELPER.check_if_instances_are_up(instance_suffix="_QUERY")
        SQL_QUERY_TEST_HELPER.setup_instances(instance_suffix="_QUERY")
        self.gcp_authenticator.gcp_revoke_authentication()

    def test_run_example_dag_cloudsql_query(self):
        self._run_dag()
