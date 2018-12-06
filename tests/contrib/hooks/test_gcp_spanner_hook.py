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
#

import unittest

from airflow.contrib.hooks.gcp_spanner_hook import SpannerHook

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

PROJECT_ID = "test-project-id"
INSTANCE_ID = "test-instance_id"
DATABASE_ID = "test-database-id"
SESSION_ID = "projects/{}/instances/{}/databases/{}/sessions"\
    "/CO8uNmWtr-d4qC2ySsRzUaX1Foi2oAwkgi65e_pejrg0N1N0j3hdpnwft6Jk".format(
        PROJECT_ID, INSTANCE_ID, DATABASE_ID)
BASE_STRING = "airflow.contrib.hooks.gcp_api_base_hook.{}"
SPANNER_STRING = "airflow.contrib.hooks.gcp_spanner_hook.{}"
SPANNER_HOOK_CONN_STRING = SPANNER_STRING.format("SpannerHook.get_conn")


def mock_init(self, gcp_conn_id, delegate_to=None, version=None):
    pass


class SpannerHookTest(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleCloudBaseHook.__init__"), new=mock_init
        ):
            self.spanner_hook = SpannerHook()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_list_instance_configs(self, mock_service):
        self.spanner_hook.list_instance_configs(PROJECT_ID)

        method = (
            mock_service.return_value.projects.return_value.instanceConfigs.return_value.list
        )
        method.assert_called_with(parent="projects/{}".format(PROJECT_ID))
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_create_instance(self, mock_service):
        instance_body = {
            "instanceId": INSTANCE_ID,
            "instance": {
                "nodeCount": 1,
                "config": "projects/{}/instanceConfigs/eur3".format(PROJECT_ID),
                "displayName": "Test Instance 1",
            },
        }
        self.spanner_hook.create_instance(PROJECT_ID, instance_body)

        method = (
            mock_service.return_value.projects.return_value.instances.return_value.create
        )
        method.assert_called_with(
            parent="projects/{}".format(PROJECT_ID), body=instance_body
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_create_instance_without_instance_id(self, mock_service):
        instance_body = {
            "instance": {
                "nodeCount": 1,
                "config": "projects/{}/instanceConfigs/eur3".format(PROJECT_ID),
                "displayName": "Test Instance 1",
            }
        }
        with self.assertRaises(ValueError):
            self.spanner_hook.create_instance(PROJECT_ID, instance_body)

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_get_instance(self, mock_service):
        self.spanner_hook.get_instance(PROJECT_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value.instances.return_value.get
        )
        method.assert_called_with(
            name="projects/{}/instances/{}".format(PROJECT_ID, INSTANCE_ID)
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_delete_instance(self, mock_service):
        self.spanner_hook.delete_instance(PROJECT_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value.instances.return_value.delete
        )
        method.assert_called_with(
            name="projects/{}/instances/{}".format(PROJECT_ID, INSTANCE_ID)
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_list_instances(self, mock_service):
        self.spanner_hook.list_instances(PROJECT_ID)

        method = (
            mock_service.return_value.projects.return_value.instances.return_value.list
        )
        method.assert_called_with(parent="projects/{}".format(PROJECT_ID))
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_create_database(self, mock_service):
        database_body = {"createStatement": "CREATE DATABASE test_db"}
        self.spanner_hook.create_database(PROJECT_ID, INSTANCE_ID, database_body)

        method = (
            mock_service.return_value.projects.return_value.instances
            .return_value.databases.return_value.create
        )
        method.assert_called_with(
            parent="projects/{}/instances/{}".format(PROJECT_ID, INSTANCE_ID),
            body=database_body,
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_create_database_without_create_statement(self, mock_service):
        database_body = {}
        with self.assertRaises(ValueError):
            self.spanner_hook.create_database(PROJECT_ID, INSTANCE_ID, database_body)

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_get_database(self, mock_service):
        self.spanner_hook.get_database(PROJECT_ID, INSTANCE_ID, DATABASE_ID)

        method = (
            mock_service.return_value.projects.return_value
            .instances.return_value.databases.return_value.get
        )
        method.assert_called_with(
            name="projects/{}/instances/{}/databases/{}".format(
                PROJECT_ID, INSTANCE_ID, DATABASE_ID
            )
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_drop_database(self, mock_service):
        self.spanner_hook.drop_database(PROJECT_ID, INSTANCE_ID, DATABASE_ID)

        method = (
            mock_service.return_value.projects.return_value
            .instances.return_value.databases.return_value.dropDatabase
        )
        method.assert_called_with(
            database="projects/{}/instances/{}/databases/{}".format(
                PROJECT_ID, INSTANCE_ID, DATABASE_ID
            )
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_list_databases(self, mock_service):
        self.spanner_hook.list_databases(PROJECT_ID, INSTANCE_ID)

        method = (
            mock_service.return_value.projects.return_value
            .instances.return_value.databases.return_value.list
        )
        method.assert_called_with(
            parent="projects/{}/instances/{}".format(PROJECT_ID, INSTANCE_ID)
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_create_session(self, mock_service):
        self.spanner_hook.create_session(PROJECT_ID, INSTANCE_ID, DATABASE_ID)

        method = (
            mock_service.return_value.projects.return_value
            .instances.return_value.databases.return_value.sessions.return_value.create
        )
        method.assert_called_with(
            database="projects/{}/instances/{}/databases/{}".format(
                PROJECT_ID, INSTANCE_ID, DATABASE_ID
            ),
            body={"session": {}},
        )
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_delete_session(self, mock_service):
        self.spanner_hook.delete_session(SESSION_ID)

        method = (
            mock_service.return_value.projects.return_value
            .instances.return_value.databases.return_value.sessions.return_value.delete
        )
        method.assert_called_with(name=SESSION_ID)
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_execute_sql(self, mock_service):
        sql_query_body = {"sql": "SELECT * FROM Persons"}

        self.spanner_hook.execute_sql(SESSION_ID, sql_query_body)

        method = (
            mock_service.return_value.projects.return_value.instances.return_value
            .databases.return_value.sessions.return_value.executeSql
        )
        method.assert_called_with(session=SESSION_ID, body=sql_query_body)
        method.return_value.execute.assert_called_with()

    @mock.patch(SPANNER_HOOK_CONN_STRING)
    def test_execute_sql_without_sql_statement(self, mock_service):
        sql_query_body = {}

        with self.assertRaises(ValueError):
            self.spanner_hook.execute_sql(SESSION_ID, sql_query_body)
