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
import unittest

import mock
from parameterized import parameterized

from airflow import AirflowException
from airflow.gcp.operators.spanner import (
    SpannerDeleteDatabaseInstanceOperator, SpannerDeleteInstanceOperator,
    SpannerDeployDatabaseInstanceOperator, SpannerDeployInstanceOperator,
    SpannerQueryDatabaseInstanceOperator, SpannerUpdateDatabaseInstanceOperator,
)

PROJECT_ID = 'project-id'
INSTANCE_ID = 'instance-id'
DB_ID = 'db1'
CONFIG_NAME = 'projects/project-id/instanceConfigs/eur3'
NODE_COUNT = '1'
DISPLAY_NAME = 'Test Instance'
INSERT_QUERY = "INSERT my_table1 (id, name) VALUES (1, 'One')"
INSERT_QUERY_2 = "INSERT my_table2 (id, name) VALUES (1, 'One')"
CREATE_QUERY = "CREATE TABLE my_table1 (id INT64, name STRING(100))"
CREATE_QUERY_2 = "CREATE TABLE my_table2 (id INT64, name STRING(100))"
DDL_STATEMENTS = [CREATE_QUERY, CREATE_QUERY_2]


class TestCloudSpanner(unittest.TestCase):
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_create(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME
        )
        mock_hook.return_value.update_instance.assert_not_called()
        self.assertIsNone(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_create_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeployInstanceOperator(
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME
        )
        mock_hook.return_value.update_instance.assert_not_called()
        self.assertIsNone(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_update(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME
        )
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertIsNone(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_update_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.update_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME
        )
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertIsNone(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_create_aborts_and_succeeds_if_instance_exists(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeployInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            configuration_name=CONFIG_NAME,
            node_count=int(NODE_COUNT),
            display_name=DISPLAY_NAME,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_instance.assert_not_called()
        self.assertIsNone(result)

    @parameterized.expand([
        ("", INSTANCE_ID, "project_id"),
        (PROJECT_ID, "", "instance_id"),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_create_ex_if_param_missing(self, project_id, instance_id,
                                                 exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerDeployInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                configuration_name=CONFIG_NAME,
                node_count=int(NODE_COUNT),
                display_name=DISPLAY_NAME,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_delete(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeleteInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID
        )
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_delete_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {"name": INSTANCE_ID}
        op = SpannerDeleteInstanceOperator(
            instance_id=INSTANCE_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_called_once_with(
            project_id=None,
            instance_id=INSTANCE_ID
        )
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_delete_aborts_and_succeeds_if_instance_does_not_exist(self,
                                                                            mock_hook):
        mock_hook.return_value.get_instance.return_value = None
        op = SpannerDeleteInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_instance.assert_not_called()
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, "project_id"),
        (PROJECT_ID, "", "instance_id"),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_delete_ex_if_param_missing(self, project_id, instance_id, exp_msg,
                                                 mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerDeleteInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_query(self, mock_hook):
        mock_hook.return_value.execute_sql.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=INSERT_QUERY,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID,
            database_id=DB_ID,
            queries=[INSERT_QUERY]
        )
        self.assertIsNone(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_query_missing_project_id(self, mock_hook):
        mock_hook.return_value.execute_sql.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=INSERT_QUERY,
            task_id="id"
        )
        result = op.execute(None)  # pylint: disable=assignment-from-no-return
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID,
            database_id=DB_ID, queries=[INSERT_QUERY]
        )
        self.assertIsNone(result)

    @parameterized.expand([
        ("", INSTANCE_ID, DB_ID, INSERT_QUERY, "project_id"),
        (PROJECT_ID, "", DB_ID, INSERT_QUERY, "instance_id"),
        (PROJECT_ID, INSTANCE_ID, "", INSERT_QUERY, "database_id"),
        (PROJECT_ID, INSTANCE_ID, DB_ID, "", "query"),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_query_ex_if_param_missing(self, project_id, instance_id,
                                                database_id, query, exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerQueryDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                query=query,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_query_dml(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=INSERT_QUERY,
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID, queries=[INSERT_QUERY]
        )

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_instance_query_dml_list(self, mock_hook):
        mock_hook.return_value.execute_dml.return_value = None
        op = SpannerQueryDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            query=[INSERT_QUERY, INSERT_QUERY_2],
            task_id="id"
        )
        op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.execute_dml.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID,
            database_id=DB_ID, queries=[INSERT_QUERY, INSERT_QUERY_2]
        )

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_create(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeployDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS
        )
        mock_hook.return_value.update_database.assert_not_called()
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_create_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeployDatabaseInstanceOperator(
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_database.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS
        )
        mock_hook.return_value.update_database.assert_not_called()
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_create_with_pre_existing_db(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeployDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.create_database.assert_not_called()
        mock_hook.return_value.update_database.assert_not_called()
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, 'project_id'),
        (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, 'instance_id'),
        (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, 'database_id'),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_create_ex_if_param_missing(self,
                                                 project_id, instance_id,
                                                 database_id, ddl_statements,
                                                 exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerDeployDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_update(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerUpdateDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.update_database.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS, operation_id=None
        )
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_update_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerUpdateDatabaseInstanceOperator(
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.update_database.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID,
            ddl_statements=DDL_STATEMENTS, operation_id=None
        )
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, 'project_id'),
        (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, 'instance_id'),
        (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, 'database_id'),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_update_ex_if_param_missing(self, project_id, instance_id,
                                                 database_id, ddl_statements,
                                                 exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerUpdateDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_update_ex_if_database_not_exist(self, mock_hook):
        mock_hook.return_value.get_database.return_value = None
        with self.assertRaises(AirflowException) as cm:
            op = SpannerUpdateDatabaseInstanceOperator(
                project_id=PROJECT_ID,
                instance_id=INSTANCE_ID,
                database_id=DB_ID,
                ddl_statements=DDL_STATEMENTS,
                task_id="id"
            )
            op.execute(None)
        err = cm.exception
        self.assertIn("The Cloud Spanner database 'db1' in project 'project-id' and "
                      "instance 'instance-id' is missing", str(err))
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_delete(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeleteDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=PROJECT_ID, instance_id=INSTANCE_ID, database_id=DB_ID
        )
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_delete_missing_project_id(self, mock_hook):
        mock_hook.return_value.get_database.return_value = {"name": DB_ID}
        op = SpannerDeleteDatabaseInstanceOperator(
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_database.assert_called_once_with(
            project_id=None, instance_id=INSTANCE_ID, database_id=DB_ID
        )
        self.assertTrue(result)

    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_delete_exits_and_succeeds_if_database_does_not_exist(self,
                                                                           mock_hook):
        mock_hook.return_value.get_database.return_value = None
        op = SpannerDeleteDatabaseInstanceOperator(
            project_id=PROJECT_ID,
            instance_id=INSTANCE_ID,
            database_id=DB_ID,
            task_id="id"
        )
        result = op.execute(None)
        mock_hook.assert_called_once_with(gcp_conn_id="google_cloud_default")
        mock_hook.return_value.delete_database.assert_not_called()
        self.assertTrue(result)

    @parameterized.expand([
        ("", INSTANCE_ID, DB_ID, DDL_STATEMENTS, 'project_id'),
        (PROJECT_ID, "", DB_ID, DDL_STATEMENTS, 'instance_id'),
        (PROJECT_ID, INSTANCE_ID, "", DDL_STATEMENTS, 'database_id'),
    ])
    @mock.patch("airflow.gcp.operators.spanner.SpannerHook")
    def test_database_delete_ex_if_param_missing(self, project_id, instance_id,
                                                 database_id, ddl_statements,
                                                 exp_msg, mock_hook):
        with self.assertRaises(AirflowException) as cm:
            SpannerDeleteDatabaseInstanceOperator(
                project_id=project_id,
                instance_id=instance_id,
                database_id=database_id,
                ddl_statements=ddl_statements,
                task_id="id"
            )
        err = cm.exception
        self.assertIn("The required parameter '{}' is empty".format(exp_msg), str(err))
        mock_hook.assert_not_called()
