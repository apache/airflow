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
from unittest import mock

from airflow.providers.databricks.operators.databricks_repos import DatabricksReposUpdateOperator

TASK_ID = 'databricks-operator'
DEFAULT_CONN_ID = 'databricks_default'


class TestDatabricksReposUpdateOperator(unittest.TestCase):
    @mock.patch('airflow.providers.databricks.operators.databricks_repos.DatabricksHook')
    def test_update_with_id(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        op = DatabricksReposUpdateOperator(task_id=TASK_ID, branch="releases", repo_id="123")
        db_mock = db_mock_class.return_value
        db_mock.update_repo.return_value = {'head_commit_id': '123456'}

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID, retry_limit=op.databricks_retry_limit, retry_delay=op.databricks_retry_delay
        )

        db_mock.update_repo.assert_called_once_with('123', {'branch': 'releases'})

    @mock.patch('airflow.providers.databricks.operators.databricks_repos.DatabricksHook')
    def test_update_with_path(self, db_mock_class):
        """
        Test the execute function in case where the run is successful.
        """
        op = DatabricksReposUpdateOperator(
            task_id=TASK_ID, tag="v1.0.0", repo_path="/Repos/user@domain.com/test-repo"
        )
        db_mock = db_mock_class.return_value
        db_mock.get_repo_by_path.return_value = '123'
        db_mock.update_repo.return_value = {'head_commit_id': '123456'}

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID, retry_limit=op.databricks_retry_limit, retry_delay=op.databricks_retry_delay
        )

        db_mock.update_repo.assert_called_once_with('123', {'tag': 'v1.0.0'})
