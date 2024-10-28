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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.databricks.operators.databricks_repos import (
    DatabricksReposCreateOperator,
    DatabricksReposDeleteOperator,
    DatabricksReposUpdateOperator,
)

TASK_ID = "databricks-operator"
DEFAULT_CONN_ID = "databricks_default"


class TestDatabricksReposUpdateOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_update_with_id(self, db_mock_class):
        """
        Test the execute function using Repo ID.
        """
        op = DatabricksReposUpdateOperator(
            task_id=TASK_ID, branch="releases", repo_id="123"
        )
        db_mock = db_mock_class.return_value
        db_mock.update_repo.return_value = {"head_commit_id": "123456"}

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposUpdateOperator",
        )

        db_mock.update_repo.assert_called_once_with("123", {"branch": "releases"})

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_update_with_path(self, db_mock_class):
        """
        Test the execute function using Repo path.
        """
        op = DatabricksReposUpdateOperator(
            task_id=TASK_ID, tag="v1.0.0", repo_path="/Repos/user@domain.com/test-repo"
        )
        db_mock = db_mock_class.return_value
        db_mock.get_repo_by_path.return_value = "123"
        db_mock.update_repo.return_value = {"head_commit_id": "123456"}

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposUpdateOperator",
        )

        db_mock.update_repo.assert_called_once_with("123", {"tag": "v1.0.0"})

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        with pytest.raises(
            AirflowException,
            match="Only one of repo_id or repo_path should be provided, but not both",
        ):
            DatabricksReposUpdateOperator(
                task_id=TASK_ID, repo_id="abc", repo_path="path", branch="abc"
            )

        with pytest.raises(
            AirflowException, match="One of repo_id or repo_path should be provided"
        ):
            DatabricksReposUpdateOperator(task_id=TASK_ID, branch="abc")

        with pytest.raises(
            AirflowException,
            match="Only one of branch or tag should be provided, but not both",
        ):
            DatabricksReposUpdateOperator(
                task_id=TASK_ID, repo_id="123", branch="123", tag="123"
            )

        with pytest.raises(
            AirflowException, match="One of branch or tag should be provided"
        ):
            DatabricksReposUpdateOperator(task_id=TASK_ID, repo_id="123")


class TestDatabricksReposDeleteOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_delete_with_id(self, db_mock_class):
        """
        Test the execute function using Repo ID.
        """
        op = DatabricksReposDeleteOperator(task_id=TASK_ID, repo_id="123")
        db_mock = db_mock_class.return_value
        db_mock.delete_repo.return_value = None

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposDeleteOperator",
        )

        db_mock.delete_repo.assert_called_once_with("123")

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_delete_with_path(self, db_mock_class):
        """
        Test the execute function using Repo path.
        """
        op = DatabricksReposDeleteOperator(
            task_id=TASK_ID, repo_path="/Repos/user@domain.com/test-repo"
        )
        db_mock = db_mock_class.return_value
        db_mock.get_repo_by_path.return_value = "123"
        db_mock.delete_repo.return_value = None

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposDeleteOperator",
        )

        db_mock.delete_repo.assert_called_once_with("123")

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        with pytest.raises(
            AirflowException,
            match="Only one of repo_id or repo_path should be provided, but not both",
        ):
            DatabricksReposDeleteOperator(
                task_id=TASK_ID, repo_id="abc", repo_path="path"
            )

        with pytest.raises(
            AirflowException, match="One of repo_id repo_path tag should be provided"
        ):
            DatabricksReposDeleteOperator(task_id=TASK_ID)


class TestDatabricksReposCreateOperator:
    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_create_plus_checkout(self, db_mock_class):
        """
        Test the execute function creating new Repo.
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/Project1/test-repo"
        op = DatabricksReposCreateOperator(
            task_id=TASK_ID, git_url=git_url, repo_path=repo_path, branch="releases"
        )
        db_mock = db_mock_class.return_value
        db_mock.update_repo.return_value = {"head_commit_id": "123456"}
        db_mock.create_repo.return_value = {"id": "123", "branch": "main"}
        db_mock.get_repo_by_path.return_value = None

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposCreateOperator",
        )

        db_mock.create_repo.assert_called_once_with(
            {"url": git_url, "provider": "gitHub", "path": repo_path}
        )
        db_mock.update_repo.assert_called_once_with("123", {"branch": "releases"})

    @mock.patch("airflow.providers.databricks.operators.databricks_repos.DatabricksHook")
    def test_create_ignore_existing_plus_checkout(self, db_mock_class):
        """
        Test the execute function creating new Repo.
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/Project1/test-repo"
        op = DatabricksReposCreateOperator(
            task_id=TASK_ID,
            git_url=git_url,
            repo_path=repo_path,
            branch="releases",
            ignore_existing_repo=True,
        )
        db_mock = db_mock_class.return_value
        db_mock.update_repo.return_value = {"head_commit_id": "123456"}
        db_mock.get_repo_by_path.return_value = "123"

        op.execute(None)

        db_mock_class.assert_called_once_with(
            DEFAULT_CONN_ID,
            retry_limit=op.databricks_retry_limit,
            retry_delay=op.databricks_retry_delay,
            caller="DatabricksReposCreateOperator",
        )

        db_mock.get_repo_by_path.assert_called_once_with(repo_path)
        db_mock.update_repo.assert_called_once_with("123", {"branch": "releases"})

    def test_init_exception(self):
        """
        Tests handling of incorrect parameters passed to ``__init__``
        """
        git_url = "https://github.com/test/test"
        repo_path = "/Repos/test-repo"
        exception_message = f"repo_path should have form of /Repos/{{folder}}/{{repo-name}}, got '{repo_path}'"

        op = DatabricksReposCreateOperator(
            task_id=TASK_ID, git_url=git_url, repo_path=repo_path
        )
        with pytest.raises(AirflowException, match=exception_message):
            op.execute(None)

        with pytest.raises(
            AirflowException,
            match="Only one of branch or tag should be provided, but not both",
        ):
            DatabricksReposCreateOperator(
                task_id=TASK_ID, git_url=git_url, branch="123", tag="123"
            )
