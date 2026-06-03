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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.tableau.hooks.tableau import TableauJobFinishCode
from airflow.providers.tableau.operators.tableau import TableauOperator


class TestTableauOperator:
    """
    Test class for TableauOperator
    """

    def setup_method(self):
        self.mocked_workbooks = []
        self.mock_datasources = []
        self.mock_tasks = []

        for i in range(3):
            mock_workbook = Mock()
            mock_workbook.id = i
            mock_workbook.name = f"wb_{i}"
            self.mocked_workbooks.append(mock_workbook)

            mock_datasource = Mock()
            mock_datasource.id = i
            mock_datasource.name = f"ds_{i}"
            self.mock_datasources.append(mock_datasource)

            mock_task = Mock()
            mock_task.id = f"task-id-{i}"
            mock_task.name = f"task_{i}"
            self.mock_tasks.append(mock_task)

        self.kwargs = {
            "site_id": "test_site",
            "task_id": "task",
            "dag": None,
            "match_with": "name",
            "method": "refresh",
        }

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_workbooks(self, mock_tableau_hook):
        """
        Test Execute Workbooks
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(blocking_refresh=False, find="wb_2", resource="workbooks", **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_workbooks_blocking(self, mock_tableau_hook):
        """
        Test execute workbooks blocking
        """
        mock_signed_in = [False]

        def mock_hook_enter():
            mock_signed_in[0] = True
            return mock_tableau_hook

        def mock_hook_exit(exc_type, exc_val, exc_tb):
            mock_signed_in[0] = False

        def mock_wait_for_state(job_id, target_state, check_interval):
            if not mock_signed_in[0]:
                raise Exception("Not signed in")

            return True

        mock_tableau_hook.return_value.__enter__ = Mock(side_effect=mock_hook_enter)
        mock_tableau_hook.return_value.__exit__ = Mock(side_effect=mock_hook_exit)
        mock_tableau_hook.wait_for_state = Mock(side_effect=mock_wait_for_state)

        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.server.jobs.get_by_id = Mock(
            return_value=Mock(finish_code=TableauJobFinishCode.SUCCESS.value)
        )

        operator = TableauOperator(find="wb_2", resource="workbooks", **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id
        mock_tableau_hook.wait_for_state.assert_called_once_with(
            job_id=job_id, check_interval=20, target_state=TableauJobFinishCode.SUCCESS
        )

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_missing_workbook(self, mock_tableau_hook):
        """
        Test execute missing workbook
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find="test", resource="workbooks", **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_datasources(self, mock_tableau_hook):
        """
        Test Execute datasources
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(blocking_refresh=False, find="ds_2", resource="datasources", **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_datasources_blocking(self, mock_tableau_hook):
        """
        Test execute datasources blocking
        """
        mock_signed_in = [False]

        def mock_hook_enter():
            mock_signed_in[0] = True
            return mock_tableau_hook

        def mock_hook_exit(exc_type, exc_val, exc_tb):
            mock_signed_in[0] = False

        def mock_wait_for_state(job_id, target_state, check_interval):
            if not mock_signed_in[0]:
                raise Exception("Not signed in")

            return True

        mock_tableau_hook.return_value.__enter__ = Mock(side_effect=mock_hook_enter)
        mock_tableau_hook.return_value.__exit__ = Mock(side_effect=mock_hook_exit)
        mock_tableau_hook.wait_for_state = Mock(side_effect=mock_wait_for_state)

        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        operator = TableauOperator(find="ds_2", resource="datasources", **self.kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id
        mock_tableau_hook.wait_for_state.assert_called_once_with(
            job_id=job_id, check_interval=20, target_state=TableauJobFinishCode.SUCCESS
        )

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_missing_datasource(self, mock_tableau_hook):
        """
        Test execute missing datasource
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(find="test", resource="datasources", **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    @patch("airflow.providers.tableau.operators.tableau.JobItem")
    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_tasks_run_with_id(self, mock_tableau_hook, mock_job_item):
        """tasks.run must fetch a TaskItem via get_by_id and parse JobItem from response bytes."""
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)

        mock_task_item = Mock()
        mock_tableau_hook.server.tasks.get_by_id = Mock(return_value=mock_task_item)
        mock_tableau_hook.server.tasks.run = Mock(return_value=b"<tsResponse/>")

        mock_job = Mock()
        mock_job.id = "job-123"
        mock_job_item.from_response = Mock(return_value=[mock_job])

        kwargs = {**self.kwargs, "method": "run", "match_with": "id"}
        operator = TableauOperator(find="task-abc", resource="tasks", **kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.tasks.get_by_id.assert_called_once_with("task-abc")
        mock_tableau_hook.server.tasks.run.assert_called_once_with(mock_task_item)
        mock_job_item.from_response.assert_called_once_with(
            b"<tsResponse/>", mock_tableau_hook.server.namespace
        )
        assert job_id == "job-123"

    @patch("airflow.providers.tableau.operators.tableau.JobItem")
    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_tasks_run_with_match_with_name(self, mock_tableau_hook, mock_job_item):
        """tasks.run should resolve the task id before fetching the TaskItem to run."""
        mock_tableau_hook.get_all = Mock(return_value=self.mock_tasks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)

        mock_task_item = Mock()
        mock_tableau_hook.server.tasks.get_by_id = Mock(return_value=mock_task_item)
        mock_tableau_hook.server.tasks.run = Mock(return_value=b"<tsResponse/>")

        mock_job = Mock()
        mock_job.id = "job-456"
        mock_job_item.from_response = Mock(return_value=[mock_job])

        kwargs = {**self.kwargs, "method": "run"}
        operator = TableauOperator(find="task_2", resource="tasks", **kwargs)

        job_id = operator.execute(context={})

        mock_tableau_hook.server.tasks.get_by_id.assert_called_once_with(self.mock_tasks[2].id)
        mock_tableau_hook.server.tasks.run.assert_called_once_with(mock_task_item)
        assert job_id == "job-456"

    @patch("airflow.providers.tableau.operators.tableau.JobItem")
    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_tasks_run_empty_job_response_raises(self, mock_tableau_hook, mock_job_item):
        """A tasks.run response without a JobItem should raise a provider exception."""
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        mock_tableau_hook.server.tasks.get_by_id = Mock(return_value=Mock())
        mock_tableau_hook.server.tasks.run = Mock(return_value=b"<tsResponse/>")
        mock_job_item.from_response = Mock(return_value=[])

        kwargs = {**self.kwargs, "method": "run", "match_with": "id"}
        operator = TableauOperator(find="task-abc", resource="tasks", **kwargs)

        with pytest.raises(ValueError, match="no JobItem"):
            operator.execute(context={})

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_missing_task(self, mock_tableau_hook):
        """Test execute missing task."""
        mock_tableau_hook.get_all = Mock(return_value=self.mock_tasks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        kwargs = {**self.kwargs, "method": "run"}
        operator = TableauOperator(find="test", resource="tasks", **kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    def test_execute_unavailable_resource(self):
        """
        Test execute unavailable resource
        """
        operator = TableauOperator(resource="test", find="test", **self.kwargs)

        with pytest.raises(AirflowException):
            operator.execute({})

    def test_get_resource_id(self):
        """
        Test get resource id
        """
        resource_id = "res_id"
        operator = TableauOperator(resource="tasks", find=resource_id, method="run", task_id="t", dag=None)
        assert operator._get_resource_id(Mock()) == resource_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_datasources_incremental_refresh(self, mock_tableau_hook):
        """
        Test execute datasources with incremental refresh
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(
            blocking_refresh=False,
            find="ds_2",
            resource="datasources",
            incremental_refresh=True,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2, incremental=True)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_datasources_full_refresh(self, mock_tableau_hook):
        """
        Test execute datasources with full refresh (default behavior)
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(
            blocking_refresh=False,
            find="ds_2",
            resource="datasources",
            incremental_refresh=False,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_workbooks_incremental_refresh(self, mock_tableau_hook):
        """
        Test execute workbooks with incremental refresh
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(
            blocking_refresh=False,
            find="wb_2",
            resource="workbooks",
            incremental_refresh=True,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2, incremental=True)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_workbooks_full_refresh(self, mock_tableau_hook):
        """
        Test execute workbooks with full refresh (default behavior)
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mocked_workbooks)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        operator = TableauOperator(
            blocking_refresh=False,
            find="wb_2",
            resource="workbooks",
            incremental_refresh=False,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        mock_tableau_hook.server.workbooks.refresh.assert_called_once_with(2)
        assert mock_tableau_hook.server.workbooks.refresh.return_value.id == job_id

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_execute_datasources_incremental_refresh_blocking(self, mock_tableau_hook):
        """
        Test execute datasources with incremental refresh blocking
        """
        mock_signed_in = [False]

        def mock_hook_enter():
            mock_signed_in[0] = True
            return mock_tableau_hook

        def mock_hook_exit(exc_type, exc_val, exc_tb):
            mock_signed_in[0] = False

        def mock_wait_for_state(job_id, target_state, check_interval):
            if not mock_signed_in[0]:
                raise Exception("Not signed in")
            return True

        mock_tableau_hook.return_value.__enter__ = Mock(side_effect=mock_hook_enter)
        mock_tableau_hook.return_value.__exit__ = Mock(side_effect=mock_hook_exit)
        mock_tableau_hook.wait_for_state = Mock(side_effect=mock_wait_for_state)
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)

        operator = TableauOperator(
            find="ds_2",
            resource="datasources",
            incremental_refresh=True,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2, incremental=True)
        assert mock_tableau_hook.server.datasources.refresh.return_value.id == job_id
        mock_tableau_hook.wait_for_state.assert_called_once_with(
            job_id=job_id, check_interval=20, target_state=TableauJobFinishCode.SUCCESS
        )

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_incremental_refresh_warning_on_non_refresh_method(self, mock_tableau_hook, caplog):
        """
        Test that a warning is logged when incremental_refresh is set but method is not 'refresh'
        """
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)

        operator = TableauOperator(
            find="ds_2",
            resource="datasources",
            method="delete",
            incremental_refresh=True,
            dag=None,
            task_id="test",
        )

        operator.execute(context={})

        assert "incremental_refresh parameter is set to True but method is 'delete'" in caplog.text
        assert "This parameter only applies to 'refresh' operations" in caplog.text

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_incremental_refresh_unsupported_version_raises(self, mock_tableau_hook):
        """
        Test that AirflowOptionalProviderFeatureException is raised when incremental_refresh=True
        but the installed tableauserverclient does not support the incremental parameter.
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)
        # Simulate older tableauserverclient that doesn't accept incremental kwarg
        mock_tableau_hook.server.datasources.refresh.side_effect = TypeError(
            "refresh() got an unexpected keyword argument 'incremental'"
        )

        operator = TableauOperator(
            blocking_refresh=False,
            find="ds_2",
            resource="datasources",
            incremental_refresh=True,
            **self.kwargs,
        )

        with pytest.raises(AirflowOptionalProviderFeatureException, match="tableauserverclient>=0.35"):
            operator.execute(context={})

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_incremental_refresh_unrelated_type_error_is_raised(self, mock_tableau_hook):
        """
        Test that an unrelated TypeError during refresh is re-raised.
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)

        # Simulate a different TypeError that does NOT contain the string "incremental"
        mock_tableau_hook.server.datasources.refresh.side_effect = TypeError("Some other type error")

        operator = TableauOperator(
            blocking_refresh=False,
            find="ds_2",
            resource="datasources",
            incremental_refresh=True,
            **self.kwargs,
        )

        with pytest.raises(TypeError, match="Some other type error"):
            operator.execute(context={})

    @patch("airflow.providers.tableau.operators.tableau.TableauHook")
    def test_full_refresh_works_on_older_versions(self, mock_tableau_hook):
        """
        Test that full refresh (incremental_refresh=False) works fine on older
        tableauserverclient versions since the incremental kwarg is not passed.
        """
        mock_tableau_hook.get_all = Mock(return_value=self.mock_datasources)
        mock_tableau_hook.return_value.__enter__ = Mock(return_value=mock_tableau_hook)

        operator = TableauOperator(
            blocking_refresh=False,
            find="ds_2",
            resource="datasources",
            incremental_refresh=False,
            **self.kwargs,
        )

        job_id = operator.execute(context={})

        # Verify that refresh was called WITHOUT the incremental parameter
        mock_tableau_hook.server.datasources.refresh.assert_called_once_with(2)
        assert job_id == mock_tableau_hook.server.datasources.refresh.return_value.id
