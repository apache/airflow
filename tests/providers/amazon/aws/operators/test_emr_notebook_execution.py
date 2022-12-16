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
from airflow.providers.amazon.aws.operators.emr import (
    EmrStartNotebookExecutionOperator,
    EmrStopNotebookExecutionOperator,
)

PARAMS = {
    "EditorId": "test_editor",
    "RelativePath": "test_relative_path",
    "ServiceRole": "test_role",
    "NotebookExecutionName": "test_name",
    "NotebookParams": "test_params",
    "NotebookInstanceSecurityGroupId": "test_notebook_instance_security_group_id",
    "Tags": [{"test_key": "test_value"}],
    "ExecutionEngine": {
        "Id": "test_cluster_id",
        "Type": "EMR",
        "MasterInstanceSecurityGroupId": "test_master_instance_security_group_id",
    },
}


class TestEmrStartNotebookExecutionOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_start_notebook_execution_wait_for_completion(self, mock_conn):
        test_execution_id = "test-execution-id"
        mock_conn.start_notebook_execution.return_value = {
            "NotebookExecutionId": test_execution_id,
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }
        mock_conn.describe_notebook_execution.return_value = {"NotebookExecution": {"Status": "FINISHED"}}

        op = EmrStartNotebookExecutionOperator(
            task_id="test-id",
            editor_id=PARAMS["EditorId"],
            relative_path=PARAMS["RelativePath"],
            cluster_id=PARAMS["ExecutionEngine"]["Id"],
            service_role=PARAMS["ServiceRole"],
            notebook_execution_name=PARAMS["NotebookExecutionName"],
            notebook_params=PARAMS["NotebookParams"],
            notebook_instance_security_group_id=PARAMS["NotebookInstanceSecurityGroupId"],
            master_instance_security_group_id=PARAMS["ExecutionEngine"]["MasterInstanceSecurityGroupId"],
            tags=PARAMS["Tags"],
            wait_for_completion=True,
        )
        op_response = op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        assert op_response == test_execution_id

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_start_notebook_execution_no_wait_for_completion(self, mock_conn):
        test_execution_id = "test-execution-id"
        mock_conn.start_notebook_execution.return_value = {
            "NotebookExecutionId": test_execution_id,
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

        op = EmrStartNotebookExecutionOperator(
            task_id="test-id",
            editor_id=PARAMS["EditorId"],
            relative_path=PARAMS["RelativePath"],
            cluster_id=PARAMS["ExecutionEngine"]["Id"],
            service_role=PARAMS["ServiceRole"],
            notebook_execution_name=PARAMS["NotebookExecutionName"],
            notebook_params=PARAMS["NotebookParams"],
            notebook_instance_security_group_id=PARAMS["NotebookInstanceSecurityGroupId"],
            master_instance_security_group_id=PARAMS["ExecutionEngine"]["MasterInstanceSecurityGroupId"],
            tags=PARAMS["Tags"],
        )
        op_response = op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        assert op.wait_for_completion is False
        assert not mock_conn.describe_notebook_execution.called
        assert op_response == test_execution_id

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_start_notebook_execution_http_code_fail(self, mock_conn):
        test_execution_id = "test-execution-id"
        mock_conn.start_notebook_execution.return_value = {
            "NotebookExecutionId": test_execution_id,
            "ResponseMetadata": {
                "HTTPStatusCode": 400,
            },
        }
        op = EmrStartNotebookExecutionOperator(
            task_id="test-id",
            editor_id=PARAMS["EditorId"],
            relative_path=PARAMS["RelativePath"],
            cluster_id=PARAMS["ExecutionEngine"]["Id"],
            service_role=PARAMS["ServiceRole"],
            notebook_execution_name=PARAMS["NotebookExecutionName"],
            notebook_params=PARAMS["NotebookParams"],
            notebook_instance_security_group_id=PARAMS["NotebookInstanceSecurityGroupId"],
            master_instance_security_group_id=PARAMS["ExecutionEngine"]["MasterInstanceSecurityGroupId"],
            tags=PARAMS["Tags"],
        )
        with pytest.raises(AirflowException, match=r"Starting notebook execution failed:"):
            op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_start_notebook_execution_wait_for_completion_multiple_attempts(self, mock_conn, _):
        test_execution_id = "test-execution-id"
        mock_conn.start_notebook_execution.return_value = {
            "NotebookExecutionId": test_execution_id,
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }
        mock_conn.describe_notebook_execution.side_effect = [
            {"NotebookExecution": {"Status": "PENDING"}},
            {"NotebookExecution": {"Status": "PENDING"}},
            {"NotebookExecution": {"Status": "FINISHED"}},
        ]

        op = EmrStartNotebookExecutionOperator(
            task_id="test-id",
            editor_id=PARAMS["EditorId"],
            relative_path=PARAMS["RelativePath"],
            cluster_id=PARAMS["ExecutionEngine"]["Id"],
            service_role=PARAMS["ServiceRole"],
            notebook_execution_name=PARAMS["NotebookExecutionName"],
            notebook_params=PARAMS["NotebookParams"],
            notebook_instance_security_group_id=PARAMS["NotebookInstanceSecurityGroupId"],
            master_instance_security_group_id=PARAMS["ExecutionEngine"]["MasterInstanceSecurityGroupId"],
            tags=PARAMS["Tags"],
            wait_for_completion=True,
        )
        op_response = op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        mock_conn.describe_notebook_execution.assert_called_with(NotebookExecutionId=test_execution_id)
        assert mock_conn.describe_notebook_execution.call_count == 3
        assert op_response == test_execution_id

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_start_notebook_execution_wait_for_completion_fail_state(self, mock_conn):
        test_execution_id = "test-execution-id"
        mock_conn.start_notebook_execution.return_value = {
            "NotebookExecutionId": test_execution_id,
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }
        mock_conn.describe_notebook_execution.return_value = {"NotebookExecution": {"Status": "FAILED"}}

        op = EmrStartNotebookExecutionOperator(
            task_id="test-id",
            editor_id=PARAMS["EditorId"],
            relative_path=PARAMS["RelativePath"],
            cluster_id=PARAMS["ExecutionEngine"]["Id"],
            service_role=PARAMS["ServiceRole"],
            notebook_execution_name=PARAMS["NotebookExecutionName"],
            notebook_params=PARAMS["NotebookParams"],
            notebook_instance_security_group_id=PARAMS["NotebookInstanceSecurityGroupId"],
            master_instance_security_group_id=PARAMS["ExecutionEngine"]["MasterInstanceSecurityGroupId"],
            tags=PARAMS["Tags"],
            wait_for_completion=True,
        )
        with pytest.raises(AirflowException, match=r"Notebook Execution reached failure state FAILED\."):
            op.execute(None)
        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)


class TestStopEmrNotebookExecutionOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_stop_notebook_execution(self, mock_conn):
        mock_conn.stop_notebook_execution.return_value = None
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id",
            notebook_execution_id=test_execution_id,
        )

        op.execute(None)

        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        assert not mock_conn.describe_notebook_execution.called

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_stop_notebook_execution_wait_for_completion(self, mock_conn):
        mock_conn.stop_notebook_execution.return_value = None
        mock_conn.describe_notebook_execution.return_value = {"NotebookExecution": {"Status": "FINISHED"}}
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)

    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_stop_notebook_execution_wait_for_completion_fail_state(self, mock_conn):
        mock_conn.stop_notebook_execution.return_value = None
        mock_conn.describe_notebook_execution.return_value = {"NotebookExecution": {"Status": "FAILED"}}
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        with pytest.raises(AirflowException, match=r"Notebook Execution reached failure state FAILED."):
            op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_conn.describe_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)

    @mock.patch("time.sleep", return_value=None)
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_stop_notebook_execution_wait_for_completion_multiple_attempts(self, mock_conn, _):
        mock_conn.stop_notebook_execution.return_value = None
        mock_conn.describe_notebook_execution.side_effect = [
            {"NotebookExecution": {"Status": "PENDING"}},
            {"NotebookExecution": {"Status": "PENDING"}},
            {"NotebookExecution": {"Status": "FINISHED"}},
        ]
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_conn.describe_notebook_execution.assert_called_with(NotebookExecutionId=test_execution_id)
        assert mock_conn.describe_notebook_execution.call_count == 3

    @mock.patch("airflow.providers.amazon.aws.operators.emr.waiter")
    @mock.patch("airflow.providers.amazon.aws.hooks.emr.EmrHook.conn")
    def test_stop_notebook_execution_waiter_config(self, mock_conn, mock_waiter):
        mock_waiter.return_value = True

        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id",
            notebook_execution_id=test_execution_id,
            wait_for_completion=True,
            waiter_countdown=400,
            waiter_check_interval_seconds=12,
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_waiter.assert_called_once_with(
            get_state_callable=mock_conn.describe_notebook_execution,
            get_state_args={"NotebookExecutionId": test_execution_id},
            parse_response=["NotebookExecution", "Status"],
            desired_state={"STOPPED", "FINISHED"},
            failure_states={"FAILED"},
            object_type="notebook execution",
            action="stopped",
            countdown=400,
            check_interval_seconds=12,
        )
