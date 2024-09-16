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
from botocore.waiter import Waiter

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.providers.amazon.aws.operators.emr import (
    EmrStartNotebookExecutionOperator,
    EmrStopNotebookExecutionOperator,
)
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields
from tests.providers.amazon.aws.utils.test_waiter import assert_expected_waiter_type

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
    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(EmrHook, "conn")
    @mock.patch.object(Waiter, "wait")
    def test_start_notebook_execution_wait_for_completion(self, mock_waiter, mock_conn, _):
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
            wait_for_completion=True,
        )
        op_response = op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_running")
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

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch("time.sleep", return_value=None)
    @mock.patch.object(EmrHook, "conn")
    @mock.patch.object(Waiter, "wait")
    def test_start_notebook_execution_wait_for_completion_multiple_attempts(self, mock_waiter, mock_conn, *_):
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
            wait_for_completion=True,
        )
        op_response = op.execute(None)

        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)
        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_running")
        assert op_response == test_execution_id

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(EmrHook, "conn")
    @mock.patch.object(Waiter, "wait")
    def test_start_notebook_execution_wait_for_completion_fail_state(self, mock_waiter, mock_conn, _):
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

        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_running")
        mock_conn.start_notebook_execution.assert_called_once_with(**PARAMS)


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

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(EmrHook, "conn")
    @mock.patch.object(Waiter, "wait")
    def test_stop_notebook_execution_wait_for_completion(self, mock_waiter, mock_conn, _):
        mock_conn.stop_notebook_execution.return_value = None
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_stopped")

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(EmrHook, "conn")
    @mock.patch.object(Waiter, "wait")
    def test_stop_notebook_execution_wait_for_completion_fail_state(self, mock_waiter, mock_conn, _):
        mock_conn.stop_notebook_execution.return_value = None
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_stopped")

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch("time.sleep", return_value=None)
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EmrHook, "conn")
    def test_stop_notebook_execution_wait_for_completion_multiple_attempts(self, mock_conn, mock_waiter, *_):
        mock_conn.stop_notebook_execution.return_value = None
        test_execution_id = "test-execution-id"

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id", notebook_execution_id=test_execution_id, wait_for_completion=True
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_waiter.assert_called_once_with(
            mock.ANY, NotebookExecutionId=test_execution_id, WaiterConfig=mock.ANY
        )
        assert_expected_waiter_type(mock_waiter, "notebook_stopped")

    @mock.patch("botocore.waiter.get_service_module_name", return_value="emr")
    @mock.patch.object(Waiter, "wait")
    @mock.patch.object(EmrHook, "conn")
    def test_stop_notebook_execution_waiter_config(self, mock_conn, mock_waiter, _):
        test_execution_id = "test-execution-id"
        waiter_max_attempts = 35
        delay = 12

        op = EmrStopNotebookExecutionOperator(
            task_id="test-id",
            notebook_execution_id=test_execution_id,
            wait_for_completion=True,
            waiter_max_attempts=waiter_max_attempts,
            waiter_delay=delay,
        )

        op.execute(None)
        mock_conn.stop_notebook_execution.assert_called_once_with(NotebookExecutionId=test_execution_id)
        mock_waiter.assert_called_once_with(
            mock.ANY,
            NotebookExecutionId=test_execution_id,
            WaiterConfig={"Delay": delay, "MaxAttempts": waiter_max_attempts},
        )
        assert_expected_waiter_type(mock_waiter, "notebook_stopped")

    def test_template_fields(self):
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

        validate_template_fields(op)
