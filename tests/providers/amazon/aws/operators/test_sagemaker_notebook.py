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

from airflow.providers.amazon.aws.hooks.sagemaker_notebook import SageMakerNotebookHook
from airflow.providers.amazon.aws.operators.sagemaker_notebook import (
    SageMakerCreateNotebookOperator,
    SageMakerDeleteNotebookOperator,
    SageMakerStartNoteBookOperator,
    SageMakerStopNotebookOperator,
)

INSTANCE_NAME = "notebook"
INSTANCE_TYPE = "ml.t3.medium"
ROLE_ARN = "arn:aws:iam:role"


class TestSagemakerCreateNotebookOperator:
    @mock.patch.object(SageMakerNotebookHook, "create_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_create_notebook_without_wait_for_completion(self, mock_hook_conn, mock_hook_create_instance):
        operator = SageMakerCreateNotebookOperator(
            task_id="task_test",
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
            wait_for_completion=False,
        )
        operator.execute(None)
        mock_hook_create_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    # test wait_for_completion
    @mock.patch.object(SageMakerNotebookHook, "create_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_create_notebook_wait_for_completion(self, mock_hook_conn, mock_hook_create_instance):
        operator = SageMakerCreateNotebookOperator(
            task_id="task_test",
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
        )
        operator.execute(None)
        mock_hook_create_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_in_service")


class TestSageMakerStopNotebookOperator:
    @mock.patch.object(SageMakerNotebookHook, "stop_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_stop_notebook_without_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerStopNotebookOperator(
            task_id="stop_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerNotebookHook, "stop_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_stop_notebook_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerStopNotebookOperator(
            task_id="stop_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_stopped")


class TestSageMakerDeleteNotebookOperator:
    @mock.patch.object(SageMakerNotebookHook, "delete_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_delete_notebook_without_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerDeleteNotebookOperator(
            task_id="delete_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerNotebookHook, "delete_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_delete_notebook_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerDeleteNotebookOperator(
            task_id="delete_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_deleted")


class TestSageMakerStartNotebookOperator:
    @mock.patch.object(SageMakerNotebookHook, "start_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_start_notebook_without_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerStartNoteBookOperator(
            task_id="start_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerNotebookHook, "start_instance")
    @mock.patch.object(SageMakerNotebookHook, "conn")
    def test_delete_notebook_wait_for_completion(self, mock_hook_conn, mock_hook_stop_instance):
        operator = SageMakerStartNoteBookOperator(
            task_id="start_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        mock_hook_stop_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_in_service")
