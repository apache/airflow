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

from typing import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerCreateNotebookOperator,
    SageMakerDeleteNotebookOperator,
    SageMakerStartNoteBookOperator,
    SageMakerStopNotebookOperator,
)
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

INSTANCE_NAME = "notebook"
INSTANCE_TYPE = "ml.t3.medium"
ROLE_ARN = "arn:aws:iam:role/role"


@pytest.fixture
def hook() -> Generator[SageMakerHook, None, None]:
    with mock_aws():
        yield SageMakerHook(aws_conn_id="aws_default")


@pytest.fixture
def create_instance_args():
    return {
        "NotebookInstanceName": INSTANCE_NAME,
        "InstanceType": INSTANCE_TYPE,
        "RoleArn": ROLE_ARN,
    }


class TestSageMakerHook:
    def test_conn(self):
        hook = SageMakerHook(aws_conn_id="sagemaker_test_conn_id")
        assert hook.aws_conn_id == "sagemaker_test_conn_id"

    def test_create_instance(self, hook: SageMakerHook, create_instance_args, capsys):
        # create a notebook
        resp = hook.conn.create_notebook_instance(**create_instance_args)
        assert resp["NotebookInstanceArn"]

    def test_start_instance(self, hook, create_instance_args, capsys):
        hook.conn.create_notebook_instance(**create_instance_args)
        resp = hook.conn.start_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_stop_instance(self, hook, create_instance_args, capsys):
        hook.conn.create_notebook_instance(**create_instance_args)
        resp = hook.conn.stop_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_delete_instance(self, hook, create_instance_args, capsys):
        hook.conn.create_notebook_instance(**create_instance_args)
        hook.conn.stop_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
        resp = hook.conn.delete_notebook_instance(NotebookInstanceName=INSTANCE_NAME)
        assert resp["ResponseMetadata"]["HTTPStatusCode"] == 200


class TestSagemakerCreateNotebookOperator:
    @mock.patch.object(SageMakerHook, "conn")
    def test_create_notebook_without_wait_for_completion(self, mock_hook_conn):
        operator = SageMakerCreateNotebookOperator(
            task_id="task_test",
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
            wait_for_completion=False,
            volume_size_in_gb=50,
        )
        operator.execute(None)
        mock_hook_conn.create_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerHook, "conn")
    def test_create_notebook_wait_for_completion(self, mock_hook_conn):
        operator = SageMakerCreateNotebookOperator(
            task_id="task_test",
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
        )
        operator.execute(None)
        mock_hook_conn.create_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_in_service")

    def test_template_fields(self):
        operator = SageMakerCreateNotebookOperator(
            task_id="task_test",
            instance_name=INSTANCE_NAME,
            instance_type=INSTANCE_TYPE,
            role_arn=ROLE_ARN,
        )
        validate_template_fields(operator)


class TestSageMakerStopNotebookOperator:
    @mock.patch.object(SageMakerHook, "conn")
    def test_stop_notebook_without_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerStopNotebookOperator(
            task_id="stop_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        hook.conn.stop_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerHook, "conn")
    def test_stop_notebook_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerStopNotebookOperator(
            task_id="stop_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        hook.conn.stop_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_stopped")

    def test_template_fields(self):
        operator = SageMakerStopNotebookOperator(
            task_id="stop_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        validate_template_fields(operator)


class TestSageMakerDeleteNotebookOperator:
    @mock.patch.object(SageMakerHook, "conn")
    def test_delete_notebook_without_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerDeleteNotebookOperator(
            task_id="delete_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        hook.conn.delete_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerHook, "conn")
    def test_delete_notebook_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerDeleteNotebookOperator(
            task_id="delete_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        hook.conn.delete_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_deleted")

    def test_template_fields(self):
        operator = SageMakerDeleteNotebookOperator(
            task_id="delete_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        validate_template_fields(operator)


class TestSageMakerStartNotebookOperator:
    @mock.patch.object(SageMakerHook, "conn")
    def test_start_notebook_without_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerStartNoteBookOperator(
            task_id="start_test", instance_name=INSTANCE_NAME, wait_for_completion=False
        )
        operator.execute(None)
        hook.conn.start_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_not_called()

    @mock.patch.object(SageMakerHook, "conn")
    def test_start_notebook_wait_for_completion(self, mock_hook_conn, hook):
        operator = SageMakerStartNoteBookOperator(
            task_id="start_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        operator.execute(None)
        hook.conn.start_notebook_instance.assert_called_once()
        mock_hook_conn.get_waiter.assert_called_once_with("notebook_instance_in_service")

    def test_template_fields(self):
        operator = SageMakerStartNoteBookOperator(
            task_id="start_test", instance_name=INSTANCE_NAME, wait_for_completion=True
        )
        validate_template_fields(operator)
