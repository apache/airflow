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

from typing import TYPE_CHECKING
from unittest import mock

import boto3
import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook, GlueSessionStates
from airflow.providers.amazon.aws.operators.glue_session import (
    GlueCreateSessionOperator,
    GlueDeleteSessionOperator,
    GlueSessionBaseOperator,
)
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from airflow.models import TaskInstance

TASK_ID = "test_glue_session_operator"
DAG_ID = "test_dag_id"
SESSION_ID = "test_session_id"
ROLE_NAME = "my_test_role"
ROLE_ARN = f"arn:aws:iam::123456789012:role/{ROLE_NAME}"
WAITERS_TEST_CASES = [
    pytest.param(None, None, id="default-values"),
    pytest.param(3.14, None, id="set-delay-only"),
    pytest.param(None, 42, id="set-max-attempts-only"),
    pytest.param(2.71828, 9000, id="user-defined"),
]


class GlueSessionBaseTestCase:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("glue", region_name="eu-west-3")
        monkeypatch.setattr(GlueSessionHook, "conn", self.client)
        monkeypatch.setenv("AIRFLOW_CONN_AWS_TEST_CONN", '{"conn_type": "aws"}')


class TestGlueSessionBaseOperator(GlueSessionBaseTestCase):
    """Test Base Glue Session Operator."""

    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_initialise_operator(self, aws_conn_id, region_name):
        """Test initialize operator."""
        op_kw = {"aws_conn_id": aws_conn_id, "region_name": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = GlueSessionBaseOperator(task_id="test_ecs_base", **op_kw)

        assert op.aws_conn_id == (aws_conn_id if aws_conn_id is not NOTSET else "aws_default")
        assert op.region_name == (region_name if region_name is not NOTSET else None)

    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, NOTSET, "ca-central-1"])
    def test_initialise_operator_hook(self, aws_conn_id, region_name):
        """Test initialize operator."""
        op_kw = {"aws_conn_id": aws_conn_id, "region_name": region_name}
        op_kw = {k: v for k, v in op_kw.items() if v is not NOTSET}
        op = GlueSessionBaseOperator(task_id="test_ecs_base", **op_kw)

        assert op.hook.aws_conn_id == (aws_conn_id if aws_conn_id is not NOTSET else "aws_default")
        assert op.hook.region_name == (region_name if region_name is not NOTSET else None)

        with mock.patch.object(GlueSessionBaseOperator, "hook", new_callable=mock.PropertyMock) as m:
            mocked_hook = mock.MagicMock(name="MockHook")
            mocked_client = mock.MagicMock(name="Mocklient")
            mocked_hook.conn = mocked_client
            m.return_value = mocked_hook

            assert op.client == mocked_client
            m.assert_called_once()


class TestGlueCreateSessionOperator(GlueSessionBaseTestCase):
    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator):
        ti: TaskInstance = create_task_instance_of_operator(
            GlueCreateSessionOperator,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            create_session_kwargs="{{ dag.dag_id }}",
            iam_role_name="{{ dag.dag_id }}",
            session_id="{{ dag.dag_id }}",
        )
        rendered_template: GlueCreateSessionOperator = ti.render_templates()

        assert DAG_ID == rendered_template.create_session_kwargs
        assert DAG_ID == rendered_template.iam_role_name
        assert DAG_ID == rendered_template.session_id

    @mock.patch.object(GlueSessionHook, "expand_role")
    def test_init_iam_role_value_error(self, mock_expand_role):
        mock_expand_role.return_value = ROLE_ARN

        with pytest.raises(ValueError, match="Cannot set iam_role_arn and iam_role_name simultaneously"):
            GlueCreateSessionOperator(
                task_id=TASK_ID,
                session_id="aws_test_glue_session",
                session_desc="This is test case job from Airflow",
                iam_role_name=ROLE_NAME,
                iam_role_arn=ROLE_ARN,
            )

    @pytest.mark.parametrize(
        "num_of_dpus, create_session_kwargs, expected_error",
        [
            [
                20,
                {"WorkerType": "G.2X", "NumberOfWorkers": 60},
                "Cannot specify num_of_dpus with custom WorkerType",
            ],
            [
                None,
                {"NumberOfWorkers": 60},
                "Need to specify custom WorkerType when specifying NumberOfWorkers",
            ],
            [
                None,
                {"WorkerType": "G.2X"},
                "Need to specify NumberOfWorkers when specifying custom WorkerType",
            ],
        ],
    )
    @mock.patch.object(GlueSessionHook, "expand_role")
    def test_init_worker_configuration_error(
        self, mock_expand_role, num_of_dpus, create_session_kwargs, expected_error
    ):
        mock_expand_role.return_value = ROLE_ARN

        with pytest.raises(ValueError, match=expected_error):
            GlueCreateSessionOperator(
                task_id=TASK_ID,
                session_id="aws_test_glue_session",
                session_desc="This is test case job from Airflow",
                iam_role_name=ROLE_NAME,
                region_name="us-east-2",
                num_of_dpus=num_of_dpus,
                create_session_kwargs=create_session_kwargs,
            )

    @mock.patch.object(GlueCreateSessionOperator, "_wait_for_task_ended")
    @mock.patch.object(GlueSessionHook, "expand_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_execute_without_failure(
        self,
        mock_conn,
        mock_expand_role,
        mock_wait,
    ):
        mock_expand_role.return_value = ROLE_ARN

        class SessionNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()

        op = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name=ROLE_NAME,
        )

        op.execute(mock.MagicMock())

        mock_conn.create_session.assert_called_once()
        mock_wait.assert_called_once()

    @mock.patch.object(GlueCreateSessionOperator, "client")
    def test_wait_end_tasks(self, mock_client):
        op = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name=ROLE_NAME,
        )

        op._wait_for_task_ended()
        mock_client.get_waiter.assert_called_once_with("session_ready")
        mock_client.get_waiter.return_value.wait.assert_called_once_with(
            Id=SESSION_ID, WaiterConfig={"Delay": 15, "MaxAttempts": 60}
        )

    @mock.patch.object(GlueSessionHook, "conn")
    @mock.patch.object(GlueSessionHook, "expand_role")
    def test_execute_deferrable(self, mock_expand_role, mock_conn):
        mock_expand_role.return_value = ROLE_ARN

        class SessionNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()
        mock_conn.create_session.return_value = {
            "Session": {"Status": GlueSessionStates.PROVISIONING, "Id": SESSION_ID}
        }

        op = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name=ROLE_NAME,
            deferrable=True,
            waiter_delay=12,
            waiter_max_attempts=34,
        )

        with pytest.raises(TaskDeferred) as defer:
            op.execute(mock.MagicMock())

        assert defer.value.trigger.waiter_delay == 12
        assert defer.value.trigger.attempts == 34

    @mock.patch.object(GlueCreateSessionOperator, "_wait_for_task_ended")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_execute_immediate_create(self, mock_conn, mock_wait):
        """Test if cluster created during initial request."""
        mock_conn.get_session.result = {"Session": {"Status": "READY"}}
        op = GlueCreateSessionOperator(task_id="task", session_id=SESSION_ID, wait_for_completion=True)

        result = op.execute(mock.MagicMock())

        mock_conn.get_session.assert_called_once_with(Id=SESSION_ID)
        mock_wait.assert_not_called()
        assert result == SESSION_ID

    @mock.patch.object(GlueCreateSessionOperator, "_wait_for_task_ended")
    @mock.patch.object(GlueSessionHook, "expand_role")
    @mock.patch.object(GlueSessionHook, "conn")
    def test_execute_without_waiter(self, mock_conn, mock_expand_role, mock_wait):
        mock_expand_role.return_value = ROLE_ARN

        class SessionNotFoundException(Exception):
            pass

        mock_conn.exceptions.EntityNotFoundException = SessionNotFoundException
        mock_conn.get_session.side_effect = SessionNotFoundException()

        op = GlueCreateSessionOperator(task_id="task", session_id=SESSION_ID, wait_for_completion=False)

        op.execute({})
        mock_conn.create_session.assert_called_once()
        mock_wait.assert_not_called()


class TestGlueDeleteSessionOperator(GlueSessionBaseTestCase):
    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator):
        ti: TaskInstance = create_task_instance_of_operator(
            GlueDeleteSessionOperator,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            session_id="{{ dag.dag_id }}",
        )
        rendered_template: GlueDeleteSessionOperator = ti.render_templates()

        assert DAG_ID == rendered_template.session_id
