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

import pytest

from airflow.exceptions import TaskDeferred
from airflow.providers.amazon.aws.hooks.glue_session import GlueSessionHook
from airflow.providers.amazon.aws.operators.glue_session import (
    GlueCreateSessionOperator,
    GlueDeleteSessionOperator,
)

if TYPE_CHECKING:
    from airflow.models import TaskInstance

TASK_ID = "test_glue_session_operator"
DAG_ID = "test_dag_id"
SESSION_ID = "test_session_id"


class TestGlueCreateSessionOperator:
    @pytest.mark.db_test
    def test_render_template(self, create_task_instance_of_operator):
        ti: TaskInstance = create_task_instance_of_operator(
            GlueCreateSessionOperator,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            create_session_kwargs="{{ dag.dag_id }}",
            iam_role_name="{{ dag.dag_id }}",
            iam_role_arn="{{ dag.dag_id }}",
            session_id="{{ dag.dag_id }}",
        )
        rendered_template: GlueCreateSessionOperator = ti.render_templates()

        assert DAG_ID == rendered_template.create_session_kwargs
        assert DAG_ID == rendered_template.iam_role_name
        assert DAG_ID == rendered_template.iam_role_arn
        assert DAG_ID == rendered_template.session_id

    @mock.patch.object(GlueSessionHook, "get_session_state")
    @mock.patch.object(GlueSessionHook, "initialize_session")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_execute_without_failure(
        self,
        _,
        mock_initialize_session,
        mock_get_session_state,
    ):
        glue_session = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name="my_test_role",
        )
        mock_get_session_state.return_value = "READY"

        glue_session.execute(mock.MagicMock())

        mock_initialize_session.assert_called_once()
        assert glue_session.session_id == SESSION_ID

    @mock.patch.object(GlueSessionHook, "initialize_session")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_role_arn_execute_deferrable(self, _, mock_initialize_session):
        glue = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_arn="test_role",
            deferrable=True,
        )
        mock_initialize_session.return_value = {"SessionState": "PROVISIONING"}

        with pytest.raises(TaskDeferred) as defer:
            glue.execute(mock.MagicMock())

        assert defer.value.trigger.session_id == SESSION_ID

    @mock.patch.object(GlueSessionHook, "initialize_session")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_execute_deferrable(self, _, mock_initialize_session):
        glue = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name="my_test_role",
            deferrable=True,
        )
        mock_initialize_session.return_value = {"SessionState": "PROVISIONING"}

        with pytest.raises(TaskDeferred) as defer:
            glue.execute(mock.MagicMock())

        assert defer.value.trigger.session_id == SESSION_ID

    @mock.patch.object(GlueSessionHook, "session_readiness")
    @mock.patch.object(GlueSessionHook, "initialize_session")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_execute_without_waiting_for_readiness(
        self, mock_get_conn, mock_initialize_session, mock_session_readiness
    ):
        glue = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name="my_test_role",
            wait_for_readiness=False,
        )
        mock_initialize_session.return_value = {"SessionState": "PROVISIONING"}

        glue.execute(mock.MagicMock())

        mock_initialize_session.assert_called_once_with()
        mock_session_readiness.assert_not_called()
        assert glue.session_id == SESSION_ID

    @mock.patch.object(GlueSessionHook, "conn")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_killed_without_delete_session_on_kill(
        self,
        _,
        mock_get_conn,
    ):
        glue_session = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            region_name="us-west-2",
            iam_role_name="my_test_role",
        )
        glue_session.on_kill()
        mock_get_conn.delete_session.assert_not_called()

    @mock.patch.object(GlueSessionHook, "conn")
    @mock.patch.object(GlueSessionHook, "get_conn")
    def test_killed_with_delete_session_on_kill(
        self,
        _,
        mock_get_conn,
    ):
        glue_session = GlueCreateSessionOperator(
            task_id=TASK_ID,
            session_id=SESSION_ID,
            aws_conn_id="aws_default",
            iam_role_name="my_test_role",
            region_name="us-west-2",
            delete_session_on_kill=True,
        )
        glue_session.on_kill()
        mock_get_conn.delete_session.assert_called_once()
