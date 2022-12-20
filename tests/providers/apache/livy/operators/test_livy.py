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

import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.apache.livy.hooks.livy import BatchState, LivyHook
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
BATCH_ID = 100
APP_ID = "application_1433865536131_34483"
GET_BATCH = {"appId": APP_ID}
LOG_RESPONSE = {"total": 3, "log": ["first_line", "second_line", "third_line"]}


class TestLivyOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        self.dag = DAG("test_dag_id", default_args=args)
        db.merge_conn(
            Connection(
                conn_id="livyunittest", conn_type="livy", host="localhost:8998", port="8998", schema="http"
            )
        )
        self.mock_context = dict(ti=MagicMock())

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.SUCCESS]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example")
        task._livy_hook = task.get_hook()
        task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state")
    def test_poll_for_termination_fail(self, mock_livy, mock_dump_logs):
        state_list = 2 * [BatchState.RUNNING] + [BatchState.ERROR]

        def side_effect(_, retry_args):
            if state_list:
                return state_list.pop(0)
            # fail if does not stop right before
            raise AssertionError()

        mock_livy.side_effect = side_effect

        task = LivyOperator(file="sparkapp", polling_interval=1, dag=self.dag, task_id="livy_example")
        task._livy_hook = task.get_hook()

        with pytest.raises(AirflowException):
            task.poll_for_termination(BATCH_ID)

        mock_livy.assert_called_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_with(BATCH_ID)
        assert mock_livy.call_count == 3

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.dump_batch_logs",
        return_value=None,
    )
    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution(self, mock_get_batch, mock_post, mock_get, mock_dump_logs):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            polling_interval=1,
            dag=self.dag,
            task_id="livy_example",
        )
        task.execute(context=self.mock_context)

        call_args = {k: v for k, v in mock_post.call_args[1].items() if v}
        assert call_args == {"file": "sparkapp"}
        mock_get.assert_called_once_with(BATCH_ID, retry_args=None)
        mock_dump_logs.assert_called_once_with(BATCH_ID)
        mock_get_batch.assert_called_once_with(BATCH_ID)
        self.mock_context["ti"].xcom_push.assert_called_once_with(key="app_id", value=APP_ID)

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_execution_with_extra_options(self, mock_get_batch, mock_post):
        extra_options = {"check_response": True}
        task = LivyOperator(
            file="sparkapp", dag=self.dag, task_id="livy_example", extra_options=extra_options
        )

        task.execute(context=self.mock_context)

        assert task.get_hook().extra_options == extra_options

    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.delete_batch")
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_deletion(self, mock_get_batch, mock_post, mock_delete):
        task = LivyOperator(
            livy_conn_id="livyunittest", file="sparkapp", dag=self.dag, task_id="livy_example"
        )
        task.execute(context=self.mock_context)
        task.kill()

        mock_delete.assert_called_once_with(BATCH_ID)

    def test_injected_hook(self):
        def_hook = LivyHook(livy_conn_id="livyunittest")

        task = LivyOperator(file="sparkapp", dag=self.dag, task_id="livy_example")
        task._livy_hook = def_hook

        assert task.get_hook() == def_hook

    @patch(
        "airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_state",
        return_value=BatchState.SUCCESS,
    )
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch_logs", return_value=LOG_RESPONSE)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.post_batch", return_value=BATCH_ID)
    @patch("airflow.providers.apache.livy.operators.livy.LivyHook.get_batch", return_value=GET_BATCH)
    def test_log_dump(self, mock_get_batch, mock_post, mock_get_logs, mock_get, caplog):
        task = LivyOperator(
            livy_conn_id="livyunittest",
            file="sparkapp",
            dag=self.dag,
            task_id="livy_example",
            polling_interval=1,
        )
        caplog.clear()
        with caplog.at_level(level=logging.INFO, logger=task.get_hook().log.name):
            task.execute(context=self.mock_context)

        assert "first_line" in caplog.messages
        assert "second_line" in caplog.messages
        assert "third_line" in caplog.messages

        mock_get.assert_called_once_with(BATCH_ID, retry_args=None)
        mock_get_logs.assert_called_once_with(BATCH_ID, 0, 100)
