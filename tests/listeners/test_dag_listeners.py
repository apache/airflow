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

import pendulum
import pytest as pytest
from sqlalchemy.orm import Session

from airflow import AirflowException, DAG
from airflow.decorators import task
from airflow.listeners import hookimpl
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagRun
from airflow.operators.empty import EmptyOperator


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    yield
    lm = get_listener_manager()
    lm.clear()


mock_running_object = mock.MagicMock()
mock_success_object = mock.MagicMock()
mock_failure_object = mock.MagicMock()


class TestDagListeners:
    @hookimpl
    def on_dag_run_running(dag_run: DagRun, msg: str, session):
        assert session is not None
        assert isinstance(session, Session)
        assert session.is_active
        mock_running_object(f'invoked with dag_run={dag_run}, msg={msg}, session={session}')

    @hookimpl
    def on_dag_run_success(dag_run: DagRun, msg: str, session):
        assert session is not None
        assert isinstance(session, Session)
        assert session.is_active
        mock_success_object(f'invoked with dag_run={dag_run}, msg={msg}, session={session}')

    @hookimpl
    def on_dag_run_failed(dag_run: DagRun, msg: str, session):
        assert session is not None
        assert isinstance(session, Session)
        assert session.is_active
        mock_failure_object(f'invoked with dag_run={dag_run}, msg={msg}, session={session}')


def test_dag_run_listener_success(create_task_instance, dag_maker):
    with DAG(
        dag_id='test_dag_run_listener_success',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None
    ) as dag:
        task1 = EmptyOperator(task_id="run_this_first")
        task2 = EmptyOperator(task_id="run_this_last")

        task1 >> task2

    lm = get_listener_manager()
    lm.add_listener(TestDagListeners)

    mock_running_object.reset_mock()
    mock_success_object.reset_mock()
    mock_failure_object.reset_mock()

    dag.test()

    mock_running_object.assert_called_once()
    mock_success_object.assert_called_once()
    mock_failure_object.assert_not_called()


def test_dag_run_listener_fail(create_task_instance, dag_maker):
    with DAG(
        dag_id='test_dag_run_listener_fail',
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule=None
    ) as dag:
        task1 = EmptyOperator(task_id="run_this_first")

        @task
        def task2(ds=None, **kwargs):
            raise AirflowException("boooooooom")

        task1 >> task2()

    lm = get_listener_manager()
    lm.add_listener(TestDagListeners)

    mock_running_object.reset_mock()
    mock_success_object.reset_mock()
    mock_failure_object.reset_mock()

    dag.test()

    mock_running_object.assert_called_once()
    mock_success_object.assert_not_called()
    mock_failure_object.assert_called_once()
