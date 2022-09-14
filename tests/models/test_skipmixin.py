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

import datetime
from unittest.mock import Mock, patch

import pendulum
import pytest

from airflow import settings
from airflow.models.skipmixin import SkipMixin
from airflow.models.taskinstance import TaskInstance as TI
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_dags, clear_db_runs

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestSkipMixin:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()

    def setup_method(self):
        self.clean_db()

    def teardown_method(self):
        self.clean_db()

    @patch('airflow.utils.timezone.utcnow')
    def test_skip(self, mock_now, dag_maker):
        session = settings.Session()
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        with dag_maker('dag'):
            tasks = [EmptyOperator(task_id='task')]
        dag_run = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            execution_date=now,
            state=State.FAILED,
        )
        SkipMixin().skip(dag_run=dag_run, execution_date=now, tasks=tasks, session=session)

        session.query(TI).filter(
            TI.dag_id == 'dag',
            TI.task_id == 'task',
            TI.state == State.SKIPPED,
            TI.start_date == now,
            TI.end_date == now,
        ).one()

    @patch('airflow.utils.timezone.utcnow')
    def test_skip_none_dagrun(self, mock_now, dag_maker):
        session = settings.Session()
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        with dag_maker(
            'dag',
            session=session,
        ):
            tasks = [EmptyOperator(task_id='task')]
        dag_maker.create_dagrun(execution_date=now)
        SkipMixin().skip(dag_run=None, execution_date=now, tasks=tasks, session=session)

        session.query(TI).filter(
            TI.dag_id == 'dag',
            TI.task_id == 'task',
            TI.state == State.SKIPPED,
            TI.start_date == now,
            TI.end_date == now,
        ).one()

    def test_skip_none_tasks(self):
        session = Mock()
        SkipMixin().skip(dag_run=None, execution_date=None, tasks=[], session=session)
        assert not session.query.called
        assert not session.commit.called

    @pytest.mark.parametrize(
        "branch_task_ids, expected_states",
        [
            (["task2"], {"task2": State.NONE, "task3": State.SKIPPED}),
            ("task2", {"task2": State.NONE, "task3": State.SKIPPED}),
            (None, {"task2": State.SKIPPED, "task3": State.SKIPPED}),
            ([], {"task2": State.SKIPPED, "task3": State.SKIPPED}),
        ],
    )
    def test_skip_all_except(self, dag_maker, branch_task_ids, expected_states):
        with dag_maker(
            'dag_test_skip_all_except',
        ):
            task1 = EmptyOperator(task_id='task1')
            task2 = EmptyOperator(task_id='task2')
            task3 = EmptyOperator(task_id='task3')

            task1 >> [task2, task3]
        dag_maker.create_dagrun()

        ti1 = TI(task1, execution_date=DEFAULT_DATE)
        ti2 = TI(task2, execution_date=DEFAULT_DATE)
        ti3 = TI(task3, execution_date=DEFAULT_DATE)

        SkipMixin().skip_all_except(ti=ti1, branch_task_ids=branch_task_ids)

        def get_state(ti):
            ti.refresh_from_db()
            return ti.state

        executed_states = {"task2": get_state(ti2), "task3": get_state(ti3)}

        assert executed_states == expected_states
