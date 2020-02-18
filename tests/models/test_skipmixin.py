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

import datetime
import unittest
from unittest.mock import Mock, patch

import pendulum

from airflow import settings
from airflow.models import DAG, SkipMixin, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestSkipMixin(unittest.TestCase):

    @patch('airflow.utils.timezone.utcnow')
    def test_skip(self, mock_now):
        session = settings.Session()
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
        )
        with dag:
            tasks = [DummyOperator(task_id='task')]
        dag_run = dag.create_dagrun(
            run_id='manual__' + now.isoformat(),
            state=State.FAILED,
        )
        SkipMixin().skip(
            dag_run=dag_run,
            execution_date=now,
            tasks=tasks,
            session=session)

        session.query(TI).filter(
            TI.dag_id == 'dag',
            TI.task_id == 'task',
            TI.state == State.SKIPPED,
            TI.start_date == now,
            TI.end_date == now,
        ).one()

    @patch('airflow.utils.timezone.utcnow')
    def test_skip_none_dagrun(self, mock_now):
        session = settings.Session()
        now = datetime.datetime.utcnow().replace(tzinfo=pendulum.timezone('UTC'))
        mock_now.return_value = now
        dag = DAG(
            'dag',
            start_date=DEFAULT_DATE,
        )
        with dag:
            tasks = [DummyOperator(task_id='task')]
        SkipMixin().skip(
            dag_run=None,
            execution_date=now,
            tasks=tasks,
            session=session)

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
        self.assertFalse(session.query.called)
        self.assertFalse(session.commit.called)
