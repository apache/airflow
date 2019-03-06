# -*- coding: utf-8 -*-
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

import json
import unittest

from freezegun import freeze_time
from mock import patch

from airflow import AirflowException
from airflow import models
from airflow.api.client.local_client import Client
from airflow.example_dags import example_bash_operator
from airflow.models import DagModel, DagBag
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from tests.test_utils.db import clear_db_pools

EXECDATE = timezone.utcnow()
EXECDATE_NOFRACTIONS = EXECDATE.replace(microsecond=0)
EXECDATE_ISO = EXECDATE_NOFRACTIONS.isoformat()


class TestLocalClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        super(TestLocalClient, cls).setUpClass()
        DagBag(example_bash_operator.__file__).get_dag("example_bash_operator").sync_to_db()

    def setUp(self):
        super(TestLocalClient, self).setUp()
        clear_db_pools()
        self.client = Client(api_base_url=None, auth=None)

    def tearDown(self):
        clear_db_pools()
        super(TestLocalClient, self).tearDown()

    @patch.object(models.DAG, 'create_dagrun')
    def test_trigger_dag(self, mock):
        test_dag_id = "example_bash_operator"
        models.DagBag(include_examples=True)

        # non existent
        with self.assertRaises(AirflowException):
            self.client.trigger_dag(dag_id="blablabla")

        with freeze_time(EXECDATE):
            # no execution date, execution date should be set automatically
            self.client.trigger_dag(dag_id=test_dag_id)
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # execution date with microseconds cutoff
            self.client.trigger_dag(dag_id=test_dag_id, execution_date=EXECDATE)
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # run id
            run_id = "my_run_id"
            self.client.trigger_dag(dag_id=test_dag_id, run_id=run_id)
            mock.assert_called_once_with(run_id=run_id,
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=None,
                                         external_trigger=True)
            mock.reset_mock()

            # test conf
            conf = '{"name": "John"}'
            self.client.trigger_dag(dag_id=test_dag_id, conf=conf)
            mock.assert_called_once_with(run_id="manual__{0}".format(EXECDATE_ISO),
                                         execution_date=EXECDATE_NOFRACTIONS,
                                         state=State.RUNNING,
                                         conf=json.loads(conf),
                                         external_trigger=True)
            mock.reset_mock()

    def test_delete_dag(self):
        key = "my_dag_id"

        with create_session() as session:
            self.assertEqual(session.query(DagModel).filter(DagModel.dag_id == key).count(), 0)
            session.add(DagModel(dag_id=key))

        with create_session() as session:
            self.assertEqual(session.query(DagModel).filter(DagModel.dag_id == key).count(), 1)

            self.client.delete_dag(dag_id=key)
            self.assertEqual(session.query(DagModel).filter(DagModel.dag_id == key).count(), 0)

    def test_get_pool(self):
        self.client.create_pool(name='foo', slots=1, description='')
        pool = self.client.get_pool(name='foo')
        self.assertEqual(pool, ('foo', 1, ''))

    def test_get_pools(self):
        self.client.create_pool(name='foo1', slots=1, description='')
        self.client.create_pool(name='foo2', slots=2, description='')
        pools = sorted(self.client.get_pools(), key=lambda p: p[0])
        self.assertEqual(pools, [('foo1', 1, ''), ('foo2', 2, '')])

    def test_create_pool(self):
        pool = self.client.create_pool(name='foo', slots=1, description='')
        self.assertEqual(pool, ('foo', 1, ''))
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 1)

    def test_delete_pool(self):
        self.client.create_pool(name='foo', slots=1, description='')
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 1)
        self.client.delete_pool(name='foo')
        with create_session() as session:
            self.assertEqual(session.query(models.Pool).count(), 0)
