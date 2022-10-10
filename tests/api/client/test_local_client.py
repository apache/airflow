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

import json
import random
import string
from unittest.mock import patch

import pendulum
import pytest
from freezegun import freeze_time

from airflow.api.client.local_client import Client
from airflow.example_dags import example_bash_operator
from airflow.exceptions import AirflowBadRequest, AirflowException, PoolNotFound
from airflow.models import DAG, DagBag, DagModel, DagRun, Pool
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType
from tests.test_utils.db import clear_db_pools

EXECDATE = timezone.utcnow()
EXECDATE_NOFRACTIONS = EXECDATE.replace(microsecond=0)
EXECDATE_ISO = EXECDATE_NOFRACTIONS.isoformat()


class TestLocalClient:
    @classmethod
    def setup_class(cls):
        DagBag(example_bash_operator.__file__).get_dag("example_bash_operator").sync_to_db()

    def setup_method(self):
        clear_db_pools()
        self.client = Client(api_base_url=None, auth=None)

    def teardown_method(self):
        clear_db_pools()

    @patch.object(DAG, 'create_dagrun')
    def test_trigger_dag(self, mock):
        test_dag_id = "example_bash_operator"
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, EXECDATE_NOFRACTIONS)

        DagBag(include_examples=True)

        # non existent
        with pytest.raises(AirflowException):
            self.client.trigger_dag(dag_id="blablabla")

        dag_model = DagModel.get_current(test_dag_id)
        dagbag = DagBag(dag_folder=dag_model.fileloc, read_dags_from_db=True)
        dag = dagbag.get_dag(test_dag_id)
        expected_dag_hash = dagbag.dags_hash.get(test_dag_id)
        expected_data_interval = dag.timetable.infer_manual_data_interval(
            run_after=pendulum.instance(EXECDATE_NOFRACTIONS)
        )

        with freeze_time(EXECDATE):
            # no execution date, execution date should be set automatically

            self.client.trigger_dag(dag_id=test_dag_id)
            mock.assert_called_once_with(
                run_id=run_id,
                execution_date=EXECDATE_NOFRACTIONS,
                state=DagRunState.QUEUED,
                conf=None,
                external_trigger=True,
                dag_hash=expected_dag_hash,
                data_interval=expected_data_interval,
            )
            mock.reset_mock()

            # execution date with microseconds cutoff
            self.client.trigger_dag(dag_id=test_dag_id, execution_date=EXECDATE)
            mock.assert_called_once_with(
                run_id=run_id,
                execution_date=EXECDATE_NOFRACTIONS,
                state=DagRunState.QUEUED,
                conf=None,
                external_trigger=True,
                dag_hash=expected_dag_hash,
                data_interval=expected_data_interval,
            )
            mock.reset_mock()

            # run id
            custom_run_id = "my_run_id"
            self.client.trigger_dag(dag_id=test_dag_id, run_id=custom_run_id)
            mock.assert_called_once_with(
                run_id=custom_run_id,
                execution_date=EXECDATE_NOFRACTIONS,
                state=DagRunState.QUEUED,
                conf=None,
                external_trigger=True,
                dag_hash=expected_dag_hash,
                data_interval=expected_data_interval,
            )
            mock.reset_mock()

            # test conf
            conf = '{"name": "John"}'
            self.client.trigger_dag(dag_id=test_dag_id, conf=conf)
            mock.assert_called_once_with(
                run_id=run_id,
                execution_date=EXECDATE_NOFRACTIONS,
                state=DagRunState.QUEUED,
                conf=json.loads(conf),
                external_trigger=True,
                dag_hash=expected_dag_hash,
                data_interval=expected_data_interval,
            )
            mock.reset_mock()

    def test_delete_dag(self):
        key = "my_dag_id"

        with create_session() as session:
            assert session.query(DagModel).filter(DagModel.dag_id == key).count() == 0
            session.add(DagModel(dag_id=key))

        with create_session() as session:
            assert session.query(DagModel).filter(DagModel.dag_id == key).count() == 1

            self.client.delete_dag(dag_id=key)
            assert session.query(DagModel).filter(DagModel.dag_id == key).count() == 0

    def test_get_pool(self):
        self.client.create_pool(name='foo', slots=1, description='')
        pool = self.client.get_pool(name='foo')
        assert pool == ('foo', 1, '')

    def test_get_pool_non_existing_raises(self):
        with pytest.raises(PoolNotFound):
            self.client.get_pool(name='foo')

    def test_get_pools(self):
        self.client.create_pool(name='foo1', slots=1, description='')
        self.client.create_pool(name='foo2', slots=2, description='')
        pools = sorted(self.client.get_pools(), key=lambda p: p[0])
        assert pools == [('default_pool', 128, 'Default pool'), ('foo1', 1, ''), ('foo2', 2, '')]

    def test_create_pool(self):
        pool = self.client.create_pool(name='foo', slots=1, description='')
        assert pool == ('foo', 1, '')
        with create_session() as session:
            assert session.query(Pool).count() == 2

    def test_create_pool_bad_slots(self):
        with pytest.raises(AirflowBadRequest, match="^Bad value for `slots`: foo$"):
            self.client.create_pool(
                name='foo',
                slots='foo',
                description='',
            )

    def test_create_pool_name_too_long(self):
        long_name = ''.join(random.choices(string.ascii_lowercase, k=300))
        pool_name_length = Pool.pool.property.columns[0].type.length
        with pytest.raises(
            AirflowBadRequest, match=f"^pool name cannot be more than {pool_name_length} characters"
        ):
            self.client.create_pool(
                name=long_name,
                slots=5,
                description='',
            )

    def test_delete_pool(self):
        self.client.create_pool(name='foo', slots=1, description='')
        with create_session() as session:
            assert session.query(Pool).count() == 2
        self.client.delete_pool(name='foo')
        with create_session() as session:
            assert session.query(Pool).count() == 1
        for name in ('', '    '):
            with pytest.raises(PoolNotFound, match=f"^Pool {name!r} doesn't exist$"):
                Pool.delete_pool(name=name)
