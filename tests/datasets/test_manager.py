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

import itertools
from datetime import datetime
from unittest import mock

import pytest
from sqlalchemy import delete

from airflow.datasets import Dataset
from airflow.datasets.manager import DatasetManager
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DagModel
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue, DatasetEvent, DatasetModel
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
from tests.listeners import dataset_listener

pytestmark = pytest.mark.db_test


pytest.importorskip("pydantic", minversion="2.0.0")


@pytest.fixture
def mock_task_instance():
    return TaskInstancePydantic(
        task_id="5",
        dag_id="7",
        run_id="11",
        map_index="13",
        start_date=datetime.now(),
        end_date=datetime.now(),
        execution_date=datetime.now(),
        duration=0.1,
        state="success",
        try_number=1,
        max_tries=4,
        hostname="host",
        unixname="unix",
        job_id=13,
        pool="default",
        pool_slots=1,
        queue="default",
        priority_weight=77,
        operator="DummyOperator",
        custom_operator_name="DummyOperator",
        queued_dttm=datetime.now(),
        queued_by_job_id=3,
        pid=12345,
        executor="default",
        executor_config=None,
        updated_at=datetime.now(),
        rendered_map_index="1",
        external_executor_id="x",
        trigger_id=1,
        trigger_timeout=datetime.now(),
        next_method="bla",
        next_kwargs=None,
        run_as_user=None,
        task=None,
        test_mode=False,
        dag_run=None,
        dag_model=None,
        raw=False,
        is_trigger_log_context=False,
    )


def create_mock_dag():
    for dag_id in itertools.count(1):
        mock_dag = mock.Mock()
        mock_dag.dag_id = dag_id
        yield mock_dag


class TestDatasetManager:
    def test_register_dataset_change_dataset_doesnt_exist(self, mock_task_instance):
        dsem = DatasetManager()

        dataset = Dataset(uri="dataset_doesnt_exist")

        mock_session = mock.Mock()
        # Gotta mock up the query results
        mock_session.scalar.return_value = None

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=dataset, session=mock_session)

        # Ensure that we have ignored the dataset and _not_ created a DatasetEvent or
        # DatasetDagRunQueue rows
        mock_session.add.assert_not_called()
        mock_session.merge.assert_not_called()

    def test_register_dataset_change(self, session, dag_maker, mock_task_instance):
        dsem = DatasetManager()

        ds = Dataset(uri="test_dataset_uri")
        dag1 = DagModel(dag_id="dag1", is_active=True)
        dag2 = DagModel(dag_id="dag2", is_active=True)
        session.add_all([dag1, dag2])

        dsm = DatasetModel(uri="test_dataset_uri")
        session.add(dsm)
        dsm.consuming_dags = [DagScheduleDatasetReference(dag_id=dag.dag_id) for dag in (dag1, dag2)]
        session.execute(delete(DatasetDagRunQueue))
        session.flush()

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=session)
        session.flush()

        # Ensure we've created a dataset
        assert session.query(DatasetEvent).filter_by(dataset_id=dsm.id).count() == 1
        assert session.query(DatasetDagRunQueue).count() == 2

    def test_register_dataset_change_no_downstreams(self, session, mock_task_instance):
        dsem = DatasetManager()

        ds = Dataset(uri="never_consumed")
        dsm = DatasetModel(uri="never_consumed")
        session.add(dsm)
        session.execute(delete(DatasetDagRunQueue))
        session.flush()

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=session)
        session.flush()

        # Ensure we've created a dataset
        assert session.query(DatasetEvent).filter_by(dataset_id=dsm.id).count() == 1
        assert session.query(DatasetDagRunQueue).count() == 0

    @pytest.mark.skip_if_database_isolation_mode
    def test_register_dataset_change_notifies_dataset_listener(self, session, mock_task_instance):
        dsem = DatasetManager()
        dataset_listener.clear()
        get_listener_manager().add_listener(dataset_listener)

        ds = Dataset(uri="test_dataset_uri_2")
        dag1 = DagModel(dag_id="dag3")
        session.add(dag1)

        dsm = DatasetModel(uri="test_dataset_uri_2")
        session.add(dsm)
        dsm.consuming_dags = [DagScheduleDatasetReference(dag_id=dag1.dag_id)]
        session.flush()

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=session)
        session.flush()

        # Ensure the listener was notified
        assert len(dataset_listener.changed) == 1
        assert dataset_listener.changed[0].uri == ds.uri

    @pytest.mark.skip_if_database_isolation_mode
    def test_create_datasets_notifies_dataset_listener(self, session):
        dsem = DatasetManager()
        dataset_listener.clear()
        get_listener_manager().add_listener(dataset_listener)

        ds = Dataset(uri="test_dataset_uri_3")

        dsms = dsem.create_datasets([ds], session=session)

        # Ensure the listener was notified
        assert len(dataset_listener.created) == 1
        assert len(dsms) == 1
        assert dataset_listener.created[0].uri == ds.uri == dsms[0].uri
