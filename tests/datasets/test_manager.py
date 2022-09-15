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

from unittest import mock

import pytest

from airflow.datasets import Dataset
from airflow.datasets.manager import DatasetManager
from airflow.models.dag import DagModel
from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue, DatasetEvent, DatasetModel


@pytest.fixture()
def mock_task_instance():
    mock_ti = mock.Mock()
    mock_ti.task_id = "5"
    mock_ti.dag_id = "7"
    mock_ti.run_id = "11"
    mock_ti.map_index = "13"
    return mock_ti


def create_mock_dag():
    n = 1
    while True:
        mock_dag = mock.Mock()
        mock_dag.dag_id = n
        n += 1
        yield mock_dag


class TestDatasetManager:
    def test_register_dataset_change_dataset_doesnt_exist(self, mock_task_instance):
        dsem = DatasetManager()

        dataset = Dataset(uri="dataset_doesnt_exist")

        mock_session = mock.Mock()
        # Gotta mock up the query results
        mock_session.query.return_value.filter.return_value.one_or_none.return_value = None

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=dataset, session=mock_session)

        # Ensure that we have ignored the dataset and _not_ created a DatasetEvent or
        # DatasetDagRunQueue rows
        mock_session.add.assert_not_called()
        mock_session.merge.assert_not_called()

    def test_register_dataset_change(self, session, dag_maker, mock_task_instance):
        dsem = DatasetManager()

        ds = Dataset(uri="test_dataset_uri")
        dag1 = DagModel(dag_id="dag1")
        dag2 = DagModel(dag_id="dag2")
        session.add_all([dag1, dag2])

        dsm = DatasetModel(uri="test_dataset_uri")
        session.add(dsm)
        dsm.consuming_dags = [DagScheduleDatasetReference(dag_id=dag.dag_id) for dag in (dag1, dag2)]
        session.flush()

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=session)

        # Ensure we've created a dataset
        assert session.query(DatasetEvent).filter_by(dataset_id=dsm.id).count() == 1
        assert session.query(DatasetDagRunQueue).count() == 2

    def test_register_dataset_change_no_downstreams(self, session, mock_task_instance):
        dsem = DatasetManager()

        ds = Dataset(uri="never_consumed")
        dsm = DatasetModel(uri="never_consumed")
        session.add(dsm)
        session.flush()

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=session)

        # Ensure we've created a dataset
        assert session.query(DatasetEvent).filter_by(dataset_id=dsm.id).count() == 1
        assert session.query(DatasetDagRunQueue).count() == 0
