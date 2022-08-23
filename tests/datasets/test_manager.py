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

from unittest import mock

import pytest

from airflow.datasets import Dataset
from airflow.datasets.manager import DatasetEventManager
from airflow.models.dataset import DatasetModel


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


class TestDatasetEventManager:
    def test_register_dataset_change_dataset_doesnt_exist(self, mock_task_instance):
        dsem = DatasetEventManager()

        dataset = Dataset(uri="dataset_doesnt_exist")

        mock_session = mock.Mock()
        # Gotta mock up the query results
        mock_session.query.return_value.filter.return_value.one_or_none.return_value = None

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=dataset, session=mock_session)

        # Ensure that we have ignored the dataset and _not_ created a DatasetEvent or
        # DatasetDagRunQueue rows
        mock_session.add.assert_not_called()
        mock_session.merge.assert_not_called()

    def test_register_dataset_change(self, mock_task_instance):
        dsem = DatasetEventManager()

        mock_dag_1 = mock.MagicMock()
        mock_dag_1.dag_id = 1
        mock_dag_2 = mock.MagicMock()
        mock_dag_2.dag_id = 2

        ds = Dataset(uri="test_dataset_uri")

        dsm = DatasetModel(uri="test_dataset_uri")
        dsm.consuming_dags = [mock_dag_1, mock_dag_2]

        mock_session = mock.Mock()
        # Gotta mock up the query results
        mock_session.query.return_value.filter.return_value.one_or_none.return_value = dsm

        dsem.register_dataset_change(task_instance=mock_task_instance, dataset=ds, session=mock_session)

        # Ensure we've created a dataset
        mock_session.add.assert_called_once()
        # Ensure that we've created DatasetDagRunQueue rows
        assert mock_session.merge.call_count == 2
