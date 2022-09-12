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

import string

import pendulum
import pytest
from dateutil.tz import UTC

from airflow import Dataset
from airflow.models.dataset import DatasetEvent, DatasetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_datasets


class TestDatasetEndpoint:

    default_time = "2020-06-11T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def cleanup(self):
        clear_db_datasets()
        yield
        # clear_db_datasets()


class TestGetDatasets(TestDatasetEndpoint):
    def test_should_respond_200(self, admin_client, session):
        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        with assert_queries_count(8):
            response = admin_client.get("/object/list_datasets")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "last_dataset_update": None,
                    "total_updates": 0,
                    "producing_task_count": 0,
                    "consuming_dag_count": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                    "producing_task_count": 0,
                    "consuming_dag_count": 0,
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        response = admin_client.get("/object/list_datasets?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg

    @pytest.mark.parametrize(
        "order_by, ordered_dataset_ids",
        [
            ("uri", [1, 2, 3, 4]),
            ("-uri", [4, 3, 2, 1]),
            ("last_dataset_update", [3, 2, 4, 1]),
            ("-last_dataset_update", [1, 4, 2, 3]),
        ],
    )
    def test_order_by(self, admin_client, session, order_by, ordered_dataset_ids):
        datasets = [
            DatasetModel(
                id=i,
                uri=string.ascii_lowercase[i],
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, len(ordered_dataset_ids) + 1)
        ]
        session.add_all(datasets)
        dataset_events = [
            DatasetEvent(
                dataset_id=datasets[2].id,
                timestamp=pendulum.today('UTC').add(days=-3),
            ),
            DatasetEvent(
                dataset_id=datasets[1].id,
                timestamp=pendulum.today('UTC').add(days=-2),
            ),
            DatasetEvent(
                dataset_id=datasets[1].id,
                timestamp=pendulum.today('UTC').add(days=-1),
            ),
        ]
        session.add_all(dataset_events)
        session.commit()
        assert session.query(DatasetModel).count() == len(ordered_dataset_ids)

        response = admin_client.get(f"/object/list_datasets?order_by={order_by}")

        assert response.status_code == 200
        assert ordered_dataset_ids == [json_dict['id'] for json_dict in response.json['datasets']]
        assert response.json['total_entries'] == len(ordered_dataset_ids)

    @pytest.mark.need_serialized_dag
    def test_correct_counts_update(self, admin_client, session, dag_maker, app, monkeypatch):
        with monkeypatch.context() as m:
            datasets = [Dataset(uri=f"s3://bucket/key/{i}") for i in [1, 2]]

            with dag_maker(dag_id='downstream', schedule=datasets, serialized=True, session=session):
                EmptyOperator(task_id='task1')

            with dag_maker(dag_id='upstream', schedule=None, serialized=True, session=session):
                EmptyOperator(task_id='task1', outlets=[datasets[0]])

            m.setattr(app, 'dag_bag', dag_maker.dagbag)

            ds1_id = session.query(DatasetModel.id).filter_by(uri=datasets[0].uri).scalar()
            ds2_id = session.query(DatasetModel.id).filter_by(uri=datasets[1].uri).scalar()

            session.add(
                DatasetEvent(
                    dataset_id=ds1_id,
                    timestamp=pendulum.DateTime(2022, 8, 1, tzinfo=UTC),
                )
            )
            session.add(
                DatasetEvent(
                    dataset_id=ds1_id,
                    timestamp=pendulum.DateTime(2022, 8, 1, tzinfo=UTC),
                )
            )
            session.add(
                DatasetEvent(
                    dataset_id=ds1_id,
                    timestamp=pendulum.DateTime(2022, 8, 1, tzinfo=UTC),
                )
            )
            session.commit()

            response = admin_client.get("/object/list_datasets")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": ds1_id,
                    "uri": "s3://bucket/key/1",
                    "last_dataset_update": "2022-08-01T00:00:00+00:00",
                    "total_updates": 3,
                    "producing_task_count": 1,
                    "consuming_dag_count": 1,
                },
                {
                    "id": ds2_id,
                    "uri": "s3://bucket/key/2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                    "producing_task_count": 0,
                    "consuming_dag_count": 1,
                },
            ],
            "total_entries": 2,
        }


class TestGetDatasetsEndpointPagination(TestDatasetEndpoint):
    @pytest.mark.parametrize(
        "url, expected_dataset_uris",
        [
            # Limit test data
            ("/object/list_datasets?limit=1", ["s3://bucket/key/1"]),
            ("/object/list_datasets?limit=5", [f"s3://bucket/key/{i}" for i in range(1, 6)]),
            # Offset test data
            ("/object/list_datasets?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 10)]),
            ("/object/list_datasets?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 10)]),
            # Limit and offset test data
            ("/object/list_datasets?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    def test_limit_and_offset(self, admin_client, session, url, expected_dataset_uris):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 10)
        ]
        session.add_all(datasets)
        session.commit()

        response = admin_client.get(url)

        assert response.status_code == 200
        dataset_uris = [dataset["uri"] for dataset in response.json["datasets"]]
        assert dataset_uris == expected_dataset_uris

    def test_should_respect_page_size_limit_default(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 60)
        ]
        session.add_all(datasets)
        session.commit()

        response = admin_client.get("/object/list_datasets")

        assert response.status_code == 200
        assert len(response.json['datasets']) == 25

    def test_should_return_max_if_req_above(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
                created_at=timezone.parse(self.default_time),
                updated_at=timezone.parse(self.default_time),
            )
            for i in range(1, 60)
        ]
        session.add_all(datasets)
        session.commit()

        response = admin_client.get("/object/list_datasets?limit=180")

        assert response.status_code == 200
        assert len(response.json['datasets']) == 50
