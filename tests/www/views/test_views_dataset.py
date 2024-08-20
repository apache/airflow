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

import pendulum
import pytest
from dateutil.tz import UTC

from airflow.assets import Dataset
from airflow.models.dataset import DatasetEvent, DatasetModel
from airflow.operators.empty import EmptyOperator
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.db import clear_db_datasets

pytestmark = pytest.mark.db_test


class TestDatasetEndpoint:
    @pytest.fixture(autouse=True)
    def cleanup(self):
        clear_db_datasets()
        yield
        clear_db_datasets()


class TestGetDatasets(TestDatasetEndpoint):
    def test_should_respond_200(self, admin_client, session):
        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        with assert_queries_count(10):
            response = admin_client.get("/object/datasets_summary")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key/1",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key/2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_400_for_invalid_attr(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        response = admin_client.get("/object/datasets_summary?order_by=fake")

        assert response.status_code == 400
        msg = "Ordering with 'fake' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_order_by_raises_400_for_invalid_datetimes(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        response = admin_client.get("/object/datasets_summary?updated_before=null")

        assert response.status_code == 400
        assert "Invalid datetime:" in response.text

        response = admin_client.get("/object/datasets_summary?updated_after=null")

        assert response.status_code == 400
        assert "Invalid datetime:" in response.text

    def test_filter_by_datetimes(self, admin_client, session):
        today = pendulum.today("UTC")

        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
            )
            for i in range(1, 4)
        ]
        session.add_all(datasets)
        # Update datasets, one per day, starting with datasets[0], ending with datasets[2]
        dataset_events = [
            DatasetEvent(
                dataset_id=datasets[i].id,
                timestamp=today.add(days=-len(datasets) + i + 1),
            )
            for i in range(len(datasets))
        ]
        session.add_all(dataset_events)
        session.commit()
        assert session.query(DatasetModel).count() == len(datasets)

        cutoff = today.add(days=-1).add(minutes=-5).to_iso8601_string()
        response = admin_client.get(f"/object/datasets_summary?updated_after={cutoff}")

        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        assert [json_dict["id"] for json_dict in response.json["datasets"]] == [2, 3]

        cutoff = today.add(days=-1).add(minutes=5).to_iso8601_string()
        response = admin_client.get(f"/object/datasets_summary?updated_before={cutoff}")

        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        assert [json_dict["id"] for json_dict in response.json["datasets"]] == [1, 2]

    @pytest.mark.parametrize(
        "order_by, ordered_dataset_ids",
        [
            ("uri", [1, 2, 3, 4]),
            ("-uri", [4, 3, 2, 1]),
            ("last_dataset_update", [4, 1, 3, 2]),
            ("-last_dataset_update", [2, 3, 1, 4]),
        ],
    )
    def test_order_by(self, admin_client, session, order_by, ordered_dataset_ids):
        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key/{i}",
            )
            for i in range(1, len(ordered_dataset_ids) + 1)
        ]
        session.add_all(datasets)
        dataset_events = [
            DatasetEvent(
                dataset_id=datasets[2].id,
                timestamp=pendulum.today("UTC").add(days=-3),
            ),
            DatasetEvent(
                dataset_id=datasets[1].id,
                timestamp=pendulum.today("UTC").add(days=-2),
            ),
            DatasetEvent(
                dataset_id=datasets[1].id,
                timestamp=pendulum.today("UTC").add(days=-1),
            ),
        ]
        session.add_all(dataset_events)
        session.commit()
        assert session.query(DatasetModel).count() == len(ordered_dataset_ids)

        response = admin_client.get(f"/object/datasets_summary?order_by={order_by}")

        assert response.status_code == 200
        assert ordered_dataset_ids == [json_dict["id"] for json_dict in response.json["datasets"]]
        assert response.json["total_entries"] == len(ordered_dataset_ids)

    def test_search_uri_pattern(self, admin_client, session):
        datasets = [
            DatasetModel(
                id=i,
                uri=f"s3://bucket/key_{i}",
            )
            for i in [1, 2]
        ]
        session.add_all(datasets)
        session.commit()
        assert session.query(DatasetModel).count() == 2

        uri_pattern = "key_2"
        response = admin_client.get(f"/object/datasets_summary?uri_pattern={uri_pattern}")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 2,
                    "uri": "s3://bucket/key_2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 1,
        }

        uri_pattern = "s3://bucket/key_"
        response = admin_client.get(f"/object/datasets_summary?uri_pattern={uri_pattern}")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": 1,
                    "uri": "s3://bucket/key_1",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": 2,
                    "uri": "s3://bucket/key_2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
            ],
            "total_entries": 2,
        }

    @pytest.mark.need_serialized_dag
    def test_correct_counts_update(self, admin_client, session, dag_maker, app, monkeypatch):
        with monkeypatch.context() as m:
            datasets = [Dataset(uri=f"s3://bucket/key/{i}") for i in [1, 2, 3, 4, 5]]

            # DAG that produces dataset #1
            with dag_maker(dag_id="upstream", schedule=None, serialized=True, session=session):
                EmptyOperator(task_id="task1", outlets=[datasets[0]])

            # DAG that is consumes only datasets #1 and #2
            with dag_maker(dag_id="downstream", schedule=datasets[:2], serialized=True, session=session):
                EmptyOperator(task_id="task1")

            # We create multiple dataset-producing and dataset-consuming DAGs because the query requires
            # COUNT(DISTINCT ...) for total_updates, or else it returns a multiple of the correct number due
            # to the outer joins with DagScheduleAssetReference and TaskOutletAssetReference
            # Two independent DAGs that produce dataset #3
            with dag_maker(dag_id="independent_producer_1", serialized=True, session=session):
                EmptyOperator(task_id="task1", outlets=[datasets[2]])
            with dag_maker(dag_id="independent_producer_2", serialized=True, session=session):
                EmptyOperator(task_id="task1", outlets=[datasets[2]])
            # Two independent DAGs that consume dataset #4
            with dag_maker(
                dag_id="independent_consumer_1",
                schedule=[datasets[3]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1")
            with dag_maker(
                dag_id="independent_consumer_2",
                schedule=[datasets[3]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1")

            # Independent DAG that is produces and consumes the same dataset, #5
            with dag_maker(
                dag_id="independent_producer_self_consumer",
                schedule=[datasets[4]],
                serialized=True,
                session=session,
            ):
                EmptyOperator(task_id="task1", outlets=[datasets[4]])

            m.setattr(app, "dag_bag", dag_maker.dagbag)

            ds1_id = session.query(DatasetModel.id).filter_by(uri=datasets[0].uri).scalar()
            ds2_id = session.query(DatasetModel.id).filter_by(uri=datasets[1].uri).scalar()
            ds3_id = session.query(DatasetModel.id).filter_by(uri=datasets[2].uri).scalar()
            ds4_id = session.query(DatasetModel.id).filter_by(uri=datasets[3].uri).scalar()
            ds5_id = session.query(DatasetModel.id).filter_by(uri=datasets[4].uri).scalar()

            # dataset 1 events
            session.add_all(
                [
                    DatasetEvent(
                        dataset_id=ds1_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(3)
                ]
            )
            # dataset 3 events
            session.add_all(
                [
                    DatasetEvent(
                        dataset_id=ds3_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(3)
                ]
            )
            # dataset 4 events
            session.add_all(
                [
                    DatasetEvent(
                        dataset_id=ds4_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(4)
                ]
            )
            # dataset 5 events
            session.add_all(
                [
                    DatasetEvent(
                        dataset_id=ds5_id,
                        timestamp=pendulum.DateTime(2022, 8, 1, i, tzinfo=UTC),
                    )
                    for i in range(5)
                ]
            )
            session.commit()

            response = admin_client.get("/object/datasets_summary")

        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            "datasets": [
                {
                    "id": ds1_id,
                    "uri": "s3://bucket/key/1",
                    "last_dataset_update": "2022-08-01T02:00:00+00:00",
                    "total_updates": 3,
                },
                {
                    "id": ds2_id,
                    "uri": "s3://bucket/key/2",
                    "last_dataset_update": None,
                    "total_updates": 0,
                },
                {
                    "id": ds3_id,
                    "uri": "s3://bucket/key/3",
                    "last_dataset_update": "2022-08-01T02:00:00+00:00",
                    "total_updates": 3,
                },
                {
                    "id": ds4_id,
                    "uri": "s3://bucket/key/4",
                    "last_dataset_update": "2022-08-01T03:00:00+00:00",
                    "total_updates": 4,
                },
                {
                    "id": ds5_id,
                    "uri": "s3://bucket/key/5",
                    "last_dataset_update": "2022-08-01T04:00:00+00:00",
                    "total_updates": 5,
                },
            ],
            "total_entries": 5,
        }


class TestGetDatasetsEndpointPagination(TestDatasetEndpoint):
    @pytest.mark.parametrize(
        "url, expected_dataset_uris",
        [
            # Limit test data
            ("/object/datasets_summary?limit=1", ["s3://bucket/key/1"]),
            ("/object/datasets_summary?limit=5", [f"s3://bucket/key/{i}" for i in range(1, 6)]),
            # Offset test data
            ("/object/datasets_summary?offset=1", [f"s3://bucket/key/{i}" for i in range(2, 10)]),
            ("/object/datasets_summary?offset=3", [f"s3://bucket/key/{i}" for i in range(4, 10)]),
            # Limit and offset test data
            ("/object/datasets_summary?offset=3&limit=3", [f"s3://bucket/key/{i}" for i in [4, 5, 6]]),
        ],
    )
    def test_limit_and_offset(self, admin_client, session, url, expected_dataset_uris):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
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
            )
            for i in range(1, 60)
        ]
        session.add_all(datasets)
        session.commit()

        response = admin_client.get("/object/datasets_summary")

        assert response.status_code == 200
        assert len(response.json["datasets"]) == 25

    def test_should_return_max_if_req_above(self, admin_client, session):
        datasets = [
            DatasetModel(
                uri=f"s3://bucket/key/{i}",
                extra={"foo": "bar"},
            )
            for i in range(1, 60)
        ]
        session.add_all(datasets)
        session.commit()

        response = admin_client.get("/object/datasets_summary?limit=180")

        assert response.status_code == 200
        assert len(response.json["datasets"]) == 50


class TestGetDatasetNextRunSummary(TestDatasetEndpoint):
    def test_next_run_dataset_summary(self, dag_maker, admin_client):
        with dag_maker(dag_id="upstream", schedule=[Dataset(uri="s3://bucket/key/1")], serialized=True):
            EmptyOperator(task_id="task1")

        response = admin_client.post("/next_run_datasets_summary", data={"dag_ids": ["upstream"]})

        assert response.status_code == 200
        assert response.json == {"upstream": {"ready": 0, "total": 1, "uri": "s3://bucket/key/1"}}
