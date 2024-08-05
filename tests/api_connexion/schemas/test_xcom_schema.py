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

import pickle

import pytest
from sqlalchemy import or_, select

from airflow.api_connexion.schemas.xcom_schema import (
    XComCollection,
    xcom_collection_item_schema,
    xcom_collection_schema,
    xcom_schema_string,
)
from airflow.models import DagRun, XCom
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session
from tests.test_utils.config import conf_vars

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module", autouse=True)
def clean_xcom():
    """Ensure there's no XCom littered by other modules."""
    with create_session() as session:
        session.query(XCom).delete()


def _compare_xcom_collections(collection1: dict, collection_2: dict):
    assert collection1.get("total_entries") == collection_2.get("total_entries")

    def sort_key(record):
        return (
            record.get("dag_id"),
            record.get("task_id"),
            record.get("execution_date"),
            record.get("map_index"),
            record.get("key"),
        )

    assert sorted(collection1.get("xcom_entries", []), key=sort_key) == sorted(
        collection_2.get("xcom_entries", []), key=sort_key
    )


@pytest.fixture
def create_xcom(create_task_instance, session):
    def maker(dag_id, task_id, execution_date, key, map_index=-1, value=None):
        ti = create_task_instance(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date,
            map_index=map_index,
            session=session,
        )
        run: DagRun = ti.dag_run
        xcom = XCom(
            dag_run_id=run.id,
            task_id=ti.task_id,
            map_index=map_index,
            key=key,
            value=value,
            timestamp=run.execution_date,
            dag_id=run.dag_id,
            run_id=run.run_id,
        )
        session.add(xcom)
        session.commit()
        return xcom

    return maker


class TestXComCollectionItemSchema:
    default_time = "2016-04-02T21:00:00+00:00"
    default_time_parsed = parse_execution_date(default_time)

    def test_serialize(self, create_xcom, session):
        create_xcom(
            dag_id="test_dag",
            task_id="test_task_id",
            execution_date=self.default_time_parsed,
            key="test_key",
        )
        xcom_model = session.query(XCom).first()
        deserialized_xcom = xcom_collection_item_schema.dump(xcom_model)
        assert deserialized_xcom == {
            "key": "test_key",
            "timestamp": self.default_time,
            "execution_date": self.default_time,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "map_index": -1,
        }

    def test_deserialize(self):
        xcom_dump = {
            "key": "test_key",
            "timestamp": self.default_time,
            "execution_date": self.default_time,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "map_index": 2,
        }
        result = xcom_collection_item_schema.load(xcom_dump)
        assert result == {
            "key": "test_key",
            "timestamp": self.default_time_parsed,
            "execution_date": self.default_time_parsed,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "map_index": 2,
        }


class TestXComCollectionSchema:
    default_time_1 = "2016-04-02T21:00:00+00:00"
    default_time_2 = "2016-04-02T21:01:00+00:00"
    time_1 = parse_execution_date(default_time_1)
    time_2 = parse_execution_date(default_time_2)

    def test_serialize(self, create_xcom, session):
        create_xcom(
            dag_id="test_dag_1",
            task_id="test_task_id_1",
            execution_date=self.time_1,
            key="test_key_1",
        )
        create_xcom(
            dag_id="test_dag_2",
            task_id="test_task_id_2",
            execution_date=self.time_2,
            key="test_key_2",
        )
        xcom_models = session.scalars(
            select(XCom)
            .where(or_(XCom.execution_date == self.time_1, XCom.execution_date == self.time_2))
            .order_by(XCom.dag_run_id)
        ).all()
        deserialized_xcoms = xcom_collection_schema.dump(
            XComCollection(
                xcom_entries=xcom_models,
                total_entries=len(xcom_models),
            )
        )
        _compare_xcom_collections(
            deserialized_xcoms,
            {
                "xcom_entries": [
                    {
                        "key": "test_key_1",
                        "timestamp": self.default_time_1,
                        "execution_date": self.default_time_1,
                        "task_id": "test_task_id_1",
                        "dag_id": "test_dag_1",
                        "map_index": -1,
                    },
                    {
                        "key": "test_key_2",
                        "timestamp": self.default_time_2,
                        "execution_date": self.default_time_2,
                        "task_id": "test_task_id_2",
                        "dag_id": "test_dag_2",
                        "map_index": -1,
                    },
                ],
                "total_entries": 2,
            },
        )


class TestXComSchema:
    default_time = "2016-04-02T21:00:00+00:00"
    default_time_parsed = parse_execution_date(default_time)

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_serialize(self, create_xcom, session):
        create_xcom(
            dag_id="test_dag",
            task_id="test_task_id",
            execution_date=self.default_time_parsed,
            key="test_key",
            value=pickle.dumps(b"test_binary"),
        )
        xcom_model = session.query(XCom).first()
        deserialized_xcom = xcom_schema_string.dump(xcom_model)
        assert deserialized_xcom == {
            "key": "test_key",
            "timestamp": self.default_time,
            "execution_date": self.default_time,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "value": "test_binary",
            "map_index": -1,
        }

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_deserialize(self):
        xcom_dump = {
            "key": "test_key",
            "timestamp": self.default_time,
            "execution_date": self.default_time,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "value": b"test_binary",
        }
        result = xcom_schema_string.load(xcom_dump)
        assert result == {
            "key": "test_key",
            "timestamp": self.default_time_parsed,
            "execution_date": self.default_time_parsed,
            "task_id": "test_task_id",
            "dag_id": "test_dag",
            "value": "test_binary",
        }
