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
import uuid

from airflow.api_connexion.schemas.xcom_schema import (
    XComCollection,
    xcom_collection_item_schema,
    xcom_collection_schema,
    xcom_schema,
)
from airflow.models import XCom
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session, provide_session


class MockXcomCollectionItem:
    DEFAULT_TIME = '2005-04-02T21:00:00+00:00'
    DEFAULT_TIME_PARSED = parse_execution_date(DEFAULT_TIME)

    def __init__(self):
        self.key: str = f'test_key_{self.example_ref}'
        self.timestamp: datetime.datetime = self.DEFAULT_TIME_PARSED
        self.execution_date: datetime.datetime = self.DEFAULT_TIME_PARSED
        self.task_id = f'test_task_id_{self.example_ref}'
        self.dag_id = f'test_dag_{self.example_ref}'

    @property
    def example_ref(self):
        return uuid.uuid4()

    @property
    def as_xcom(self):
        return XCom(
            key=self.key,
            timestamp=self.timestamp,
            execution_date=self.execution_date,
            task_id=self.task_id,
            dag_id=self.dag_id,
        )

    @property
    def deserialized_xcom(self):
        return {
            'key': self.key,
            'timestamp': self.DEFAULT_TIME,
            'execution_date': self.DEFAULT_TIME,
            'task_id': self.task_id,
            'dag_id': self.dag_id,
        }

    @property
    def from_dump_xcom(self):
        return {
            'key': self.key,
            'timestamp': self.DEFAULT_TIME_PARSED,
            'execution_date': self.DEFAULT_TIME_PARSED,
            'task_id': self.task_id,
            'dag_id': self.dag_id,
        }


class MockXcomSchema(MockXcomCollectionItem):
    def __init__(self):
        super().__init__()
        self.value = f'test_binary_{self.example_ref}'

    @property
    def as_xcom(self):
        return XCom(
            key=self.key,
            timestamp=self.timestamp,
            execution_date=self.execution_date,
            task_id=self.task_id,
            dag_id=self.dag_id,
            value=self.value.encode('ascii'),
        )

    @property
    def deserialized_xcom(self):
        return {
            'key': self.key,
            'timestamp': self.DEFAULT_TIME,
            'execution_date': self.DEFAULT_TIME,
            'task_id': self.task_id,
            'dag_id': self.dag_id,
            'value': self.value,
        }

    @property
    def from_dump_xcom(self):
        return {
            'key': self.key,
            'timestamp': self.DEFAULT_TIME_PARSED,
            'execution_date': self.DEFAULT_TIME_PARSED,
            'task_id': self.task_id,
            'dag_id': self.dag_id,
            'value': self.value,
        }


class TestXComSchemaBase(unittest.TestCase):
    def setUp(self):
        self.clear_db()

    def tearDown(self) -> None:
        self.clear_db()

    @staticmethod
    def clear_db():
        """
        Clear Hanging XComs pre test
        """
        with create_session() as session:
            session.query(XCom).delete()


class TestXComCollectionItemSchema(TestXComSchemaBase):
    def setUp(self):
        super().setUp()
        self.fake_xcom = MockXcomCollectionItem()
        self.xcom_model = self.fake_xcom.as_xcom

    @provide_session
    def test_serialize(self, session):
        session.add(self.xcom_model)
        session.commit()
        query_results = session.query(XCom).first()
        actual = xcom_collection_item_schema.dump(query_results)
        expected = self.fake_xcom.deserialized_xcom
        assert actual == expected

    def test_deserialize(self):
        xcom_dump = self.fake_xcom.deserialized_xcom
        actual = xcom_collection_item_schema.load(xcom_dump)
        expected = self.fake_xcom.from_dump_xcom
        assert actual == expected


class TestXComCollectionSchema(TestXComSchemaBase):
    @provide_session
    def test_serialize(self, session):
        xcoms = [MockXcomCollectionItem() for _ in range(2)]
        xcom_models = [xcom.as_xcom for xcom in xcoms]
        session.add_all(xcom_models)
        session.commit()
        xcom_models_queried = session.query(XCom).all()
        actual = xcom_collection_schema.dump(
            XComCollection(
                xcom_entries=xcom_models_queried,
                total_entries=session.query(XCom).count(),
            )
        )
        expected = {
            'xcom_entries': [xcom.deserialized_xcom for xcom in xcoms],
            'total_entries': len(xcoms),
        }
        assert actual == expected


class TestXComSchema(TestXComSchemaBase):
    def setUp(self) -> None:
        super().setUp()

    @provide_session
    def test_serialize(self, session):
        fake_xcom = MockXcomSchema()
        xcom_model = fake_xcom.as_xcom
        session.add(xcom_model)
        session.commit()
        query_result = session.query(XCom).first()
        actual = xcom_schema.dump(query_result)
        expected = fake_xcom.deserialized_xcom
        assert actual == expected

    def test_deserialize(self):
        fake_xcom = MockXcomSchema()
        xcom_dump = fake_xcom.deserialized_xcom
        actual = xcom_schema.load(xcom_dump)
        expected = fake_xcom.from_dump_xcom
        assert actual == expected
