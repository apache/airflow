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
import unittest

from sqlalchemy import or_

from airflow.api_connexion.schemas.xcom_schema import (
    XComCollection, xcom_collection_item_schema, xcom_collection_schema, xcom_schema,
)
from airflow.models import XCom
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session, provide_session


class TestXComSchemaBase(unittest.TestCase):

    def setUp(self):
        """
        Clear Hanging XComs pre test
        """
        with create_session() as session:
            session.query(XCom).delete()

    def tearDown(self) -> None:
        """
        Clear Hanging XComs post test
        """
        with create_session() as session:
            session.query(XCom).delete()


class TestXComCollectionItemSchema(TestXComSchemaBase):

    def setUp(self) -> None:
        super().setUp()
        self.default_time = '2005-04-02T21:00:00+00:00'
        self.default_time_parsed = parse_execution_date(self.default_time)

    @provide_session
    def test_serialize(self, session):
        xcom_model = XCom(
            key='test_key',
            timestamp=self.default_time_parsed,
            execution_date=self.default_time_parsed,
            task_id='test_task_id',
            dag_id='test_dag',
        )
        session.add(xcom_model)
        session.commit()
        xcom_model = session.query(XCom).first()
        deserialized_xcom = xcom_collection_item_schema.dump(xcom_model)
        self.assertEqual(
            deserialized_xcom[0],
            {
                'key': 'test_key',
                'timestamp': self.default_time,
                'execution_date': self.default_time,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
            }
        )

    def test_deserialize(self):
        xcom_dump = {
            'key': 'test_key',
            'timestamp': self.default_time,
            'execution_date': self.default_time,
            'task_id': 'test_task_id',
            'dag_id': 'test_dag',
        }
        result = xcom_collection_item_schema.load(xcom_dump)
        self.assertEqual(
            result[0],
            {
                'key': 'test_key',
                'timestamp': self.default_time_parsed,
                'execution_date': self.default_time_parsed,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
            }
        )


class TestXComCollectionSchema(TestXComSchemaBase):

    def setUp(self) -> None:
        super().setUp()
        self.default_time_1 = '2005-04-02T21:00:00+00:00'
        self.default_time_2 = '2005-04-02T21:01:00+00:00'
        self.time_1 = parse_execution_date(self.default_time_1)
        self.time_2 = parse_execution_date(self.default_time_2)

    @provide_session
    def test_serialize(self, session):
        xcom_model_1 = XCom(
            key='test_key_1',
            timestamp=self.time_1,
            execution_date=self.time_1,
            task_id='test_task_id_1',
            dag_id='test_dag_1',
        )
        xcom_model_2 = XCom(
            key='test_key_2',
            timestamp=self.time_2,
            execution_date=self.time_2,
            task_id='test_task_id_2',
            dag_id='test_dag_2',
        )
        xcom_models = [xcom_model_1, xcom_model_2]
        session.add_all(xcom_models)
        session.commit()
        xcom_models_query = session.query(XCom).filter(
            or_(XCom.execution_date == self.time_1, XCom.execution_date == self.time_2)
        )
        xcom_models_queried = xcom_models_query.all()
        deserialized_xcoms = xcom_collection_schema.dump(XComCollection(
            xcom_entries=xcom_models_queried,
            total_entries=xcom_models_query.count(),
        ))
        self.assertEqual(
            deserialized_xcoms[0],
            {
                'xcom_entries': [
                    {
                        'key': 'test_key_1',
                        'timestamp': self.default_time_1,
                        'execution_date': self.default_time_1,
                        'task_id': 'test_task_id_1',
                        'dag_id': 'test_dag_1',
                    },
                    {
                        'key': 'test_key_2',
                        'timestamp': self.default_time_2,
                        'execution_date': self.default_time_2,
                        'task_id': 'test_task_id_2',
                        'dag_id': 'test_dag_2',
                    }
                ],
                'total_entries': len(xcom_models),
            }
        )


class TestXComSchema(TestXComSchemaBase):

    def setUp(self) -> None:
        super().setUp()
        self.default_time = '2005-04-02T21:00:00+00:00'
        self.default_time_parsed = parse_execution_date(self.default_time)

    @provide_session
    def test_serialize(self, session):
        xcom_model = XCom(
            key='test_key',
            timestamp=self.default_time_parsed,
            execution_date=self.default_time_parsed,
            task_id='test_task_id',
            dag_id='test_dag',
            value=b'test_binary',
        )
        session.add(xcom_model)
        session.commit()
        xcom_model = session.query(XCom).first()
        deserialized_xcom = xcom_schema.dump(xcom_model)
        self.assertEqual(
            deserialized_xcom[0],
            {
                'key': 'test_key',
                'timestamp': self.default_time,
                'execution_date': self.default_time,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
                'value': 'test_binary',
            }
        )

    def test_deserialize(self):
        xcom_dump = {
            'key': 'test_key',
            'timestamp': self.default_time,
            'execution_date': self.default_time,
            'task_id': 'test_task_id',
            'dag_id': 'test_dag',
            'value': b'test_binary',
        }
        result = xcom_schema.load(xcom_dump)
        self.assertEqual(
            result[0],
            {
                'key': 'test_key',
                'timestamp': self.default_time_parsed,
                'execution_date': self.default_time_parsed,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
                'value': 'test_binary',
            }
        )
