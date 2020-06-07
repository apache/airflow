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
    xcom_collection_item_schema, xcom_collection_schema, xcom_schema,
)
from airflow.models import XCom
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session


class TestXComCollectionItemSchema(unittest.TestCase):

    def setUp(self) -> None:
        self.now = timezone.utcnow()
        with create_session() as session:
            session.query(XCom).delete()

    @provide_session
    def test_serialize(self, session):
        xcom_model = XCom(
            key='test_key',
            timestamp=self.now,
            execution_date=self.now,
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
                'timestamp': self.now.isoformat(),
                'execution_date': self.now.isoformat(),
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
            }
        )

    @provide_session
    def test_deserialize(self, session):
        xcom_dump = {
            'key': 'test_key',
            'timestamp': self.now.isoformat(),
            'execution_date': self.now.isoformat(),
            'task_id': 'test_task_id',
            'dag_id': 'test_dag',
        }
        result = xcom_collection_item_schema.load(xcom_dump)
        self.assertEqual(
            result[0],
            {
                'key': 'test_key',
                'timestamp': self.now,
                'execution_date': self.now,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
            }
        )


class TestXComCollectionSchema(unittest.TestCase):

    def setUp(self) -> None:
        self.t1 = timezone.utcnow()
        self.t2 = timezone.utcnow()
        with create_session() as session:
            session.query(XCom).delete()

    @provide_session
    def test_serialize(self, session):
        xcom_model_1 = XCom(
            key='test_key_1',
            timestamp=self.t1,
            execution_date=self.t1,
            task_id='test_task_id_1',
            dag_id='test_dag_1',
        )
        xcom_model_2 = XCom(
            key='test_key_2',
            timestamp=self.t2,
            execution_date=self.t2,
            task_id='test_task_id_2',
            dag_id='test_dag_2',
        )
        xcom_models = [xcom_model_1, xcom_model_2]
        session.add_all(xcom_models)
        session.commit()
        xcom_models_queried = session.query(XCom).filter(
            or_(XCom.execution_date == self.t1, XCom.execution_date == self.t2)
        ).all()
        deserialized_xcoms = xcom_collection_schema.dump(xcom_models_queried)
        self.assertEqual(
            deserialized_xcoms[0],
            {
                'xcom_entries': [
                    {
                        'key': 'test_key_1',
                        'timestamp': self.t1.isoformat(),
                        'execution_date': self.t1.isoformat(),
                        'task_id': 'test_task_id_1',
                        'dag_id': 'test_dag_1',
                    },
                    {
                        'key': 'test_key_2',
                        'timestamp': self.t2.isoformat(),
                        'execution_date': self.t2.isoformat(),
                        'task_id': 'test_task_id_2',
                        'dag_id': 'test_dag_2',
                    }
                ],
                'total_entries': 2
            }
        )


class TestXComSchema(unittest.TestCase):

    def setUp(self) -> None:
        self.now = timezone.utcnow()
        with create_session() as session:
            session.query(XCom).delete()

    @provide_session
    def test_serialize(self, session):
        xcom_model = XCom(
            key='test_key',
            timestamp=self.now,
            execution_date=self.now,
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
                'timestamp': self.now.isoformat(),
                'execution_date': self.now.isoformat(),
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
                'value': 'test_binary',
            }
        )

    @provide_session
    def test_deserialize(self, session):
        xcom_dump = {
            'key': 'test_key',
            'timestamp': self.now.isoformat(),
            'execution_date': self.now.isoformat(),
            'task_id': 'test_task_id',
            'dag_id': 'test_dag',
            'value': b'test_binary',
        }
        result = xcom_schema.load(xcom_dump)
        self.assertEqual(
            result[0],
            {
                'key': 'test_key',
                'timestamp': self.now,
                'execution_date': self.now,
                'task_id': 'test_task_id',
                'dag_id': 'test_dag',
                'value': 'test_binary',
            }
        )
