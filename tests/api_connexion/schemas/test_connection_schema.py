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

import re

import marshmallow
import pytest

from airflow.api_connexion.schemas.connection_schema import (
    ConnectionCollection,
    connection_collection_item_schema,
    connection_collection_schema,
    connection_schema,
    connection_test_schema,
)
from airflow.models import Connection
from airflow.utils.session import create_session, provide_session
from tests.test_utils.db import clear_db_connections


class TestConnectionCollectionItemSchema:
    def setup_method(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def teardown_method(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialize(self, session):
        connection_model = Connection(
            conn_id='mysql_default',
            conn_type='mysql',
            host='mysql',
            login='login',
            schema='testschema',
            port=80,
        )
        session.add(connection_model)
        session.commit()
        connection_model = session.query(Connection).first()
        deserialized_connection = connection_collection_item_schema.dump(connection_model)
        assert deserialized_connection == {
            'connection_id': "mysql_default",
            'conn_type': 'mysql',
            'description': None,
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
        }

    def test_deserialize(self):
        connection_dump_1 = {
            'connection_id': "mysql_default_1",
            'conn_type': 'mysql',
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
        }
        connection_dump_2 = {
            'connection_id': "mysql_default_2",
            'conn_type': "postgres",
        }
        result_1 = connection_collection_item_schema.load(connection_dump_1)
        result_2 = connection_collection_item_schema.load(connection_dump_2)

        assert result_1 == {
            'conn_id': "mysql_default_1",
            'conn_type': 'mysql',
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
        }
        assert result_2 == {
            'conn_id': "mysql_default_2",
            'conn_type': "postgres",
        }

    def test_deserialize_required_fields(self):
        connection_dump_1 = {
            'connection_id': "mysql_default_2",
        }
        with pytest.raises(
            marshmallow.exceptions.ValidationError,
            match=re.escape("{'conn_type': ['Missing data for required field.']}"),
        ):
            connection_collection_item_schema.load(connection_dump_1)


class TestConnectionCollectionSchema:
    def setup_method(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def teardown_method(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialize(self, session):
        connection_model_1 = Connection(conn_id='mysql_default_1', conn_type='test-type')
        connection_model_2 = Connection(conn_id='mysql_default_2', conn_type='test-type2')
        connections = [connection_model_1, connection_model_2]
        session.add_all(connections)
        session.commit()
        instance = ConnectionCollection(connections=connections, total_entries=2)
        deserialized_connections = connection_collection_schema.dump(instance)
        assert deserialized_connections == {
            'connections': [
                {
                    "connection_id": "mysql_default_1",
                    "conn_type": "test-type",
                    "description": None,
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
                {
                    "connection_id": "mysql_default_2",
                    "conn_type": "test-type2",
                    "description": None,
                    "host": None,
                    "login": None,
                    'schema': None,
                    'port': None,
                },
            ],
            'total_entries': 2,
        }


class TestConnectionSchema:
    def setup_method(self) -> None:
        with create_session() as session:
            session.query(Connection).delete()

    def teardown_method(self) -> None:
        clear_db_connections()

    @provide_session
    def test_serialize(self, session):
        connection_model = Connection(
            conn_id='mysql_default',
            conn_type='mysql',
            host='mysql',
            login='login',
            schema='testschema',
            port=80,
            password='test-password',
            extra="{'key':'string'}",
        )
        session.add(connection_model)
        session.commit()
        connection_model = session.query(Connection).first()
        deserialized_connection = connection_schema.dump(connection_model)
        assert deserialized_connection == {
            'connection_id': "mysql_default",
            'conn_type': 'mysql',
            'description': None,
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
            'extra': "{'key':'string'}",
        }

    def test_deserialize(self):
        den = {
            'connection_id': "mysql_default",
            'conn_type': 'mysql',
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
            'extra': "{'key':'string'}",
        }
        result = connection_schema.load(den)
        assert result == {
            'conn_id': "mysql_default",
            'conn_type': 'mysql',
            'host': 'mysql',
            'login': 'login',
            'schema': 'testschema',
            'port': 80,
            'extra': "{'key':'string'}",
        }


class TestConnectionTestSchema:
    def test_response(self):
        data = {
            'status': True,
            'message': 'Connection tested successful',
        }
        result = connection_test_schema.load(data)
        assert result == {
            'status': True,
            'message': 'Connection tested successful',
        }
