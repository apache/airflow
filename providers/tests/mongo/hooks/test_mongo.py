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

import importlib
import warnings
from typing import TYPE_CHECKING

import pymongo
import pytest

from airflow.exceptions import AirflowConfigException, AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.mongo.hooks.mongo import MongoHook

from tests_common.test_utils.compat import connection_as_json

pytestmark = pytest.mark.db_test

if TYPE_CHECKING:
    from types import ModuleType

mongomock: ModuleType | None

try:
    mongomock = importlib.import_module("mongomock")
except ImportError:
    mongomock = None


@pytest.fixture(scope="module", autouse=True)
def mongo_connections():
    """Create MongoDB connections which use for testing purpose."""
    connections = [
        Connection(conn_id="mongo_default", conn_type="mongo", host="mongo", port=27017),
        Connection(
            conn_id="mongo_default_with_srv",
            conn_type="mongo",
            host="mongo",
            extra='{"srv": true}',
        ),
        Connection(conn_id="mongo_invalid_conn_type", conn_type="mongodb", host="mongo", port=27017),
        Connection(conn_id="mongo_invalid_conn_type_srv", conn_type="mongodb+srv", host="mongo", port=27017),
        Connection(
            conn_id="mongo_invalid_srv_with_port",
            conn_type="mongo",
            host="mongo",
            port=27017,
            extra='{"srv": true}',
        ),
        # Mongo establishes connection during initialization, so we need to have this connection
        Connection(conn_id="fake_connection", conn_type="mongo", host="mongo", port=27017),
    ]

    with pytest.MonkeyPatch.context() as mp:
        for conn in connections:
            mp.setenv(f"AIRFLOW_CONN_{conn.conn_id.upper()}", connection_as_json(conn))
        yield


class MongoHookTest(MongoHook):
    """
    Extending hook so that a mockmongo collection object can be passed in
    to get_collection()
    """

    def __init__(self, mongo_conn_id="mongo_default", *args, **kwargs):
        super().__init__(mongo_conn_id=mongo_conn_id, *args, **kwargs)

    def get_collection(self, mock_collection, mongo_db=None):
        return mock_collection


@pytest.mark.skipif(mongomock is None, reason="mongomock package not present")
class TestMongoHook:
    def setup_method(self):
        self.hook = MongoHookTest(mongo_conn_id="mongo_default")
        self.conn = self.hook.get_conn()

    def test_mongo_conn_id(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", category=AirflowProviderDeprecationWarning)
            # Use default "mongo_default"
            assert MongoHook().mongo_conn_id == "mongo_default"
            # Positional argument
            assert MongoHook("fake_connection").mongo_conn_id == "fake_connection"

        warning_message = "Parameter `conn_id` is deprecated"
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            assert MongoHook(conn_id="fake_connection").mongo_conn_id == "fake_connection"

        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            assert (
                MongoHook(conn_id="fake_connection", mongo_conn_id="foo-bar").mongo_conn_id
                == "fake_connection"
            )

    def test_get_conn(self):
        assert self.hook.connection.port == 27017
        assert isinstance(self.conn, pymongo.MongoClient)

    def test_srv(self):
        hook = MongoHook(mongo_conn_id="mongo_default_with_srv")
        assert hook.uri.startswith("mongodb+srv://")

    def test_invalid_conn_type(self):
        with pytest.raises(
            AirflowConfigException,
            match="conn_type 'mongodb' not allowed for MongoHook; conn_type must be 'mongo'",
        ):
            MongoHook(mongo_conn_id="mongo_invalid_conn_type")

    def test_invalid_conn_type_srv(self):
        with pytest.raises(
            AirflowConfigException,
            match="Mongo SRV connections should have the conn_type 'mongo' and set 'use_srv=true' in extras",
        ):
            MongoHook(mongo_conn_id="mongo_invalid_conn_type_srv")

    def test_invalid_srv_with_port(self):
        with pytest.raises(AirflowConfigException, match="srv URI should not specify a port"):
            MongoHook(mongo_conn_id="mongo_invalid_srv_with_port")

    def test_insert_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {"test_insert_one": "test_value"}
        self.hook.insert_one(collection, obj)

        result_obj = collection.find_one(filter=obj)

        assert obj == result_obj

    def test_insert_many(self):
        collection = mongomock.MongoClient().db.collection
        objs = [{"test_insert_many_1": "test_value"}, {"test_insert_many_2": "test_value"}]

        self.hook.insert_many(collection, objs)

        result_objs = list(collection.find())
        assert len(result_objs) == 2

    def test_update_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {"_id": "1", "field": 0}
        collection.insert_one(obj)

        filter_doc = obj
        update_doc = {"$inc": {"field": 123}}

        self.hook.update_one(collection, filter_doc, update_doc)

        result_obj = collection.find_one(filter="1")
        assert 123 == result_obj["field"]

    def test_update_one_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        filter_doc = {"_id": "1", "field": 0}
        update_doc = {"$inc": {"field": 123}}

        self.hook.update_one(collection, filter_doc, update_doc, upsert=True)

        result_obj = collection.find_one(filter="1")
        assert 123 == result_obj["field"]

    def test_update_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": 0}
        obj2 = {"_id": "2", "field": 0}
        collection.insert_many([obj1, obj2])

        filter_doc = {"field": 0}
        update_doc = {"$inc": {"field": 123}}

        self.hook.update_many(collection, filter_doc, update_doc)

        result_obj = collection.find_one(filter="1")
        assert 123 == result_obj["field"]

        result_obj = collection.find_one(filter="2")
        assert 123 == result_obj["field"]

    def test_update_many_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        filter_doc = {"_id": "1", "field": 0}
        update_doc = {"$inc": {"field": 123}}

        self.hook.update_many(collection, filter_doc, update_doc, upsert=True)

        result_obj = collection.find_one(filter="1")
        assert 123 == result_obj["field"]

    def test_replace_one(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": "test_value_1"}
        obj2 = {"_id": "2", "field": "test_value_2"}
        collection.insert_many([obj1, obj2])

        obj1["field"] = "test_value_1_updated"
        self.hook.replace_one(collection, obj1)

        result_obj = collection.find_one(filter="1")
        assert "test_value_1_updated" == result_obj["field"]

        # Other document should stay intact
        result_obj = collection.find_one(filter="2")
        assert "test_value_2" == result_obj["field"]

    def test_replace_one_with_filter(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": "test_value_1"}
        obj2 = {"_id": "2", "field": "test_value_2"}
        collection.insert_many([obj1, obj2])

        obj1["field"] = "test_value_1_updated"
        self.hook.replace_one(collection, obj1, {"field": "test_value_1"})

        result_obj = collection.find_one(filter="1")
        assert "test_value_1_updated" == result_obj["field"]

        # Other document should stay intact
        result_obj = collection.find_one(filter="2")
        assert "test_value_2" == result_obj["field"]

    def test_replace_one_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        obj = {"_id": "1", "field": "test_value_1"}
        self.hook.replace_one(collection, obj, upsert=True)

        result_obj = collection.find_one(filter="1")
        assert "test_value_1" == result_obj["field"]

    def test_replace_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": "test_value_1"}
        obj2 = {"_id": "2", "field": "test_value_2"}
        collection.insert_many([obj1, obj2])

        obj1["field"] = "test_value_1_updated"
        obj2["field"] = "test_value_2_updated"
        self.hook.replace_many(collection, [obj1, obj2])

        result_obj = collection.find_one(filter="1")
        assert "test_value_1_updated" == result_obj["field"]

        result_obj = collection.find_one(filter="2")
        assert "test_value_2_updated" == result_obj["field"]

    def test_replace_many_with_upsert(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": "test_value_1"}
        obj2 = {"_id": "2", "field": "test_value_2"}

        self.hook.replace_many(collection, [obj1, obj2], upsert=True)

        result_obj = collection.find_one(filter="1")
        assert "test_value_1" == result_obj["field"]

        result_obj = collection.find_one(filter="2")
        assert "test_value_2" == result_obj["field"]

    def test_create_uri_with_all_creds(self):
        self.hook.connection.login = "test_user"
        self.hook.connection.password = "test_password"
        self.hook.connection.host = "test_host"
        self.hook.connection.port = 1234
        self.hook.connection.schema = "test_db"
        assert self.hook._create_uri() == "mongodb://test_user:test_password@test_host:1234/test_db"

    def test_create_uri_no_creds(self):
        self.hook.connection.login = None
        self.hook.connection.password = None
        self.hook.connection.port = None
        assert self.hook._create_uri() == "mongodb://mongo/None"

    def test_create_uri_srv_true(self):
        self.hook.extras["srv"] = True
        self.hook.connection.login = "test_user"
        self.hook.connection.password = "test_password"
        self.hook.connection.host = "test_host"
        self.hook.connection.port = 1234
        self.hook.connection.schema = "test_db"
        assert self.hook._create_uri() == "mongodb+srv://test_user:test_password@test_host:1234/test_db"

    def test_delete_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {"_id": "1"}
        collection.insert_one(obj)

        self.hook.delete_one(collection, {"_id": "1"})

        assert 0 == collection.count_documents({})

    def test_delete_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {"_id": "1", "field": "value"}
        obj2 = {"_id": "2", "field": "value"}
        collection.insert_many([obj1, obj2])

        self.hook.delete_many(collection, {"field": "value"})

        assert 0 == collection.count_documents({})

    def test_find_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {"test_find_one": "test_value"}
        collection.insert_one(obj)

        result_obj = self.hook.find(collection, {}, find_one=True)
        result_obj = {result: result_obj[result] for result in result_obj}
        assert obj == result_obj

    def test_find_many(self):
        collection = mongomock.MongoClient().db.collection
        objs = [{"_id": 1, "test_find_many_1": "test_value"}, {"_id": 2, "test_find_many_2": "test_value"}]
        collection.insert_many(objs)

        result_objs = self.hook.find(mongo_collection=collection, query={}, projection={}, find_one=False)

        assert len(list(result_objs)) > 1

    def test_find_many_with_projection(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {"_id": "1", "test_find_many_1": "test_value", "field_3": "a"},
            {"_id": "2", "test_find_many_2": "test_value", "field_3": "b"},
        ]
        collection.insert_many(objs)

        projection = {"_id": 0}
        result_objs = self.hook.find(
            mongo_collection=collection, query={}, projection=projection, find_one=False
        )
        assert "_id" not in result_objs[0]

    def test_aggregate(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {"test_id": "1", "test_status": "success"},
            {"test_id": "2", "test_status": "failure"},
            {"test_id": "3", "test_status": "success"},
        ]

        collection.insert_many(objs)

        aggregate_query = [{"$match": {"test_status": "success"}}]

        results = self.hook.aggregate(collection, aggregate_query)
        assert len(list(results)) == 2

    def test_distinct(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {"test_id": "1", "test_status": "success"},
            {"test_id": "2", "test_status": "failure"},
            {"test_id": "3", "test_status": "success"},
        ]

        collection.insert_many(objs)

        results = self.hook.distinct(collection, "test_status")
        assert len(results) == 2

    def test_distinct_with_filter(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {"test_id": "1", "test_status": "success"},
            {"test_id": "2", "test_status": "failure"},
            {"test_id": "3", "test_status": "success"},
        ]

        collection.insert_many(objs)

        results = self.hook.distinct(collection, "test_id", {"test_status": "failure"})
        assert len(results) == 1


def test_context_manager():
    with MongoHook(mongo_conn_id="mongo_default") as ctx_hook:
        ctx_hook.get_conn()

        assert isinstance(ctx_hook, MongoHook)
        assert ctx_hook.client is not None

    assert ctx_hook.client is None
