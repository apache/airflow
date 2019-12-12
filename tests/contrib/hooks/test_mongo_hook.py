# -*- coding: utf-8 -*-
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
import unittest

import pymongo

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.models import Connection
from airflow.utils import db

try:
    import mongomock
except ImportError:
    mongomock = None


class MongoHookTest(MongoHook):
    """
    Extending hook so that a mockmongo collection object can be passed in
    to get_collection()
    """
    def __init__(self, conn_id='mongo_default', *args, **kwargs):
        super().__init__(conn_id=conn_id, *args, **kwargs)

    def get_collection(self, mock_collection, mongo_db=None):
        return mock_collection


class TestMongoHook(unittest.TestCase):
    def setUp(self):
        self.hook = MongoHookTest(conn_id='mongo_default', mongo_db='default')
        self.conn = self.hook.get_conn()
        db.merge_conn(
            Connection(
                conn_id='mongo_default_with_srv', conn_type='mongo',
                host='mongo', port='27017', extra='{"srv": true}'))

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_get_conn(self):
        self.assertEqual(self.hook.connection.port, 27017)
        self.assertIsInstance(self.conn, pymongo.MongoClient)

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_srv(self):
        hook = MongoHook(conn_id='mongo_default_with_srv')
        self.assertTrue(hook.uri.startswith('mongodb+srv://'))

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_insert_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {'test_insert_one': 'test_value'}
        self.hook.insert_one(collection, obj)

        result_obj = collection.find_one(filter=obj)

        self.assertEqual(obj, result_obj)

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_insert_many(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {'test_insert_many_1': 'test_value'},
            {'test_insert_many_2': 'test_value'}
        ]

        self.hook.insert_many(collection, objs)

        result_objs = collection.find()
        result_objs = [result for result in result_objs]
        self.assertEqual(len(result_objs), 2)

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_update_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {'_id': '1', 'field': 0}
        collection.insert_one(obj)

        filter_doc = obj
        update_doc = {'$inc': {'field': 123}}

        self.hook.update_one(collection, filter_doc, update_doc)

        result_obj = collection.find_one(filter='1')
        self.assertEqual(123, result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_update_one_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        filter_doc = {'_id': '1', 'field': 0}
        update_doc = {'$inc': {'field': 123}}

        self.hook.update_one(collection, filter_doc, update_doc, upsert=True)

        result_obj = collection.find_one(filter='1')
        self.assertEqual(123, result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_update_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 0}
        obj2 = {'_id': '2', 'field': 0}
        collection.insert_many([obj1, obj2])

        filter_doc = {'field': 0}
        update_doc = {'$inc': {'field': 123}}

        self.hook.update_many(collection, filter_doc, update_doc)

        result_obj = collection.find_one(filter='1')
        self.assertEqual(123, result_obj['field'])

        result_obj = collection.find_one(filter='2')
        self.assertEqual(123, result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_update_many_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        filter_doc = {'_id': '1', 'field': 0}
        update_doc = {'$inc': {'field': 123}}

        self.hook.update_many(collection, filter_doc, update_doc, upsert=True)

        result_obj = collection.find_one(filter='1')
        self.assertEqual(123, result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_replace_one(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 'test_value_1'}
        obj2 = {'_id': '2', 'field': 'test_value_2'}
        collection.insert_many([obj1, obj2])

        obj1['field'] = 'test_value_1_updated'
        self.hook.replace_one(collection, obj1)

        result_obj = collection.find_one(filter='1')
        self.assertEqual('test_value_1_updated', result_obj['field'])

        # Other document should stay intact
        result_obj = collection.find_one(filter='2')
        self.assertEqual('test_value_2', result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_replace_one_with_filter(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 'test_value_1'}
        obj2 = {'_id': '2', 'field': 'test_value_2'}
        collection.insert_many([obj1, obj2])

        obj1['field'] = 'test_value_1_updated'
        self.hook.replace_one(collection, obj1, {'field': 'test_value_1'})

        result_obj = collection.find_one(filter='1')
        self.assertEqual('test_value_1_updated', result_obj['field'])

        # Other document should stay intact
        result_obj = collection.find_one(filter='2')
        self.assertEqual('test_value_2', result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_replace_one_with_upsert(self):
        collection = mongomock.MongoClient().db.collection

        obj = {'_id': '1', 'field': 'test_value_1'}
        self.hook.replace_one(collection, obj, upsert=True)

        result_obj = collection.find_one(filter='1')
        self.assertEqual('test_value_1', result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_replace_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 'test_value_1'}
        obj2 = {'_id': '2', 'field': 'test_value_2'}
        collection.insert_many([obj1, obj2])

        obj1['field'] = 'test_value_1_updated'
        obj2['field'] = 'test_value_2_updated'
        self.hook.replace_many(collection, [obj1, obj2])

        result_obj = collection.find_one(filter='1')
        self.assertEqual('test_value_1_updated', result_obj['field'])

        result_obj = collection.find_one(filter='2')
        self.assertEqual('test_value_2_updated', result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_replace_many_with_upsert(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 'test_value_1'}
        obj2 = {'_id': '2', 'field': 'test_value_2'}

        self.hook.replace_many(collection, [obj1, obj2], upsert=True)

        result_obj = collection.find_one(filter='1')
        self.assertEqual('test_value_1', result_obj['field'])

        result_obj = collection.find_one(filter='2')
        self.assertEqual('test_value_2', result_obj['field'])

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_delete_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {'_id': '1'}
        collection.insert_one(obj)

        self.hook.delete_one(collection, {'_id': '1'})

        self.assertEqual(0, collection.count())

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_delete_many(self):
        collection = mongomock.MongoClient().db.collection
        obj1 = {'_id': '1', 'field': 'value'}
        obj2 = {'_id': '2', 'field': 'value'}
        collection.insert_many([obj1, obj2])

        self.hook.delete_many(collection, {'field': 'value'})

        self.assertEqual(0, collection.count())

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_find_one(self):
        collection = mongomock.MongoClient().db.collection
        obj = {'test_find_one': 'test_value'}
        collection.insert(obj)

        result_obj = self.hook.find(collection, {}, find_one=True)
        result_obj = {result: result_obj[result] for result in result_obj}
        self.assertEqual(obj, result_obj)

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_find_many(self):
        collection = mongomock.MongoClient().db.collection
        objs = [{'test_find_many_1': 'test_value'}, {'test_find_many_2': 'test_value'}]
        collection.insert(objs)

        result_objs = self.hook.find(collection, {}, find_one=False)
        result_objs = [result for result in result_objs]

        self.assertGreater(len(result_objs), 1)

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_aggregate(self):
        collection = mongomock.MongoClient().db.collection
        objs = [
            {
                'test_id': '1',
                'test_status': 'success'
            },
            {
                'test_id': '2',
                'test_status': 'failure'
            },
            {
                'test_id': '3',
                'test_status': 'success'
            }
        ]

        collection.insert(objs)

        aggregate_query = [
            {"$match": {'test_status': 'success'}}
        ]

        results = self.hook.aggregate(collection, aggregate_query)
        results = [result for result in results]
        self.assertEqual(len(results), 2)

    def test_context_manager(self):
        with MongoHook(conn_id='mongo_default', mongo_db='default') as ctxHook:
            ctxHook.get_conn()

            self.assertIsInstance(ctxHook, MongoHook)
            self.assertIsNotNone(ctxHook.client)

        self.assertIsNone(ctxHook.client)


if __name__ == '__main__':
    unittest.main()
