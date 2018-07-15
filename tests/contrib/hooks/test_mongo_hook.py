# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import pymongo
try:
    import mongomock
except ImportError:
    mongomock = None

from airflow import configuration
from airflow.contrib.hooks.mongo_hook import MongoHook


class MongoHookTest(MongoHook):
    '''
    Extending hook so that a mockmongo collection object can be passed in
    to get_collection()
    '''
    def __init__(self, conn_id='mongo_default', *args, **kwargs):
        super(MongoHookTest, self).__init__(conn_id=conn_id, *args, **kwargs)

    def get_collection(self, mock_collection, mongo_db=None):
        return mock_collection


class TestMongoHook(unittest.TestCase):
    def setUp(self):
        configuration.load_test_config()
        self.hook = MongoHookTest(conn_id='mongo_default', mongo_db='default')
        self.conn = self.hook.get_conn()

    @unittest.skipIf(mongomock is None, 'mongomock package not present')
    def test_get_conn(self):
        self.assertEqual(self.hook.connection.port, 27017)
        self.assertIsInstance(self.conn, pymongo.MongoClient)

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


if __name__ == '__main__':
    unittest.main()
