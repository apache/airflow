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

from airflow import DAG
from airflow.contrib.operators.mongo_to_s3 import MongoToS3Operator
from airflow.models import TaskInstance
from airflow.utils import timezone
from tests.compat import mock

TASK_ID = 'test_mongo_to_s3_operator'
MONGO_CONN_ID = 'default_mongo'
S3_CONN_ID = 'default_s3'
MONGO_COLLECTION = 'example_collection'
MONGO_QUERY = {"$lt": "{{ ts + 'Z' }}"}
S3_BUCKET = 'example_bucket'
S3_KEY = 'example_key'

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MOCK_MONGO_RETURN = [
    {'example_return_key_1': 'example_return_value_1'},
    {'example_return_key_2': 'example_return_value_2'}
]


class TestMongoToS3Operator(unittest.TestCase):

    def setUp(self):
        args = {
            'owner': 'airflow',
            'start_date': DEFAULT_DATE
        }

        self.dag = DAG('test_dag_id', default_args=args)

        self.mock_operator = MongoToS3Operator(
            task_id=TASK_ID,
            mongo_conn_id=MONGO_CONN_ID,
            s3_conn_id=S3_CONN_ID,
            mongo_collection=MONGO_COLLECTION,
            mongo_query=MONGO_QUERY,
            s3_bucket=S3_BUCKET,
            s3_key=S3_KEY,
            dag=self.dag
        )

    def test_init(self):
        self.assertEqual(self.mock_operator.task_id, TASK_ID)
        self.assertEqual(self.mock_operator.mongo_conn_id, MONGO_CONN_ID)
        self.assertEqual(self.mock_operator.s3_conn_id, S3_CONN_ID)
        self.assertEqual(self.mock_operator.mongo_collection, MONGO_COLLECTION)
        self.assertEqual(self.mock_operator.mongo_query, MONGO_QUERY)
        self.assertEqual(self.mock_operator.s3_bucket, S3_BUCKET)
        self.assertEqual(self.mock_operator.s3_key, S3_KEY)

    def test_template_field_overrides(self):
        self.assertEqual(self.mock_operator.template_fields, ['s3_key', 'mongo_query'])

    def test_render_template(self):
        ti = TaskInstance(self.mock_operator, DEFAULT_DATE)
        ti.render_templates()

        expected_rendered_template = {'$lt': '2017-01-01T00:00:00+00:00Z'}

        self.assertDictEqual(
            expected_rendered_template,
            getattr(self.mock_operator, 'mongo_query')
        )

    @mock.patch('airflow.contrib.operators.mongo_to_s3.MongoHook')
    @mock.patch('airflow.contrib.operators.mongo_to_s3.S3Hook')
    def test_execute(self, mock_s3_hook, mock_mongo_hook):
        operator = self.mock_operator

        mock_mongo_hook.return_value.find.return_value = iter(MOCK_MONGO_RETURN)
        mock_s3_hook.return_value.load_string.return_value = True

        operator.execute(None)

        mock_mongo_hook.return_value.find.assert_called_once_with(
            mongo_collection=MONGO_COLLECTION,
            query=MONGO_QUERY,
            mongo_db=None
        )

        op_stringify = self.mock_operator._stringify
        op_transform = self.mock_operator.transform

        s3_doc_str = op_stringify(op_transform(MOCK_MONGO_RETURN))

        mock_s3_hook.return_value.load_string.assert_called_once_with(
            string_data=s3_doc_str,
            key=S3_KEY,
            bucket_name=S3_BUCKET,
            replace=False
        )


if __name__ == '__main__':
    unittest.main()
