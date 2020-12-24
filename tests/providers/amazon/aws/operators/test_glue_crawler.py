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
from unittest import mock

from airflow import configuration
from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator

mock_crawler_name = 'test-crawler'
mock_role_name = 'test-role'
mock_config = {
    'Name': mock_crawler_name,
    'Description': 'Test glue crawler from Airflow',
    'DatabaseName': 'test_db',
    'Role': mock_role_name,
    'Targets': {
        'S3Targets': [
            {
                'Path': 's3://test-glue-crawler/foo/',
                'Exclusions': [
                    's3://test-glue-crawler/bar/',
                ],
                'ConnectionName': 'test-s3-conn',
            }
        ],
        'JdbcTargets': [
            {
                'ConnectionName': 'test-jdbc-conn',
                'Path': 'test_db/test_table>',
                'Exclusions': [
                    'string',
                ],
            }
        ],
        'MongoDBTargets': [
            {'ConnectionName': 'test-mongo-conn', 'Path': 'test_db/test_collection', 'ScanAll': True}
        ],
        'DynamoDBTargets': [{'Path': 'test_db/test_table', 'scanAll': True, 'scanRate': 123.0}],
        'CatalogTargets': [
            {
                'DatabaseName': 'test_glue_db',
                'Tables': [
                    'test',
                ],
            }
        ],
    },
    'Classifiers': ['test-classifier'],
    'TablePrefix': 'test',
    'SchemaChangePolicy': {
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'DEPRECATE_IN_DATABASE',
    },
    'RecrawlPolicy': {'RecrawlBehavior': 'CRAWL_EVERYTHING'},
    'LineageConfiguration': 'ENABLE',
    'Configuration': """
    {
        "Version": 1.0,
        "CrawlerOutput": {
            "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" }
        }
    }
    """,
    'SecurityConfiguration': 'test',
    'Tags': {'test': 'foo'},
}


class TestAwsGlueCrawlerOperator(unittest.TestCase):
    @mock.patch('airflow.providers.amazon.aws.hooks.glue_crawler.AwsGlueCrawlerHook')
    def setUp(self, glue_hook_mock):
        configuration.load_test_config()

        self.glue_hook_mock = glue_hook_mock
        self.glue = AwsGlueCrawlerOperator(task_id='test_glue_operator', config=mock_config)

    @mock.patch.object(AwsGlueCrawlerHook, 'start_crawler')
    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    def test_execute_without_failure(self, mock_get_conn, mock_start_crawler):
        mock_start_crawler.return_value = {}
        self.glue.execute(None)

        mock_start_crawler.assert_called_once_with({})
        self.assertEqual(self.glue.crawler_name, mock_crawler_name)
