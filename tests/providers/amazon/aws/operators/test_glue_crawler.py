
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

import mock

from airflow import configuration
from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook
from airflow.providers.amazon.aws.operators.glue_crawler import AwsGlueCrawlerOperator


mock_crawler_name = 'test-crawler'
mock_role_name = 'test-role'

mock_crawler_config = {
    'crawler_name': mock_crawler_name,
    'crawler_desc': 'Test glue crawler from Airflow',
    'glue_db_name': 'test_db',
    'iam_role_name': mock_role_name,
    'aws_conn_id': 'aws_default',
    's3_targets_configuration': [
        {
            'Path': 's3://test-glue-crawler/foo/',
            'Exclusions': [
                's3://test-glue-crawler/bar/',
            ],
            'ConnectionName': 'test-s3-conn'
        }
    ],
    'jdbc_targets_configuration': [
        {
            'ConnectionName': 'test-jdbc-conn',
            'Path': 'test_db/test_table>',
            'Exclusions': [
                'string',
            ]
        }
    ],
    'mongo_targets_configuration': [
        {
            'ConnectionName': 'test-mongo-conn',
            'Path': 'test_db/test_collection',
            'ScanAll': True
        }
    ],
    'dynamo_targets_configuration': [
        {
            'Path': 'test_db/test_table',
            'scanAll': True|False,
            'scanRate': 123.0
        }
    ],
    'glue_catalog_targets_configuration': [
        {
            'DatabaseName': 'test_glue_db',
            'Tables': [
                'test',
            ]
        }
    ],
    'cron_schedule': 'cron(12 12 * * ? *)',
    'classifiers': 'test-classifier',
    'table_prefix': 'test',
    'update_behavior': 'LOG',
    'delete_behavior': 'LOG',
    'recrawl_behavior': 'CRAWL_EVERYTHING',
    'lineage_settings': 'ENABLE',
    'json_configuration': 'test',
    'security_configuration': 'test',
    'tags': {
        'test': 'foo'
    }
}

class TestAwsGlueCrawlerOperator(unittest.TestCase):

    @mock.patch('airflow.providers.amazon.aws.hooks.glue_crawler.AwsGlueCrawlerHook')
    def setUp(self, glue_hook_mock):
        configuration.load_test_config()

        self.glue_hook_mock = glue_hook_mock
        self.glue = AwsGlueCrawlerOperator(task_id='test_glue_operator', **mock_crawler_config)
                                    

    @mock.patch.object(AwsGlueCrawlerHook, 'initialize_crawler')
    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    def test_execute_without_failure(self, mock_get_conn, mock_initialize_crawler):
        mock_initialize_crawler.return_value = {'Status': 'SUCCEEDED'}
        self.glue.execute(None)

        mock_initialize_crawler.assert_called_once_with({})
        self.assertEqual(self.glue.crawler_name, mock_crawler_name)