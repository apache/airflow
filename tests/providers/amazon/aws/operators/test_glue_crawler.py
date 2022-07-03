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

import unittest
from unittest import mock

from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

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


class TestGlueCrawlerOperator(unittest.TestCase):
    def setUp(self):
        self.glue = GlueCrawlerOperator(task_id='test_glue_crawler_operator', config=mock_config)

    @mock.patch('airflow.providers.amazon.aws.operators.glue_crawler.GlueCrawlerHook')
    def test_execute_without_failure(self, mock_hook):
        mock_hook.return_value.has_crawler.return_value = True
        self.glue.execute({})

        mock_hook.assert_has_calls(
            [
                mock.call('aws_default'),
                mock.call().has_crawler('test-crawler'),
                mock.call().update_crawler(**mock_config),
                mock.call().start_crawler(mock_crawler_name),
                mock.call().wait_for_crawler_completion(crawler_name=mock_crawler_name, poll_interval=5),
            ]
        )
