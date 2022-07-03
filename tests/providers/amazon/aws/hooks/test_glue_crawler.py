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

import unittest
from copy import deepcopy
from unittest import mock

from airflow.providers.amazon.aws.hooks.glue_crawler import GlueCrawlerHook

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


class TestGlueCrawlerHook(unittest.TestCase):
    @classmethod
    def setUp(cls):
        cls.hook = GlueCrawlerHook(aws_conn_id="aws_default")

    def test_init(self):
        self.assertEqual(self.hook.aws_conn_id, "aws_default")

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_has_crawler(self, mock_get_conn):
        response = self.hook.has_crawler(mock_crawler_name)
        self.assertEqual(response, True)
        mock_get_conn.return_value.get_crawler.assert_called_once_with(Name=mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_has_crawler_crawled_doesnt_exists(self, mock_get_conn):
        class MockException(Exception):
            pass

        mock_get_conn.return_value.exceptions.EntityNotFoundException = MockException
        mock_get_conn.return_value.get_crawler.side_effect = MockException("AAA")
        response = self.hook.has_crawler(mock_crawler_name)
        self.assertEqual(response, False)
        mock_get_conn.return_value.get_crawler.assert_called_once_with(Name=mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_crawler_needed(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {'Crawler': mock_config}

        mock_config_two = deepcopy(mock_config)
        mock_config_two['Role'] = 'test-2-role'
        response = self.hook.update_crawler(**mock_config_two)
        self.assertEqual(response, True)
        mock_get_conn.return_value.get_crawler.assert_called_once_with(Name=mock_crawler_name)
        mock_get_conn.return_value.update_crawler.assert_called_once_with(**mock_config_two)

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_update_crawler_not_needed(self, mock_get_conn):
        mock_get_conn.return_value.get_crawler.return_value = {'Crawler': mock_config}
        response = self.hook.update_crawler(**mock_config)
        self.assertEqual(response, False)
        mock_get_conn.return_value.get_crawler.assert_called_once_with(Name=mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_create_crawler(self, mock_get_conn):
        mock_get_conn.return_value.create_crawler.return_value = {'Crawler': {'Name': mock_crawler_name}}
        glue_crawler = self.hook.create_crawler(**mock_config)
        self.assertIn("Crawler", glue_crawler)
        self.assertIn("Name", glue_crawler["Crawler"])
        self.assertEqual(glue_crawler["Crawler"]["Name"], mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_start_crawler(self, mock_get_conn):
        result = self.hook.start_crawler(mock_crawler_name)
        self.assertEqual(result, mock_get_conn.return_value.start_crawler.return_value)

        mock_get_conn.return_value.start_crawler.assert_called_once_with(Name=mock_crawler_name)

    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    @mock.patch.object(GlueCrawlerHook, "get_conn")
    def test_wait_for_crawler_completion_instant_ready(self, mock_get_conn, mock_get_crawler):
        mock_get_crawler.side_effect = [
            {'State': 'READY', 'LastCrawl': {'Status': 'MOCK_STATUS'}},
        ]
        mock_get_conn.return_value.get_crawler_metrics.return_value = {
            'CrawlerMetricsList': [
                {
                    'LastRuntimeSeconds': 'TEST-A',
                    'MedianRuntimeSeconds': 'TEST-B',
                    'TablesCreated': 'TEST-C',
                    'TablesUpdated': 'TEST-D',
                    'TablesDeleted': 'TEST-E',
                }
            ]
        }
        result = self.hook.wait_for_crawler_completion(mock_crawler_name)
        self.assertEqual(result, 'MOCK_STATUS')
        mock_get_conn.assert_has_calls(
            [
                mock.call(),
                mock.call().get_crawler_metrics(CrawlerNameList=[mock_crawler_name]),
            ]
        )
        mock_get_crawler.assert_has_calls(
            [
                mock.call(mock_crawler_name),
            ]
        )

    @mock.patch.object(GlueCrawlerHook, "get_conn")
    @mock.patch.object(GlueCrawlerHook, "get_crawler")
    @mock.patch('airflow.providers.amazon.aws.hooks.glue_crawler.sleep')
    def test_wait_for_crawler_completion_retry_two_times(self, mock_sleep, mock_get_crawler, mock_get_conn):
        mock_get_crawler.side_effect = [
            {'State': 'RUNNING'},
            {'State': 'READY', 'LastCrawl': {'Status': 'MOCK_STATUS'}},
        ]
        mock_get_conn.return_value.get_crawler_metrics.side_effect = [
            {'CrawlerMetricsList': [{'TimeLeftSeconds': 12}]},
            {
                'CrawlerMetricsList': [
                    {
                        'LastRuntimeSeconds': 'TEST-A',
                        'MedianRuntimeSeconds': 'TEST-B',
                        'TablesCreated': 'TEST-C',
                        'TablesUpdated': 'TEST-D',
                        'TablesDeleted': 'TEST-E',
                    }
                ]
            },
        ]
        result = self.hook.wait_for_crawler_completion(mock_crawler_name)
        self.assertEqual(result, 'MOCK_STATUS')
        mock_get_conn.assert_has_calls(
            [
                mock.call(),
                mock.call().get_crawler_metrics(CrawlerNameList=[mock_crawler_name]),
            ]
        )
        mock_get_crawler.assert_has_calls(
            [
                mock.call(mock_crawler_name),
                mock.call(mock_crawler_name),
            ]
        )


if __name__ == '__main__':
    unittest.main()
