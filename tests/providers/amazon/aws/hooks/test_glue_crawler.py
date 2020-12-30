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
import json
import unittest
from unittest import mock

from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook

try:
    from moto import mock_iam
except ImportError:
    mock_iam = None

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


class TestAwsGlueCrawlerHook(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.hook = AwsGlueCrawlerHook(aws_conn_id="aws_default", poll_interval=5)

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_get_iam_execution_role(self):
        iam_role = self.hook.get_client_type('iam').create_role(
            Path="/",
            RoleName=mock_role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": {
                        "Effect": "Allow",
                        "Principal": {"Service": "glue.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    },
                }
            ),
        )
        iam_role = self.hook.get_iam_execution_role(role_name=mock_role_name)

        self.assertEqual(iam_role, mock_role_name)

    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    @mock.patch.object(AwsGlueCrawlerHook, "get_or_create_crawler")
    def test_get_or_create_crawler(self, mock_get_conn, mock_get_or_create_crawler):

        mock_glue_crawler = mock_get_conn.return_value.get_crawler(Name=mock_crawler_name).return_value['Crawler']['Name']
        glue_crawler = self.hook.get_or_create_crawler(config=mock_config)

        self.assertEqual(glue_crawler, mock_glue_crawler)

    @mock.patch.object(AwsGlueCrawlerHook, "wait_for_crawler_completion")
    @mock.patch.object(AwsGlueCrawlerHook, "get_or_create_crawler")
    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    def test_start_crawler(self, mock_get_conn, mock_get_or_create_crawler, mock_completion):

        mock_get_or_create_crawler.Name = mock.Mock(Name=mock_crawler_name)
        mock_get_conn.return_value.start_crawler(crawler_name=mock_crawler_name)

        mock_crawler_state = mock_completion.return_value
        glue_crawler_state = self.hook.wait_for_crawler_completion(crawler_name=mock_crawler_name)

        self.assertEqual(glue_crawler_state, mock_crawler_state)


if __name__ == '__main__':
    unittest.main()
