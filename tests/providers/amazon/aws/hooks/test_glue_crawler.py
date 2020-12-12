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

import mock

from airflow.providers.amazon.aws.hooks.glue_crawler import AwsGlueCrawlerHook

try:
    from moto import mock_iam
except ImportError:
    mock_iam = None

mock_crawler_name = 'test-crawler'
mock_role_name = 'test-role'
mock_crawler_config = {
    'crawler_name': mock_crawler_name,
    'crawler_desc': 'Test glue crawler from Airflow',
    'glue_db_name': 'test_db',
    'iam_role_name': mock_role_name,
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
class TestAwsGlueCrawlerHook(unittest.TestCase):
    def setUp(self):
        self.some_aws_region = "us-west-2"

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_get_iam_execution_role(self):
        hook = AwsGlueCrawlerHook(**mock_crawler_config)

        iam_role = hook.get_client_type('iam').create_role(
            Path="/",
            RoleName=mock_role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": {
                    "Effect": "Allow",
                    "Principal": {"Service": "glue.amazonaws.com"},
                    "Action": "sts:AssumeRole"
                }
            })
        )
        iam_role = hook.get_iam_execution_role()

        self.assertIsNotNone(iam_role)

    @mock.patch.object(AwsGlueCrawlerHook, "get_iam_execution_role")
    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    def test_get_or_create_glue_crawler(self, mock_get_conn,
                                    mock_get_iam_execution_role
                                    ):
        mock_get_iam_execution_role.return_value = mock.MagicMock(Role={'RoleName': mock_role_name})

        mock_glue_crawler = mock_get_conn.return_value.get_crawler()['Crawler']['Name']
        glue_crawler = AwsGlueCrawlerHook(**mock_crawler_config)
        glue_crawler = glue_crawler.get_or_create_glue_crawler()

        self.assertEqual(glue_crawler, mock_glue_crawler)

    @mock.patch.object(AwsGlueCrawlerHook, "crawler_completion")
    @mock.patch.object(AwsGlueCrawlerHook, "get_or_create_glue_crawler")
    @mock.patch.object(AwsGlueCrawlerHook, "get_conn")
    def test_initialize_crawler(self, mock_get_conn,
                            mock_get_or_create_glue_crawler,
                            mock_completion):

        mock_get_or_create_glue_crawler.Name = mock.Mock(Name=mock_crawler_name)
        mock_get_conn.return_value.start_crawler()

        mock_crawler_run_state = mock_completion.return_value
        glue_crawler_run_state = AwsGlueCrawlerHook(**mock_crawler_config)
        glue_crawler_run_state = glue_crawler_run_state.crawler_completion()

        self.assertEqual(glue_crawler_run_state, mock_crawler_run_state, msg='Mocks but be equal')


if __name__ == '__main__':
    unittest.main()