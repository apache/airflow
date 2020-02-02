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
#
import unittest

import boto3
import mock

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.aws_hook import AwsHook

try:
    from moto import mock_emr, mock_dynamodb2, mock_sts, mock_iam
except ImportError:
    mock_emr = None
    mock_dynamodb2 = None
    mock_sts = None
    mock_iam = None


class TestAwsHook(unittest.TestCase):
    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        client = boto3.client('emr', region_name='us-east-1')
        if client.list_clusters()['Clusters']:
            raise ValueError('AWS not properly mocked')

        hook = AwsHook(aws_conn_id='aws_default')
        client_from_hook = hook.get_client_type('emr')

        self.assertEqual(client_from_hook.list_clusters()['Clusters'], [])

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_resource_type_returns_a_boto3_resource_of_the_requested_type(self):
        hook = AwsHook(aws_conn_id='aws_default')
        resource_from_hook = hook.get_resource_type('dynamodb')

        # this table needs to be created in production
        table = resource_from_hook.create_table(  # pylint: disable=no-member
            TableName='test_airflow',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        table.meta.client.get_waiter(
            'table_exists').wait(TableName='test_airflow')

        self.assertEqual(table.item_count, 0)

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_session_returns_a_boto3_session(self):
        hook = AwsHook(aws_conn_id='aws_default')
        session_from_hook = hook.get_session()
        resource_from_session = session_from_hook.resource('dynamodb')
        table = resource_from_session.create_table(  # pylint: disable=no-member
            TableName='test_airflow',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        )

        table.meta.client.get_waiter(
            'table_exists').wait(TableName='test_airflow')

        self.assertEqual(table.item_count, 0)

    @mock.patch.object(AwsHook, 'get_connection')
    def test_get_credentials_from_login_with_token(self, mock_get_connection):
        mock_connection = Connection(login='aws_access_key_id',
                                     password='aws_secret_access_key',
                                     extra='{"aws_session_token": "test_token"}'
                                     )
        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertEqual(credentials_from_hook.token, 'test_token')

    @mock.patch.object(AwsHook, 'get_connection')
    def test_get_credentials_from_login_without_token(self, mock_get_connection):
        mock_connection = Connection(login='aws_access_key_id',
                                     password='aws_secret_access_key',
                                     )

        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertIsNone(credentials_from_hook.token)

    @mock.patch.object(AwsHook, 'get_connection')
    def test_get_credentials_from_extra_with_token(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"aws_access_key_id": "aws_access_key_id",'
                  '"aws_secret_access_key": "aws_secret_access_key",'
                  ' "aws_session_token": "session_token"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertEqual(credentials_from_hook.token, 'session_token')

    @mock.patch.object(AwsHook, 'get_connection')
    def test_get_credentials_from_extra_without_token(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"aws_access_key_id": "aws_access_key_id",'
                  '"aws_secret_access_key": "aws_secret_access_key"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertIsNone(credentials_from_hook.token)

    @mock.patch('airflow.providers.amazon.aws.hooks.aws_hook._parse_s3_config',
                return_value=('aws_access_key_id', 'aws_secret_access_key'))
    @mock.patch.object(AwsHook, 'get_connection')
    def test_get_credentials_from_extra_with_s3_config_and_profile(
        self, mock_get_connection, mock_parse_s3_config
    ):
        mock_connection = Connection(
            extra='{"s3_config_format": "aws", '
                  '"profile": "test", '
                  '"s3_config_file": "aws-credentials", '
                  '"region_name": "us-east-1"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        hook._get_credentials(region_name=None)
        mock_parse_s3_config.assert_called_once_with(
            'aws-credentials',
            'aws',
            'test'
        )

    @unittest.skipIf(mock_sts is None, 'mock_sts package not present')
    @mock.patch.object(AwsHook, 'get_connection')
    @mock_sts
    def test_get_credentials_from_role_arn(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"role_arn":"arn:aws:iam::123456:role/role_arn"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsHook()
        credentials_from_hook = hook.get_credentials()
        self.assertIn("ASIA", credentials_from_hook.access_key)

        # We assert the length instead of actual values as the values are random:
        # Details: https://github.com/spulec/moto/commit/ab0d23a0ba2506e6338ae20b3fde70da049f7b03
        self.assertEqual(20, len(credentials_from_hook.access_key))
        self.assertEqual(40, len(credentials_from_hook.secret_key))
        self.assertEqual(356, len(credentials_from_hook.token))

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_expand_role(self):
        conn = boto3.client('iam', region_name='us-east-1')
        conn.create_role(RoleName='test-role', AssumeRolePolicyDocument='some policy')
        hook = AwsHook()
        arn = hook.expand_role('test-role')
        expect_arn = conn.get_role(RoleName='test-role').get('Role').get('Arn')
        self.assertEqual(arn, expect_arn)


if __name__ == '__main__':
    unittest.main()
