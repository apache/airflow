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
import json
import unittest
from unittest import mock

import boto3

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

try:
    from moto import mock_dynamodb2, mock_emr, mock_iam, mock_sts
except ImportError:
    mock_emr = None
    mock_dynamodb2 = None
    mock_sts = None
    mock_iam = None


class TestAwsBaseHook(unittest.TestCase):
    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        client = boto3.client('emr', region_name='us-east-1')
        if client.list_clusters()['Clusters']:
            raise ValueError('AWS not properly mocked')

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='emr')
        client_from_hook = hook.get_client_type('emr')

        self.assertEqual(client_from_hook.list_clusters()['Clusters'], [])

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_resource_type_returns_a_boto3_resource_of_the_requested_type(self):
        hook = AwsBaseHook(aws_conn_id='aws_default', resource_type='dynamodb')
        resource_from_hook = hook.get_resource_type('dynamodb')

        # this table needs to be created in production
        table = resource_from_hook.create_table(  # pylint: disable=no-member
            TableName='test_airflow',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10},
        )

        table.meta.client.get_waiter('table_exists').wait(TableName='test_airflow')

        self.assertEqual(table.item_count, 0)

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_session_returns_a_boto3_session(self):
        hook = AwsBaseHook(aws_conn_id='aws_default', resource_type='dynamodb')
        session_from_hook = hook.get_session()
        resource_from_session = session_from_hook.resource('dynamodb')
        table = resource_from_session.create_table(  # pylint: disable=no-member
            TableName='test_airflow',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10},
        )

        table.meta.client.get_waiter('table_exists').wait(TableName='test_airflow')

        self.assertEqual(table.item_count, 0)

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_login_with_token(self, mock_get_connection):
        mock_connection = Connection(
            login='aws_access_key_id',
            password='aws_secret_access_key',
            extra='{"aws_session_token": "test_token"}',
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertEqual(credentials_from_hook.token, 'test_token')

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_login_without_token(self, mock_get_connection):
        mock_connection = Connection(
            login='aws_access_key_id',
            password='aws_secret_access_key',
        )

        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='spam')
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertIsNone(credentials_from_hook.token)

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_extra_with_token(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"aws_access_key_id": "aws_access_key_id",'
            '"aws_secret_access_key": "aws_secret_access_key",'
            ' "aws_session_token": "session_token"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertEqual(credentials_from_hook.token, 'session_token')

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_extra_without_token(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"aws_access_key_id": "aws_access_key_id",'
            '"aws_secret_access_key": "aws_secret_access_key"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        self.assertEqual(credentials_from_hook.access_key, 'aws_access_key_id')
        self.assertEqual(credentials_from_hook.secret_key, 'aws_secret_access_key')
        self.assertIsNone(credentials_from_hook.token)

    @mock.patch(
        'airflow.providers.amazon.aws.hooks.base_aws._parse_s3_config',
        return_value=('aws_access_key_id', 'aws_secret_access_key'),
    )
    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_extra_with_s3_config_and_profile(
        self, mock_get_connection, mock_parse_s3_config
    ):
        mock_connection = Connection(
            extra='{"s3_config_format": "aws", '
            '"profile": "test", '
            '"s3_config_file": "aws-credentials", '
            '"region_name": "us-east-1"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        hook._get_credentials(region_name=None)
        mock_parse_s3_config.assert_called_once_with('aws-credentials', 'aws', 'test')

    @unittest.skipIf(mock_sts is None, 'mock_sts package not present')
    @mock.patch.object(AwsBaseHook, 'get_connection')
    @mock_sts
    def test_get_credentials_from_role_arn(self, mock_get_connection):
        mock_connection = Connection(extra='{"role_arn":"arn:aws:iam::123456:role/role_arn"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        self.assertIn("ASIA", credentials_from_hook.access_key)

        # We assert the length instead of actual values as the values are random:
        # Details: https://github.com/spulec/moto/commit/ab0d23a0ba2506e6338ae20b3fde70da049f7b03
        self.assertEqual(20, len(credentials_from_hook.access_key))
        self.assertEqual(40, len(credentials_from_hook.secret_key))
        self.assertEqual(356, len(credentials_from_hook.token))

    def test_get_credentials_from_gcp_credentials(self):
        mock_connection = Connection(
            extra=json.dumps(
                {
                    "role_arn": "arn:aws:iam::123456:role/role_arn",
                    "assume_role_method": "assume_role_with_web_identity",
                    "assume_role_with_web_identity_federation": 'google',
                    "assume_role_with_web_identity_federation_audience": 'aws-federation.airflow.apache.org',
                }
            )
        )

        # Store original __import__
        orig_import = __import__
        mock_id_token_credentials = mock.Mock()

        def import_mock(name, *args):
            if name == 'airflow.providers.google.common.utils.id_token_credentials':
                return mock_id_token_credentials
            return orig_import(name, *args)

        with mock.patch('builtins.__import__', side_effect=import_mock), mock.patch.dict(
            'os.environ', AIRFLOW_CONN_AWS_DEFAULT=mock_connection.get_uri()
        ), mock.patch('airflow.providers.amazon.aws.hooks.base_aws.boto3') as mock_boto3, mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws.botocore'
        ) as mock_botocore, mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws.botocore.session'
        ) as mock_session:
            hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')

            credentials_from_hook = hook.get_credentials()
            mock_get_credentials = mock_boto3.session.Session.return_value.get_credentials
            self.assertEqual(
                mock_get_credentials.return_value.get_frozen_credentials.return_value,
                credentials_from_hook,
            )

        mock_boto3.assert_has_calls(
            [
                mock.call.session.Session(
                    aws_access_key_id=None,
                    aws_secret_access_key=None,
                    aws_session_token=None,
                    region_name=None,
                ),
                mock.call.session.Session()._session.__bool__(),
                mock.call.session.Session(
                    botocore_session=mock_session.Session.return_value,
                    region_name=mock_boto3.session.Session.return_value.region_name,
                ),
                mock.call.session.Session().get_credentials(),
                mock.call.session.Session().get_credentials().get_frozen_credentials(),
            ]
        )
        mock_fetcher = mock_botocore.credentials.AssumeRoleWithWebIdentityCredentialFetcher
        mock_botocore.assert_has_calls(
            [
                mock.call.credentials.AssumeRoleWithWebIdentityCredentialFetcher(
                    client_creator=mock_boto3.session.Session.return_value._session.create_client,
                    extra_args={},
                    role_arn='arn:aws:iam::123456:role/role_arn',
                    web_identity_token_loader=mock.ANY,
                ),
                mock.call.credentials.DeferredRefreshableCredentials(
                    method='assume-role-with-web-identity',
                    refresh_using=mock_fetcher.return_value.fetch_credentials,
                    time_fetcher=mock.ANY,
                ),
            ]
        )

        mock_session.assert_has_calls([mock.call.Session()])
        mock_id_token_credentials.assert_has_calls(
            [mock.call.get_default_id_token_credentials(target_audience='aws-federation.airflow.apache.org')]
        )

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_expand_role(self):
        conn = boto3.client('iam', region_name='us-east-1')
        conn.create_role(RoleName='test-role', AssumeRolePolicyDocument='some policy')
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        arn = hook.expand_role('test-role')
        expect_arn = conn.get_role(RoleName='test-role').get('Role').get('Arn')
        self.assertEqual(arn, expect_arn)

    def test_use_default_boto3_behaviour_without_conn_id(self):
        for conn_id in (None, ''):
            hook = AwsBaseHook(aws_conn_id=conn_id, client_type='s3')
            # should cause no exception
            hook.get_client_type('s3')
