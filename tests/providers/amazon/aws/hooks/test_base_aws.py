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
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from unittest import mock

import boto3
import pytest
from moto.core import ACCOUNT_ID

from airflow.models import Connection
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

try:
    from moto import mock_dynamodb2, mock_emr, mock_iam, mock_sts
except ImportError:
    mock_emr = None
    mock_dynamodb2 = None
    mock_sts = None
    mock_iam = None

SAML_ASSERTION = """
<?xml version="1.0"?>
<samlp:Response xmlns:samlp="urn:oasis:names:tc:SAML:2.0:protocol" ID="_00000000-0000-0000-0000-000000000000" Version="2.0" IssueInstant="2012-01-01T12:00:00.000Z" Destination="https://signin.aws.amazon.com/saml" Consent="urn:oasis:names:tc:SAML:2.0:consent:unspecified">
  <Issuer xmlns="urn:oasis:names:tc:SAML:2.0:assertion">http://localhost/</Issuer>
  <samlp:Status>
    <samlp:StatusCode Value="urn:oasis:names:tc:SAML:2.0:status:Success"/>
  </samlp:Status>
  <Assertion xmlns="urn:oasis:names:tc:SAML:2.0:assertion" ID="_00000000-0000-0000-0000-000000000000" IssueInstant="2012-12-01T12:00:00.000Z" Version="2.0">
    <Issuer>http://localhost:3000/</Issuer>
    <ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
      <ds:SignedInfo>
        <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
        <ds:Reference URI="#_00000000-0000-0000-0000-000000000000">
          <ds:Transforms>
            <ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
            <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
          </ds:Transforms>
          <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
          <ds:DigestValue>NTIyMzk0ZGI4MjI0ZjI5ZGNhYjkyOGQyZGQ1NTZjODViZjk5YTY4ODFjOWRjNjkyYzZmODY2ZDQ4NjlkZjY3YSAgLQo=</ds:DigestValue>
        </ds:Reference>
      </ds:SignedInfo>
      <ds:SignatureValue>NTIyMzk0ZGI4MjI0ZjI5ZGNhYjkyOGQyZGQ1NTZjODViZjk5YTY4ODFjOWRjNjkyYzZmODY2ZDQ4NjlkZjY3YSAgLQo=</ds:SignatureValue>
      <KeyInfo xmlns="http://www.w3.org/2000/09/xmldsig#">
        <ds:X509Data>
          <ds:X509Certificate>NTIyMzk0ZGI4MjI0ZjI5ZGNhYjkyOGQyZGQ1NTZjODViZjk5YTY4ODFjOWRjNjkyYzZmODY2ZDQ4NjlkZjY3YSAgLQo=</ds:X509Certificate>
        </ds:X509Data>
      </KeyInfo>
    </ds:Signature>
    <Subject>
      <NameID Format="urn:oasis:names:tc:SAML:2.0:nameid-format:persistent">{username}</NameID>
      <SubjectConfirmation Method="urn:oasis:names:tc:SAML:2.0:cm:bearer">
        <SubjectConfirmationData NotOnOrAfter="2012-01-01T13:00:00.000Z" Recipient="https://signin.aws.amazon.com/saml"/>
      </SubjectConfirmation>
    </Subject>
    <Conditions NotBefore="2012-01-01T12:00:00.000Z" NotOnOrAfter="2012-01-01T13:00:00.000Z">
      <AudienceRestriction>
        <Audience>urn:amazon:webservices</Audience>
      </AudienceRestriction>
    </Conditions>
    <AttributeStatement>
      <Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName">
        <AttributeValue>{username}@localhost</AttributeValue>
      </Attribute>
      <Attribute Name="https://aws.amazon.com/SAML/Attributes/Role">
        <AttributeValue>arn:aws:iam::{account_id}:saml-provider/{provider_name},arn:aws:iam::{account_id}:role/{role_name}</AttributeValue>
      </Attribute>
      <Attribute Name="https://aws.amazon.com/SAML/Attributes/SessionDuration">
        <AttributeValue>900</AttributeValue>
      </Attribute>
    </AttributeStatement>
    <AuthnStatement AuthnInstant="2012-01-01T12:00:00.000Z" SessionIndex="_00000000-0000-0000-0000-000000000000">
      <AuthnContext>
        <AuthnContextClassRef>urn:oasis:names:tc:SAML:2.0:ac:classes:PasswordProtectedTransport</AuthnContextClassRef>
      </AuthnContext>
    </AuthnStatement>
  </Assertion>
</samlp:Response>""".format(  # noqa: E501
    account_id=ACCOUNT_ID,
    role_name="test-role",
    provider_name="TestProvFed",
    username="testuser",
).replace(
    "\n", ""
)


class TestAwsBaseHook(unittest.TestCase):
    @unittest.skipIf(mock_emr is None, 'mock_emr package not present')
    @mock_emr
    def test_get_client_type_returns_a_boto3_client_of_the_requested_type(self):
        client = boto3.client('emr', region_name='us-east-1')
        if client.list_clusters()['Clusters']:
            raise ValueError('AWS not properly mocked')

        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='emr')
        client_from_hook = hook.get_client_type('emr')

        assert client_from_hook.list_clusters()['Clusters'] == []

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_resource_type_returns_a_boto3_resource_of_the_requested_type(self):
        hook = AwsBaseHook(aws_conn_id='aws_default', resource_type='dynamodb')
        resource_from_hook = hook.get_resource_type('dynamodb')

        # this table needs to be created in production
        table = resource_from_hook.create_table(
            TableName='test_airflow',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10},
        )

        table.meta.client.get_waiter('table_exists').wait(TableName='test_airflow')

        assert table.item_count == 0

    @unittest.skipIf(mock_dynamodb2 is None, 'mock_dynamo2 package not present')
    @mock_dynamodb2
    def test_get_session_returns_a_boto3_session(self):
        hook = AwsBaseHook(aws_conn_id='aws_default', resource_type='dynamodb')
        session_from_hook = hook.get_session()
        resource_from_session = session_from_hook.resource('dynamodb')
        table = resource_from_session.create_table(
            TableName='test_airflow',
            KeySchema=[
                {'AttributeName': 'id', 'KeyType': 'HASH'},
            ],
            AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
            ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10},
        )

        table.meta.client.get_waiter('table_exists').wait(TableName='test_airflow')

        assert table.item_count == 0

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
        assert credentials_from_hook.access_key == 'aws_access_key_id'
        assert credentials_from_hook.secret_key == 'aws_secret_access_key'
        assert credentials_from_hook.token == 'test_token'

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_login_without_token(self, mock_get_connection):
        mock_connection = Connection(
            login='aws_access_key_id',
            password='aws_secret_access_key',
        )

        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='spam')
        credentials_from_hook = hook.get_credentials()
        assert credentials_from_hook.access_key == 'aws_access_key_id'
        assert credentials_from_hook.secret_key == 'aws_secret_access_key'
        assert credentials_from_hook.token is None

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
        assert credentials_from_hook.access_key == 'aws_access_key_id'
        assert credentials_from_hook.secret_key == 'aws_secret_access_key'
        assert credentials_from_hook.token == 'session_token'

    @mock.patch.object(AwsBaseHook, 'get_connection')
    def test_get_credentials_from_extra_without_token(self, mock_get_connection):
        mock_connection = Connection(
            extra='{"aws_access_key_id": "aws_access_key_id",'
            '"aws_secret_access_key": "aws_secret_access_key"}'
        )
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        assert credentials_from_hook.access_key == 'aws_access_key_id'
        assert credentials_from_hook.secret_key == 'aws_secret_access_key'
        assert credentials_from_hook.token is None

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
    def test_assume_role(self, mock_get_connection):
        aws_conn_id = 'aws/test'
        role_arn = 'arn:aws:iam::123456:role/role_arn'
        slugified_role_session_name = 'airflow_aws-test'

        mock_connection = Connection(
            conn_id=aws_conn_id,
            extra=json.dumps(
                {
                    "role_arn": role_arn,
                }
            ),
        )
        mock_get_connection.return_value = mock_connection

        def mock_assume_role(**kwargs):
            assert kwargs['RoleArn'] == role_arn
            # The role session name gets invalid characters removed/replaced with hyphens
            # (e.g. / is replaced with -)
            assert kwargs['RoleSessionName'] == slugified_role_session_name
            sts_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Credentials': {
                    'Expiration': datetime.now(),
                    'AccessKeyId': 1,
                    'SecretAccessKey': 1,
                    'SessionToken': 1,
                },
            }
            return sts_response

        with mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws.requests.Session.get'
        ) as mock_get, mock.patch('airflow.providers.amazon.aws.hooks.base_aws.boto3') as mock_boto3:
            mock_get.return_value.ok = True

            mock_client = mock_boto3.session.Session.return_value.client
            mock_client.return_value.assume_role.side_effect = mock_assume_role

            hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type='s3')
            hook.get_client_type('s3')

        calls_assume_role = [
            mock.call.session.Session().client('sts', config=None),
            mock.call.session.Session()
            .client()
            .assume_role(
                RoleArn=role_arn,
                RoleSessionName=slugified_role_session_name,
            ),
        ]
        mock_boto3.assert_has_calls(calls_assume_role)

    @unittest.skipIf(mock_sts is None, 'mock_sts package not present')
    @mock.patch.object(AwsBaseHook, 'get_connection')
    @mock_sts
    def test_get_credentials_from_role_arn(self, mock_get_connection):
        mock_connection = Connection(extra='{"role_arn":"arn:aws:iam::123456:role/role_arn"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        credentials_from_hook = hook.get_credentials()
        assert "ASIA" in credentials_from_hook.access_key

        # We assert the length instead of actual values as the values are random:
        # Details: https://github.com/spulec/moto/commit/ab0d23a0ba2506e6338ae20b3fde70da049f7b03
        assert 20 == len(credentials_from_hook.access_key)
        assert 40 == len(credentials_from_hook.secret_key)
        assert 356 == len(credentials_from_hook.token)

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
            assert (
                mock_get_credentials.return_value.get_frozen_credentials.return_value == credentials_from_hook
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
                mock.call.session.Session(botocore_session=mock_session.get_session.return_value),
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

        mock_session.assert_has_calls(
            [
                mock.call.get_session(),
                mock.call.get_session().set_config_variable(
                    'region', mock_boto3.session.Session.return_value.region_name
                ),
            ]
        )
        mock_id_token_credentials.assert_has_calls(
            [mock.call.get_default_id_token_credentials(target_audience='aws-federation.airflow.apache.org')]
        )

    @unittest.skipIf(mock_sts is None, 'mock_sts package not present')
    @mock.patch.object(AwsBaseHook, 'get_connection')
    @mock_sts
    def test_assume_role_with_saml(self, mock_get_connection):

        idp_url = "https://my-idp.local.corp"
        principal_arn = "principal_arn_1234567890"
        role_arn = "arn:aws:iam::123456:role/role_arn"
        xpath = "1234"
        duration_seconds = 901

        mock_connection = Connection(
            extra=json.dumps(
                {
                    "role_arn": role_arn,
                    "assume_role_method": "assume_role_with_saml",
                    "assume_role_with_saml": {
                        "principal_arn": principal_arn,
                        "idp_url": idp_url,
                        "idp_auth_method": "http_spegno_auth",
                        "mutual_authentication": "REQUIRED",
                        "saml_response_xpath": xpath,
                        "log_idp_response": True,
                    },
                    "assume_role_kwargs": {"DurationSeconds": duration_seconds},
                }
            )
        )
        mock_get_connection.return_value = mock_connection

        encoded_saml_assertion = b64encode(SAML_ASSERTION.encode("utf-8")).decode("utf-8")

        # Store original __import__
        orig_import = __import__
        mock_requests_gssapi = mock.Mock()
        mock_auth = mock_requests_gssapi.HTTPSPNEGOAuth()

        mock_lxml = mock.Mock()
        mock_xpath = mock_lxml.etree.fromstring.return_value.xpath
        mock_xpath.return_value = encoded_saml_assertion

        def import_mock(name, *args, **kwargs):
            if name == 'requests_gssapi':
                return mock_requests_gssapi
            if name == 'lxml':
                return mock_lxml
            return orig_import(name, *args, **kwargs)

        def mock_assume_role_with_saml(**kwargs):
            assert kwargs['RoleArn'] == role_arn
            assert kwargs['PrincipalArn'] == principal_arn
            assert kwargs['SAMLAssertion'] == encoded_saml_assertion
            assert kwargs['DurationSeconds'] == duration_seconds
            sts_response = {
                'ResponseMetadata': {'HTTPStatusCode': 200},
                'Credentials': {
                    'Expiration': datetime.now(),
                    'AccessKeyId': 1,
                    'SecretAccessKey': 1,
                    'SessionToken': 1,
                },
            }
            return sts_response

        with mock.patch('builtins.__import__', side_effect=import_mock), mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws.requests.Session.get'
        ) as mock_get, mock.patch('airflow.providers.amazon.aws.hooks.base_aws.boto3') as mock_boto3:
            mock_get.return_value.ok = True

            mock_client = mock_boto3.session.Session.return_value.client
            mock_client.return_value.assume_role_with_saml.side_effect = mock_assume_role_with_saml

            hook = AwsBaseHook(aws_conn_id='aws_default', client_type='s3')
            hook.get_client_type('s3')

            mock_get.assert_called_once_with(idp_url, auth=mock_auth)
            mock_xpath.assert_called_once_with(xpath)

        calls_assume_role_with_saml = [
            mock.call.session.Session().client('sts', config=None),
            mock.call.session.Session()
            .client()
            .assume_role_with_saml(
                DurationSeconds=duration_seconds,
                PrincipalArn=principal_arn,
                RoleArn=role_arn,
                SAMLAssertion=encoded_saml_assertion,
            ),
        ]
        mock_boto3.assert_has_calls(calls_assume_role_with_saml)

    @unittest.skipIf(mock_iam is None, 'mock_iam package not present')
    @mock_iam
    def test_expand_role(self):
        conn = boto3.client('iam', region_name='us-east-1')
        conn.create_role(RoleName='test-role', AssumeRolePolicyDocument='some policy')
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')
        arn = hook.expand_role('test-role')
        expect_arn = conn.get_role(RoleName='test-role').get('Role').get('Arn')
        assert arn == expect_arn

    def test_use_default_boto3_behaviour_without_conn_id(self):
        for conn_id in (None, ''):
            hook = AwsBaseHook(aws_conn_id=conn_id, client_type='s3')
            # should cause no exception
            hook.get_client_type('s3')

    @unittest.skipIf(mock_sts is None, 'mock_sts package not present')
    @mock.patch.object(AwsBaseHook, 'get_connection')
    @mock_sts
    def test_refreshable_credentials(self, mock_get_connection):
        role_arn = "arn:aws:iam::123456:role/role_arn"
        conn_id = "F5"
        mock_connection = Connection(conn_id=conn_id, extra='{"role_arn":"' + role_arn + '"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id='aws_default', client_type='airflow_test')

        expire_on_calls = []

        def mock_refresh_credentials():
            expiry_datetime = datetime.now(timezone.utc)
            expire_on_call = expire_on_calls.pop()
            if expire_on_call:
                expiry_datetime -= timedelta(minutes=1000)
            else:
                expiry_datetime += timedelta(minutes=1000)
            credentials = {
                "access_key": "1",
                "secret_key": "2",
                "token": "3",
                "expiry_time": expiry_datetime.isoformat(),
            }
            return credentials

        # Test with credentials that have not expired
        expire_on_calls = [False]
        with mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws._SessionFactory._refresh_credentials'
        ) as mock_refresh:
            mock_refresh.side_effect = mock_refresh_credentials
            client = hook.get_client_type('sts')
            assert mock_refresh.call_count == 1
            client.get_caller_identity()
            assert mock_refresh.call_count == 1
            client.get_caller_identity()
            assert mock_refresh.call_count == 1
            assert len(expire_on_calls) == 0

        # Test with credentials that have expired
        expire_on_calls = [False, True]
        with mock.patch(
            'airflow.providers.amazon.aws.hooks.base_aws._SessionFactory._refresh_credentials'
        ) as mock_refresh:
            mock_refresh.side_effect = mock_refresh_credentials
            client = hook.get_client_type('sts')
            client.get_caller_identity()
            assert mock_refresh.call_count == 2
            client.get_caller_identity()
            assert mock_refresh.call_count == 2
            assert len(expire_on_calls) == 0


class ThrowErrorUntilCount:
    """Holds counter state for invoking a method several times in a row."""

    def __init__(self, count, quota_retry, **kwargs):
        self.counter = 0
        self.count = count
        self.retry_args = quota_retry
        self.kwargs = kwargs
        self.log = None

    def __call__(self):
        """
        Raise an Forbidden until after count threshold has been crossed.
        Then return True.
        """
        if self.counter < self.count:
            self.counter += 1
            raise Exception()
        return True


def _always_true_predicate(e: Exception):
    return True


@AwsBaseHook.retry(_always_true_predicate)
def _retryable_test(thing):
    return thing()


def _always_false_predicate(e: Exception):
    return False


@AwsBaseHook.retry(_always_false_predicate)
def _non_retryable_test(thing):
    return thing()


class TestRetryDecorator(unittest.TestCase):  # ptlint: disable=invalid-name
    def test_do_nothing_on_non_exception(self):
        result = _retryable_test(lambda: 42)
        assert result, 42

    def test_retry_on_exception(self):
        quota_retry = {
            'stop_after_delay': 2,
            'multiplier': 1,
            'min': 1,
            'max': 10,
        }
        custom_fn = ThrowErrorUntilCount(
            count=2,
            quota_retry=quota_retry,
        )
        result = _retryable_test(custom_fn)
        assert custom_fn.counter == 2
        assert result

    def test_no_retry_on_exception(self):
        quota_retry = {
            'stop_after_delay': 2,
            'multiplier': 1,
            'min': 1,
            'max': 10,
        }
        custom_fn = ThrowErrorUntilCount(
            count=2,
            quota_retry=quota_retry,
        )
        with pytest.raises(Exception):
            _non_retryable_test(custom_fn)

    def test_raise_exception_when_no_retry_args(self):
        custom_fn = ThrowErrorUntilCount(count=2, quota_retry=None)
        with pytest.raises(Exception):
            _retryable_test(custom_fn)
