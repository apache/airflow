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

import json
import os
from base64 import b64encode
from datetime import datetime, timedelta, timezone
from unittest import mock
from uuid import UUID

import boto3
import pytest
from botocore.config import Config
from botocore.credentials import ReadOnlyCredentials
from botocore.exceptions import NoCredentialsError
from moto import mock_dynamodb, mock_emr, mock_iam, mock_sts
from moto.core import DEFAULT_ACCOUNT_ID

from airflow.models.connection import Connection
from airflow.providers.amazon.aws.hooks.base_aws import (
    AwsBaseHook,
    BaseSessionFactory,
    resolve_session_factory,
)
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper
from tests.test_utils.config import conf_vars

MOCK_AWS_CONN_ID = "mock-conn-id"
MOCK_CONN_TYPE = "aws"
MOCK_BOTO3_SESSION = mock.MagicMock(return_value="Mock boto3.session.Session")


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
    account_id=DEFAULT_ACCOUNT_ID,
    role_name="test-role",
    provider_name="TestProvFed",
    username="testuser",
).replace(
    "\n", ""
)


class CustomSessionFactory(BaseSessionFactory):
    def create_session(self):
        return mock.MagicMock()


@pytest.fixture
def mock_conn(request):
    conn = Connection(conn_type=MOCK_CONN_TYPE, conn_id=MOCK_AWS_CONN_ID)
    if request.param == "unwrapped":
        return conn
    if request.param == "wrapped":
        return AwsConnectionWrapper(conn=conn)
    else:
        raise ValueError("invalid internal test config")


class TestSessionFactory:
    @conf_vars(
        {("aws", "session_factory"): "tests.providers.amazon.aws.hooks.test_base_aws.CustomSessionFactory"}
    )
    def test_resolve_session_factory_class(self):
        cls = resolve_session_factory()
        assert issubclass(cls, CustomSessionFactory)

    @conf_vars({("aws", "session_factory"): ""})
    def test_resolve_session_factory_class_fallback_to_base_session_factory(self):
        cls = resolve_session_factory()
        assert issubclass(cls, BaseSessionFactory)

    def test_resolve_session_factory_class_fallback_to_base_session_factory_no_config(self):
        cls = resolve_session_factory()
        assert issubclass(cls, BaseSessionFactory)

    @pytest.mark.parametrize("mock_conn", ["unwrapped", "wrapped"], indirect=True)
    def test_conn_property(self, mock_conn):
        sf = BaseSessionFactory(conn=mock_conn, region_name=None, config=None)
        session_factory_conn = sf.conn
        assert isinstance(session_factory_conn, AwsConnectionWrapper)
        assert session_factory_conn.conn_id == MOCK_AWS_CONN_ID
        assert session_factory_conn.conn_type == MOCK_CONN_TYPE
        assert sf.conn is session_factory_conn

    def test_empty_conn_property(self):
        sf = BaseSessionFactory(conn=None, region_name=None, config=None)
        assert isinstance(sf.conn, AwsConnectionWrapper)

    @pytest.mark.parametrize(
        "region_name,conn_region_name",
        [
            ("eu-west-1", "cn-north-1"),
            ("eu-west-1", None),
            (None, "cn-north-1"),
            (None, None),
        ],
    )
    def test_resolve_region_name(self, region_name, conn_region_name):
        conn = AwsConnectionWrapper(
            conn=Connection(conn_type=MOCK_CONN_TYPE, conn_id=MOCK_AWS_CONN_ID),
            region_name=conn_region_name,
        )
        sf = BaseSessionFactory(conn=conn, region_name=region_name, config=None)
        expected = region_name or conn_region_name
        assert sf.region_name == expected

    @pytest.mark.parametrize(
        "botocore_config, conn_botocore_config",
        [
            (Config(s3={"us_east_1_regional_endpoint": "regional"}), None),
            (Config(s3={"us_east_1_regional_endpoint": "regional"}), Config(region_name="ap-southeast-1")),
            (None, Config(region_name="ap-southeast-1")),
            (None, None),
        ],
    )
    def test_resolve_botocore_config(self, botocore_config, conn_botocore_config):
        conn = AwsConnectionWrapper(
            conn=Connection(conn_type=MOCK_CONN_TYPE, conn_id=MOCK_AWS_CONN_ID),
            botocore_config=conn_botocore_config,
        )
        sf = BaseSessionFactory(conn=conn, config=botocore_config)
        expected = botocore_config or conn_botocore_config
        assert sf.config == expected

    @pytest.mark.parametrize("region_name", ["eu-central-1", None])
    @mock.patch("boto3.session.Session", new_callable=mock.PropertyMock, return_value=MOCK_BOTO3_SESSION)
    def test_create_session_boto3_credential_strategy(self, mock_boto3_session, region_name, caplog):
        sf = BaseSessionFactory(conn=AwsConnectionWrapper(conn=None), region_name=region_name, config=None)
        session = sf.create_session()
        mock_boto3_session.assert_called_once_with(region_name=region_name)
        assert session == MOCK_BOTO3_SESSION
        logging_message = "No connection ID provided. Fallback on boto3 credential strategy"
        assert any(logging_message in log_text for log_text in caplog.messages)

    @pytest.mark.parametrize("region_name", ["eu-central-1", None])
    @pytest.mark.parametrize("profile_name", ["default", None])
    @mock.patch("boto3.session.Session", new_callable=mock.PropertyMock, return_value=MOCK_BOTO3_SESSION)
    def test_create_session_from_credentials(self, mock_boto3_session, region_name, profile_name):
        mock_conn = Connection(
            conn_type=MOCK_CONN_TYPE, conn_id=MOCK_AWS_CONN_ID, extra={"profile_name": profile_name}
        )
        mock_conn_config = AwsConnectionWrapper(conn=mock_conn)
        sf = BaseSessionFactory(conn=mock_conn_config, region_name=region_name, config=None)
        session = sf.create_session()

        expected_arguments = mock_conn_config.session_kwargs
        if region_name:
            expected_arguments["region_name"] = region_name
        mock_boto3_session.assert_called_once_with(**expected_arguments)
        assert session == MOCK_BOTO3_SESSION

    @mock_sts
    @pytest.mark.parametrize(
        "conn_id, conn_extra",
        [
            (
                "assume-with-initial-creds",
                {
                    "aws_access_key_id": "mock_aws_access_key_id",
                    "aws_secret_access_key": "mock_aws_access_key_id",
                    "aws_session_token": "mock_aws_session_token",
                },
            ),
            ("assume-without-initial-creds", {}),
        ],
    )
    @pytest.mark.parametrize("region_name", ["ap-southeast-2", "sa-east-1"])
    def test_get_credentials_from_role_arn(self, conn_id, conn_extra, region_name):
        """Test creation session which set role_arn extra in connection."""
        extra = {
            **conn_extra,
            "role_arn": "arn:aws:iam::123456:role/role_arn",
            "region_name": region_name,
        }
        conn = AwsConnectionWrapper.from_connection_metadata(conn_id=conn_id, extra=extra)
        sf = BaseSessionFactory(conn=conn)
        session = sf.create_session()
        assert session.region_name == region_name
        # Validate method of botocore credentials provider.
        # It shouldn't be 'explicit' which refers in this case to initial credentials.
        assert session.get_credentials().method == "sts-assume-role"


class TestAwsBaseHook:
    @mock_emr
    def test_get_client_type_set_in_class_attribute(self):
        client = boto3.client("emr", region_name="us-east-1")
        if client.list_clusters()["Clusters"]:
            raise ValueError("AWS not properly mocked")
        hook = AwsBaseHook(aws_conn_id="aws_default", client_type="emr")
        client_from_hook = hook.get_client_type()

        assert client_from_hook.list_clusters()["Clusters"] == []

    @mock_dynamodb
    def test_get_resource_type_set_in_class_attribute(self):
        hook = AwsBaseHook(aws_conn_id="aws_default", resource_type="dynamodb")
        resource_from_hook = hook.get_resource_type()

        # this table needs to be created in production
        table = resource_from_hook.create_table(
            TableName="test_airflow",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        table.meta.client.get_waiter("table_exists").wait(TableName="test_airflow")

        assert table.item_count == 0

    @mock_dynamodb
    def test_get_session_returns_a_boto3_session(self):
        hook = AwsBaseHook(aws_conn_id="aws_default", resource_type="dynamodb")
        session_from_hook = hook.get_session()
        resource_from_session = session_from_hook.resource("dynamodb")
        table = resource_from_session.create_table(
            TableName="test_airflow",
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
            ProvisionedThroughput={"ReadCapacityUnits": 10, "WriteCapacityUnits": 10},
        )

        table.meta.client.get_waiter("table_exists").wait(TableName="test_airflow")

        assert table.item_count == 0

    @pytest.mark.parametrize(
        "client_meta",
        [
            AwsBaseHook(client_type="s3").get_client_type().meta,
            AwsBaseHook(resource_type="dynamodb").get_resource_type().meta.client.meta,
        ],
    )
    def test_user_agent_extra_update(self, client_meta):
        """
        We are only looking for the keys appended by the AwsBaseHook. A user_agent string
        is a number of key/value pairs such as: `BOTO3/1.25.4 AIRFLOW/2.5.0.DEV0 AMPP/6.0.0`.
        """
        expected_user_agent_tag_keys = ["Airflow", "AmPP", "Caller", "DagRunKey"]

        result_user_agent_tags = client_meta.config.user_agent.split(" ")
        result_user_agent_tag_keys = [tag.split("/")[0].lower() for tag in result_user_agent_tags]

        for key in expected_user_agent_tag_keys:
            assert key.lower() in result_user_agent_tag_keys

    @staticmethod
    def fetch_tags() -> dict[str:str]:
        """Helper method which creates an AwsBaseHook and returns the user agent string split into a dict."""
        user_agent_string = AwsBaseHook(client_type="s3").get_client_type().meta.config.user_agent
        # Split the list of {Key}/{Value} into a dict
        return dict(tag.split("/") for tag in user_agent_string.split(" "))

    @pytest.mark.parametrize("found_classes", [["RandomOperator"], ["BaseSensorOperator", "TestSensor"]])
    @mock.patch.object(AwsBaseHook, "_find_class_name")
    def test_user_agent_caller_target_function_found(self, mock_class_name, found_classes):
        mock_class_name.side_effect = found_classes

        user_agent_tags = self.fetch_tags()

        assert mock_class_name.call_count == len(found_classes)
        assert user_agent_tags["Caller"] == found_classes[-1]

    def test_user_agent_caller_target_function_not_found(self):
        default_caller_name = "Unknown"

        user_agent_tags = self.fetch_tags()

        assert user_agent_tags["Caller"] == default_caller_name

    @pytest.mark.parametrize("env_var, expected_version", [({"AIRFLOW_CTX_DAG_ID": "banana"}, 5), [{}, None]])
    @mock.patch.object(AwsBaseHook, "_get_caller", return_value="Test")
    def test_user_agent_dag_run_key_is_hashed_correctly(self, _, env_var, expected_version):
        with mock.patch.dict(os.environ, env_var, clear=True):
            dag_run_key = self.fetch_tags()["DagRunKey"]

            assert UUID(dag_run_key).version == expected_version

    @mock.patch.object(AwsBaseHook, "get_connection")
    @mock_sts
    def test_assume_role(self, mock_get_connection):
        aws_conn_id = "aws/test"
        role_arn = "arn:aws:iam::123456:role/role_arn"
        slugified_role_session_name = "airflow_aws-test"

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
            assert kwargs["RoleArn"] == role_arn
            # The role session name gets invalid characters removed/replaced with hyphens
            # (e.g. / is replaced with -)
            assert kwargs["RoleSessionName"] == slugified_role_session_name
            sts_response = {
                "ResponseMetadata": {"HTTPStatusCode": 200},
                "Credentials": {
                    "Expiration": datetime.now(),
                    "AccessKeyId": 1,
                    "SecretAccessKey": 1,
                    "SessionToken": 1,
                },
            }
            return sts_response

        with mock.patch(
            "airflow.providers.amazon.aws.hooks.base_aws.requests.Session.get"
        ) as mock_get, mock.patch("airflow.providers.amazon.aws.hooks.base_aws.boto3") as mock_boto3:
            mock_get.return_value.ok = True

            mock_client = mock_boto3.session.Session.return_value.client
            mock_client.return_value.assume_role.side_effect = mock_assume_role

            hook = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="s3")
            hook.get_client_type("s3")

        calls_assume_role = [
            mock.call.session.Session().client("sts", config=mock.ANY),
            mock.call.session.Session()
            .client()
            .assume_role(
                RoleArn=role_arn,
                RoleSessionName=slugified_role_session_name,
            ),
        ]
        mock_boto3.assert_has_calls(calls_assume_role)

    def test_get_credentials_from_gcp_credentials(self):
        mock_connection = Connection(
            extra=json.dumps(
                {
                    "role_arn": "arn:aws:iam::123456:role/role_arn",
                    "assume_role_method": "assume_role_with_web_identity",
                    "assume_role_with_web_identity_federation": "google",
                    "assume_role_with_web_identity_federation_audience": "aws-federation.airflow.apache.org",
                }
            )
        )
        mock_connection.conn_type = "aws"

        # Store original __import__
        orig_import = __import__
        mock_id_token_credentials = mock.Mock()

        def import_mock(name, *args):
            if name == "airflow.providers.google.common.utils.id_token_credentials":
                return mock_id_token_credentials
            return orig_import(name, *args)

        with mock.patch("builtins.__import__", side_effect=import_mock), mock.patch.dict(
            "os.environ", AIRFLOW_CONN_AWS_DEFAULT=mock_connection.get_uri()
        ), mock.patch("airflow.providers.amazon.aws.hooks.base_aws.boto3") as mock_boto3, mock.patch(
            "airflow.providers.amazon.aws.hooks.base_aws.botocore"
        ) as mock_botocore, mock.patch(
            "airflow.providers.amazon.aws.hooks.base_aws.botocore.session"
        ) as mock_session:
            hook = AwsBaseHook(aws_conn_id="aws_default", client_type="airflow_test")

            credentials_from_hook = hook.get_credentials()
            mock_get_credentials = mock_boto3.session.Session.return_value.get_credentials
            assert (
                mock_get_credentials.return_value.get_frozen_credentials.return_value == credentials_from_hook
            )

        mock_boto3.assert_has_calls(
            [
                mock.call.session.Session(),
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
                    role_arn="arn:aws:iam::123456:role/role_arn",
                    web_identity_token_loader=mock.ANY,
                ),
                mock.call.credentials.DeferredRefreshableCredentials(
                    method="assume-role-with-web-identity",
                    refresh_using=mock_fetcher.return_value.fetch_credentials,
                    time_fetcher=mock.ANY,
                ),
            ]
        )

        mock_session.assert_has_calls(
            [
                mock.call.get_session(),
                mock.call.get_session().set_config_variable(
                    "region", mock_boto3.session.Session.return_value.region_name
                ),
            ]
        )
        mock_id_token_credentials.assert_has_calls(
            [mock.call.get_default_id_token_credentials(target_audience="aws-federation.airflow.apache.org")]
        )

    @mock.patch.object(AwsBaseHook, "get_connection")
    @mock_sts
    def test_assume_role_with_saml(self, mock_get_connection):

        idp_url = "https://my-idp.local.corp"
        principal_arn = "principal_arn_1234567890"
        role_arn = "arn:aws:iam::123456:role/role_arn"
        xpath = "1234"
        duration_seconds = 901

        mock_connection = Connection(
            conn_id=MOCK_AWS_CONN_ID,
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
            ),
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
            if name == "requests_gssapi":
                return mock_requests_gssapi
            if name == "lxml":
                return mock_lxml
            return orig_import(name, *args, **kwargs)

        def mock_assume_role_with_saml(**kwargs):
            assert kwargs["RoleArn"] == role_arn
            assert kwargs["PrincipalArn"] == principal_arn
            assert kwargs["SAMLAssertion"] == encoded_saml_assertion
            assert kwargs["DurationSeconds"] == duration_seconds
            sts_response = {
                "ResponseMetadata": {"HTTPStatusCode": 200},
                "Credentials": {
                    "Expiration": datetime.now(),
                    "AccessKeyId": 1,
                    "SecretAccessKey": 1,
                    "SessionToken": 1,
                },
            }
            return sts_response

        with mock.patch("builtins.__import__", side_effect=import_mock), mock.patch(
            "airflow.providers.amazon.aws.hooks.base_aws.requests.Session.get"
        ) as mock_get, mock.patch("airflow.providers.amazon.aws.hooks.base_aws.boto3") as mock_boto3:
            mock_get.return_value.ok = True

            mock_client = mock_boto3.session.Session.return_value.client
            mock_client.return_value.assume_role_with_saml.side_effect = mock_assume_role_with_saml

            hook = AwsBaseHook(aws_conn_id="aws_default", client_type="s3")
            hook.get_client_type("s3")

            mock_get.assert_called_once_with(idp_url, auth=mock_auth)
            mock_xpath.assert_called_once_with(xpath)

        calls_assume_role_with_saml = [
            mock.call.session.Session().client("sts", config=mock.ANY),
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

    @mock_iam
    def test_expand_role(self):
        conn = boto3.client("iam", region_name="us-east-1")
        conn.create_role(RoleName="test-role", AssumeRolePolicyDocument="some policy")
        hook = AwsBaseHook(aws_conn_id="aws_default", client_type="airflow_test")
        arn = hook.expand_role("test-role")
        expect_arn = conn.get_role(RoleName="test-role").get("Role").get("Arn")
        assert arn == expect_arn

    def test_use_default_boto3_behaviour_without_conn_id(self):
        for conn_id in (None, ""):
            hook = AwsBaseHook(aws_conn_id=conn_id, client_type="s3")
            # should cause no exception
            hook.get_client_type("s3")

    @mock.patch.object(AwsBaseHook, "get_connection")
    @mock_sts
    def test_refreshable_credentials(self, mock_get_connection):
        role_arn = "arn:aws:iam::123456:role/role_arn"
        conn_id = "F5"
        mock_connection = Connection(conn_id=conn_id, extra='{"role_arn":"' + role_arn + '"}')
        mock_get_connection.return_value = mock_connection
        hook = AwsBaseHook(aws_conn_id="aws_default", client_type="sts")

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
            "airflow.providers.amazon.aws.hooks.base_aws.BaseSessionFactory._refresh_credentials"
        ) as mock_refresh:
            mock_refresh.side_effect = mock_refresh_credentials
            client = hook.get_client_type()
            assert mock_refresh.call_count == 1
            client.get_caller_identity()
            assert mock_refresh.call_count == 1
            client.get_caller_identity()
            assert mock_refresh.call_count == 1
            assert len(expire_on_calls) == 0

        # Test with credentials that have expired
        expire_on_calls = [False, True]
        with mock.patch(
            "airflow.providers.amazon.aws.hooks.base_aws.BaseSessionFactory._refresh_credentials"
        ) as mock_refresh:
            mock_refresh.side_effect = mock_refresh_credentials
            client = hook.get_client_type("sts")
            client.get_caller_identity()
            assert mock_refresh.call_count == 2
            client.get_caller_identity()
            assert mock_refresh.call_count == 2
            assert len(expire_on_calls) == 0

    @mock_dynamodb
    @pytest.mark.parametrize("conn_type", ["client", "resource"])
    @pytest.mark.parametrize(
        "connection_uri,region_name,env_region,expected_region_name",
        [
            ("aws://?region_name=eu-west-1", None, "", "eu-west-1"),
            ("aws://?region_name=eu-west-1", "cn-north-1", "", "cn-north-1"),
            ("aws://?region_name=eu-west-1", None, "us-east-2", "eu-west-1"),
            ("aws://?region_name=eu-west-1", "cn-north-1", "us-gov-east-1", "cn-north-1"),
            ("aws://?", "cn-north-1", "us-gov-east-1", "cn-north-1"),
            ("aws://?", None, "us-gov-east-1", "us-gov-east-1"),
        ],
    )
    def test_connection_region_name(
        self, conn_type, connection_uri, region_name, env_region, expected_region_name
    ):
        with mock.patch.dict(
            "os.environ", AIRFLOW_CONN_TEST_CONN=connection_uri, AWS_DEFAULT_REGION=env_region
        ):
            if conn_type == "client":
                hook = AwsBaseHook(aws_conn_id="test_conn", region_name=region_name, client_type="dynamodb")
            elif conn_type == "resource":
                hook = AwsBaseHook(aws_conn_id="test_conn", region_name=region_name, resource_type="dynamodb")
            else:
                raise ValueError(f"Unsupported conn_type={conn_type!r}")

            assert hook.conn_region_name == expected_region_name

    @mock_dynamodb
    @pytest.mark.parametrize("conn_type", ["client", "resource"])
    @pytest.mark.parametrize(
        "connection_uri,expected_partition",
        [
            ("aws://?region_name=eu-west-1", "aws"),
            ("aws://?region_name=cn-north-1", "aws-cn"),
            ("aws://?region_name=us-gov-east-1", "aws-us-gov"),
        ],
    )
    def test_connection_aws_partition(self, conn_type, connection_uri, expected_partition):
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=connection_uri):
            if conn_type == "client":
                hook = AwsBaseHook(aws_conn_id="test_conn", client_type="dynamodb")
            elif conn_type == "resource":
                hook = AwsBaseHook(aws_conn_id="test_conn", resource_type="dynamodb")
            else:
                raise ValueError(f"Unsupported conn_type={conn_type!r}")

            assert hook.conn_partition == expected_partition

    @pytest.mark.parametrize(
        "client_type,resource_type",
        [
            ("s3", "dynamodb"),
            (None, None),
            ("", ""),
        ],
    )
    def test_connection_client_resource_types_check(self, client_type, resource_type):
        # Should not raise any error during Hook initialisation.
        hook = AwsBaseHook(aws_conn_id=None, client_type=client_type, resource_type=resource_type)

        with pytest.raises(ValueError, match="Either client_type=.* or resource_type=.* must be provided"):
            hook.get_conn()

    @mock_sts
    def test_hook_connection_test(self):
        hook = AwsBaseHook(client_type="s3")
        result, message = hook.test_connection()
        assert result
        assert hook.client_type == "s3"  # Same client_type which defined during initialisation

    @mock.patch("boto3.session.Session")
    def test_hook_connection_test_failed(self, mock_boto3_session):
        """Test ``test_connection`` failure."""
        hook = AwsBaseHook(client_type="ec2")

        # Tests that STS API return non 200 code. Under normal circumstances this is hardly possible.
        response_metadata = {"HTTPStatusCode": 500, "reason": "Test Failure"}
        mock_sts_client = mock.MagicMock()
        mock_sts_client.return_value.get_caller_identity.return_value = {
            "ResponseMetadata": response_metadata
        }
        mock_boto3_session.return_value.client = mock_sts_client
        result, message = hook.test_connection()
        assert not result
        assert message == json.dumps(response_metadata)
        mock_sts_client.assert_called_once_with("sts")

        def mock_error():
            raise ConnectionError("Test Error")

        # Something bad happen during boto3.session.Session creation (e.g. wrong credentials or conn error)
        mock_boto3_session.reset_mock()
        mock_boto3_session.side_effect = mock_error
        result, message = hook.test_connection()
        assert not result
        assert message == "'ConnectionError' error occurred while testing connection: Test Error"

        assert hook.client_type == "ec2"

    @mock.patch.dict(os.environ, {f"AIRFLOW_CONN_{MOCK_AWS_CONN_ID.upper()}": "aws://"})
    def test_conn_config_conn_id_exists(self):
        """Test retrieve connection config if aws_conn_id exists."""
        hook = AwsBaseHook(aws_conn_id=MOCK_AWS_CONN_ID)
        conn_config_exist = hook.conn_config
        assert conn_config_exist is hook.conn_config, "Expected cached Connection Config"
        assert isinstance(conn_config_exist, AwsConnectionWrapper)
        assert conn_config_exist

    @pytest.mark.parametrize("aws_conn_id", ["", None], ids=["empty", "None"])
    def test_conn_config_conn_id_empty(self, aws_conn_id):
        """Test retrieve connection config if aws_conn_id empty or None."""
        conn_config_empty = AwsBaseHook(aws_conn_id=aws_conn_id).conn_config
        assert isinstance(conn_config_empty, AwsConnectionWrapper)
        assert not conn_config_empty

    def test_conn_config_conn_id_not_exists(self):
        """Test fallback connection config if aws_conn_id not exists."""
        warning_message = (
            r"Unable to find AWS Connection ID '.*', switching to empty\. "
            r"This behaviour is deprecated and will be removed in a future releases"
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            conn_config_fallback_not_exists = AwsBaseHook(aws_conn_id="aws-conn-not-exists").conn_config
        assert isinstance(conn_config_fallback_not_exists, AwsConnectionWrapper)
        assert not conn_config_fallback_not_exists

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.SessionFactory")
    @pytest.mark.parametrize("hook_region_name", [None, "eu-west-1"])
    @pytest.mark.parametrize(
        "hook_botocore_config", [None, Config(s3={"us_east_1_regional_endpoint": "regional"})]
    )
    @pytest.mark.parametrize("method_region_name", [None, "cn-north-1"])
    def test_get_session(
        self, mock_session_factory, hook_region_name, hook_botocore_config, method_region_name
    ):
        """Test get boto3 Session by hook."""
        mock_session_factory_instance = mock_session_factory.return_value
        mock_session_factory_instance.create_session.return_value = MOCK_BOTO3_SESSION

        hook = AwsBaseHook(aws_conn_id=None, region_name=hook_region_name, config=hook_botocore_config)
        session = hook.get_session(region_name=method_region_name)
        mock_session_factory.assert_called_once_with(
            conn=hook.conn_config,
            region_name=method_region_name,
            config=mock.ANY,
        )
        assert mock_session_factory_instance.create_session.assert_called_once
        assert session == MOCK_BOTO3_SESSION

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook.get_session",
        return_value=MOCK_BOTO3_SESSION,
    )
    @pytest.mark.parametrize("region_name", [None, "aws-global", "eu-west-1"])
    def test_deprecate_private_method__get_credentials(self, mock_boto3_session, region_name):
        """Test deprecated method AwsGenericHook._get_credentials."""
        hook = AwsBaseHook(aws_conn_id=None)
        warning_message = (
            r"`AwsGenericHook._get_credentials` method deprecated and will be removed in a future releases\. "
            r"Please use `AwsGenericHook.get_session` method and "
            r"`AwsGenericHook.conn_config.endpoint_url` property instead\."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            session, endpoint = hook._get_credentials(region_name)

        mock_boto3_session.assert_called_once_with(region_name=region_name)
        assert session == MOCK_BOTO3_SESSION
        assert endpoint == hook.conn_config.endpoint_url

    @pytest.mark.parametrize("verify", [None, "path/to/cert/hook-bundle.pem", False])
    @pytest.mark.parametrize("conn_verify", [None, "path/to/cert/conn-bundle.pem", False])
    def test_resolve_verify(self, verify, conn_verify):
        mock_conn = Connection(
            conn_id="test_conn",
            conn_type="aws",
            extra={"verify": conn_verify} if conn_verify is not None else {},
        )

        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=mock_conn.get_uri()):
            hook = AwsBaseHook(aws_conn_id="test_conn", verify=verify)
            expected = verify if verify is not None else conn_verify
            assert hook.verify == expected

    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsGenericHook.get_session")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.mask_secret")
    @pytest.mark.parametrize("token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("secret_key", ["mock-aws-secret-access-key"])
    @pytest.mark.parametrize("access_key", ["mock-aws-access-key-id"])
    def test_get_credentials_mask_secrets(
        self, mock_mask_secret, mock_boto3_session, access_key, secret_key, token
    ):
        expected_credentials = ReadOnlyCredentials(access_key=access_key, secret_key=secret_key, token=token)
        mock_credentials = mock.MagicMock(return_value=expected_credentials)
        mock_boto3_session.return_value.get_credentials.return_value.get_frozen_credentials = mock_credentials
        expected_calls = [mock.call(secret_key)]
        if token:
            expected_calls.append(mock.call(token))

        hook = AwsBaseHook(aws_conn_id=None)
        credentials = hook.get_credentials()
        assert mock_mask_secret.mock_calls == expected_calls
        assert credentials == expected_credentials


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
        Raise an Exception until after count threshold has been crossed.
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


class TestRetryDecorator:  # ptlint: disable=invalid-name
    def test_do_nothing_on_non_exception(self):
        result = _retryable_test(lambda: 42)
        assert result, 42

    @mock.patch("time.sleep", return_value=0)
    def test_retry_on_exception(self, _):
        quota_retry = {
            "stop_after_delay": 2,
            "multiplier": 1,
            "min": 1,
            "max": 10,
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
            "stop_after_delay": 2,
            "multiplier": 1,
            "min": 1,
            "max": 10,
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


def test_raise_no_creds_default_credentials_strategy(tmp_path_factory, monkeypatch):
    """Test raise an error if no credentials provided and default boto3 strategy unable to get creds."""
    for env_key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        # Delete aws credentials environment variables
        monkeypatch.delenv(env_key, raising=False)

    hook = AwsBaseHook(aws_conn_id=None, client_type="sts")
    with pytest.raises(NoCredentialsError):
        # Call AWS STS API method GetCallerIdentity
        # which should return result in case of valid credentials
        result = hook.conn.get_caller_identity()
        # In normal circumstances lines below should not execute.
        # We want to show additional information why this test not passed
        assert not result, f"Credentials Method: {hook.get_session().get_credentials().method}"
