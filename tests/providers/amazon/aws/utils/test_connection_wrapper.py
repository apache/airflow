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

from dataclasses import fields
from typing import Optional
from unittest import mock

import pytest
from botocore.config import Config

from airflow.models import Connection
from airflow.providers.amazon.aws.utils.connection_wrapper import AwsConnectionWrapper

MOCK_AWS_CONN_ID = "mock-conn-id"
MOCK_CONN_TYPE = "aws"
MOCK_ROLE_ARN = "arn:aws:iam::222222222222:role/awesome-role"


def mock_connection_factory(
    conn_id: Optional[str] = MOCK_AWS_CONN_ID, conn_type: Optional[str] = MOCK_CONN_TYPE, **kwargs
) -> Connection:
    return Connection(conn_id=conn_id, conn_type=conn_type, **kwargs)


class TestAwsConnectionWrapper:
    @pytest.mark.parametrize("extra", [{"foo": "bar", "spam": "egg"}, '{"foo": "bar", "spam": "egg"}', None])
    def test_values_from_connection(self, extra):
        mock_conn = mock_connection_factory(
            login="mock-login",
            password="mock-password",
            extra=extra,
            # AwsBaseHook never use this attributes from airflow.models.Connection
            host="mock-host",
            schema="mock-schema",
            port=42,
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn)

        assert wrap_conn.conn_id == mock_conn.conn_id
        assert wrap_conn.conn_type == mock_conn.conn_type
        assert wrap_conn.login == mock_conn.login
        assert wrap_conn.password == mock_conn.password

        # Check that original extra config from connection persists in wrapper
        assert wrap_conn.extra_config == mock_conn.extra_dejson
        assert wrap_conn.extra_config is not mock_conn.extra_dejson
        # `extra_config` is a same object that return by `extra_dejson`
        assert wrap_conn.extra_config is wrap_conn.extra_dejson

        # Check that not assigned other attributes from airflow.models.Connection to wrapper
        assert not hasattr(wrap_conn, "host")
        assert not hasattr(wrap_conn, "schema")
        assert not hasattr(wrap_conn, "port")

        # Check that Wrapper is True if assign connection
        assert wrap_conn

    def test_no_connection(self):
        assert not AwsConnectionWrapper(conn=None)

    @pytest.mark.parametrize("conn_type", ["aws", None])
    def test_expected_aws_connection_type(self, conn_type):
        wrap_conn = AwsConnectionWrapper(conn=mock_connection_factory(conn_type=conn_type))
        assert wrap_conn.conn_type == "aws"

    @pytest.mark.parametrize("conn_type", ["AWS", "boto3", "s3", "emr", "google", "google-cloud-platform"])
    def test_unexpected_aws_connection_type(self, conn_type):
        warning_message = f"expected connection type 'aws', got '{conn_type}'"
        with pytest.warns(UserWarning, match=warning_message):
            wrap_conn = AwsConnectionWrapper(conn=mock_connection_factory(conn_type=conn_type))
            assert wrap_conn.conn_type == conn_type

    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("aws_secret_access_key", ["mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_access_key_id", ["mock-aws-access-key-id"])
    def test_get_credentials_from_login(self, aws_access_key_id, aws_secret_access_key, aws_session_token):
        mock_conn = mock_connection_factory(
            login=aws_access_key_id,
            password=aws_secret_access_key,
            extra={"aws_session_token": aws_session_token} if aws_session_token else None,
        )

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.aws_access_key_id == aws_access_key_id
        assert wrap_conn.aws_secret_access_key == aws_secret_access_key
        assert wrap_conn.aws_session_token == aws_session_token

    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("aws_secret_access_key", ["mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_access_key_id", ["mock-aws-access-key-id"])
    def test_get_credentials_from_extra(self, aws_access_key_id, aws_secret_access_key, aws_session_token):
        mock_conn_extra = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }
        if aws_session_token:
            mock_conn_extra["aws_session_token"] = aws_session_token
        mock_conn = mock_connection_factory(login=None, password=None, extra=mock_conn_extra)

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.aws_access_key_id == aws_access_key_id
        assert wrap_conn.aws_secret_access_key == aws_secret_access_key
        assert wrap_conn.aws_session_token == aws_session_token

    @pytest.mark.parametrize("aws_access_key_id", ["mock-aws-access-key-id"])
    @pytest.mark.parametrize("aws_secret_access_key", ["mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    def test_get_credentials_from_session_kwargs(
        self, aws_access_key_id, aws_secret_access_key, aws_session_token
    ):
        mock_conn_extra = {
            "session_kwargs": {
                "aws_access_key_id": aws_access_key_id,
                "aws_secret_access_key": aws_secret_access_key,
                "aws_session_token": aws_session_token,
            },
        }
        mock_conn = mock_connection_factory(login=None, password=None, extra=mock_conn_extra)

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.aws_access_key_id == aws_access_key_id
        assert wrap_conn.aws_secret_access_key == aws_secret_access_key
        assert wrap_conn.aws_session_token == aws_session_token

    # This function never tested and mark as deprecated. Only test expected output
    @mock.patch("airflow.providers.amazon.aws.utils.connection_wrapper._parse_s3_config")
    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("aws_secret_access_key", ["mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_access_key_id", ["mock-aws-access-key-id"])
    def test_get_credentials_from_s3_config(
        self, mock_parse_s3_config, aws_access_key_id, aws_secret_access_key, aws_session_token
    ):
        mock_parse_s3_config.return_value = (aws_access_key_id, aws_secret_access_key)
        mock_conn_extra = {
            "s3_config_format": "aws",
            "profile": "test",
            "s3_config_file": "aws-credentials",
        }
        if aws_session_token:
            mock_conn_extra["aws_session_token"] = aws_session_token
        mock_conn = mock_connection_factory(login=None, password=None, extra=mock_conn_extra)

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        mock_parse_s3_config.assert_called_once_with('aws-credentials', 'aws', 'test')
        assert wrap_conn.aws_access_key_id == aws_access_key_id
        assert wrap_conn.aws_secret_access_key == aws_secret_access_key
        assert wrap_conn.aws_session_token == aws_session_token

    @pytest.mark.parametrize("aws_access_key_id", [None, "mock-aws-access-key-id"])
    @pytest.mark.parametrize("aws_secret_access_key", [None, "mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("profile_name", [None, "mock-profile"])
    @pytest.mark.parametrize("region_name", [None, "mock-region-name"])
    def test_get_session_kwargs_from_wrapper(
        self, aws_access_key_id, aws_secret_access_key, aws_session_token, profile_name, region_name
    ):
        mock_conn_extra = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
            "profile_name": profile_name,
            "region_name": region_name,
        }
        mock_conn = mock_connection_factory(extra=mock_conn_extra)
        expected = {}
        if aws_access_key_id:
            expected["aws_access_key_id"] = aws_access_key_id
        if aws_secret_access_key:
            expected["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            expected["aws_session_token"] = aws_session_token
        if profile_name:
            expected["profile_name"] = profile_name
        if region_name:
            expected["region_name"] = region_name
        with pytest.warns(None):
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        session_kwargs = wrap_conn.session_kwargs
        assert session_kwargs == expected

        # Test that session parameters immutable
        session_kwargs["botocore_session"] = "foo.bar"
        assert wrap_conn.session_kwargs == expected
        assert wrap_conn.session_kwargs != session_kwargs

    @pytest.mark.parametrize("aws_access_key_id", [None, "mock-aws-access-key-id"])
    @pytest.mark.parametrize("aws_secret_access_key", [None, "mock-aws-secret-access-key"])
    @pytest.mark.parametrize("aws_session_token", [None, "mock-aws-session-token"])
    @pytest.mark.parametrize("profile_name", [None, "mock-profile"])
    @pytest.mark.parametrize("region_name", [None, "mock-region-name"])
    def test_get_session_kwargs_deprecation(
        self, aws_access_key_id, aws_secret_access_key, aws_session_token, profile_name, region_name
    ):
        mock_conn_extra_session_kwargs = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
            "aws_session_token": aws_session_token,
            "profile_name": profile_name,
            "region_name": region_name,
        }
        mock_conn = mock_connection_factory(extra={"session_kwargs": mock_conn_extra_session_kwargs})
        expected = {}
        if aws_access_key_id and aws_secret_access_key:
            expected["aws_access_key_id"] = aws_access_key_id
            expected["aws_secret_access_key"] = aws_secret_access_key
        if aws_session_token:
            expected["aws_session_token"] = aws_session_token
        if profile_name:
            expected["profile_name"] = profile_name
        if region_name:
            expected["region_name"] = region_name

        warning_message = (
            r"'session_kwargs' in extra config is deprecated and will be removed in a future releases. "
            r"Please specify arguments passed to boto3 Session directly in .* extra."
        )
        with pytest.warns(DeprecationWarning, match=warning_message):
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        session_kwargs = wrap_conn.session_kwargs
        assert session_kwargs == expected

    @pytest.mark.parametrize(
        "region_name,conn_region_name",
        [
            ("mock-region-name", None),
            ("mock-region-name", "mock-connection-region-name"),
            (None, "mock-connection-region-name"),
            (None, None),
        ],
    )
    def test_get_region_name(self, region_name, conn_region_name):
        mock_conn = mock_connection_factory(
            extra={"region_name": conn_region_name} if conn_region_name else None
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn, region_name=region_name)
        if region_name:
            assert wrap_conn.region_name == region_name, "Expected provided region_name"
        else:
            assert wrap_conn.region_name == conn_region_name, "Expected connection region_name"

    def test_warn_wrong_profile_param_used(self):
        mock_conn = mock_connection_factory(extra={"profile": "mock-profile"})
        warning_message = "Found 'profile' without specifying 's3_config_file' in .* set 'profile_name' in"
        with pytest.warns(UserWarning, match=warning_message):
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert "profile_name" not in wrap_conn.session_kwargs

    @mock.patch("airflow.providers.amazon.aws.utils.connection_wrapper.Config")
    @pytest.mark.parametrize(
        "botocore_config, botocore_config_kwargs",
        [
            (Config(s3={"us_east_1_regional_endpoint": "regional"}), None),
            (Config(region_name="ap-southeast-1"), {"user_agent": "Airflow Amazon Provider"}),
            (None, {"user_agent": "Airflow Amazon Provider"}),
            (None, None),
        ],
    )
    def test_get_botocore_config(self, mock_botocore_config, botocore_config, botocore_config_kwargs):
        mock_conn = mock_connection_factory(
            extra={"config_kwargs": botocore_config_kwargs} if botocore_config_kwargs else None
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn, botocore_config=botocore_config)

        if botocore_config:
            assert wrap_conn.botocore_config == botocore_config, "Expected provided botocore_config"
            assert mock_botocore_config.assert_not_called
        elif not botocore_config_kwargs:
            assert wrap_conn.botocore_config is None, "Expected default botocore_config"
            assert mock_botocore_config.assert_not_called
        else:
            assert mock_botocore_config.assert_called_once
            assert mock.call(**botocore_config_kwargs) in mock_botocore_config.mock_calls

    @pytest.mark.parametrize(
        "extra, expected",
        [
            ({"host": "https://host.aws"}, "https://host.aws"),
            ({"endpoint_url": "https://endpoint.aws"}, "https://endpoint.aws"),
            ({"host": "https://host.aws", "endpoint_url": "https://endpoint.aws"}, "https://host.aws"),
        ],
        ids=["'host' is used", "'endpoint_url' is used", "'host' preferred over 'endpoint_url'"],
    )
    def test_get_endpoint_url_from_extra(self, extra, expected):
        mock_conn = mock_connection_factory(extra=extra)
        expected_deprecation_message = (
            "extra['host'] is deprecated and will be removed in a future release."
            " Please set extra['endpoint_url'] instead"
        )

        with pytest.warns(None) as records:
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)

        if extra.get("host"):
            assert len(records) == 1
            assert str(records[0].message) == expected_deprecation_message

        assert wrap_conn.endpoint_url == expected

    @pytest.mark.parametrize("aws_account_id, aws_iam_role", [(None, None), ("111111111111", "another-role")])
    def test_get_role_arn(self, aws_account_id, aws_iam_role):
        mock_conn = mock_connection_factory(
            extra={
                "role_arn": MOCK_ROLE_ARN,
                "aws_account_id": aws_account_id,
                "aws_iam_role": aws_iam_role,
            }
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.role_arn == MOCK_ROLE_ARN

    @pytest.mark.parametrize(
        "aws_account_id, aws_iam_role, expected",
        [
            ("222222222222", "mock-role", "arn:aws:iam::222222222222:role/mock-role"),
            ("333333333333", "role-path/mock-role", "arn:aws:iam::333333333333:role/role-path/mock-role"),
        ],
    )
    def test_constructing_role_arn(self, aws_account_id, aws_iam_role, expected):
        mock_conn = mock_connection_factory(
            extra={
                "aws_account_id": aws_account_id,
                "aws_iam_role": aws_iam_role,
            }
        )
        with pytest.warns(DeprecationWarning, match="Please set 'role_arn' in .* extra"):
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.role_arn == expected

    def test_empty_role_arn(self):
        wrap_conn = AwsConnectionWrapper(conn=mock_connection_factory())
        assert wrap_conn.role_arn is None
        assert wrap_conn.assume_role_method is None
        assert wrap_conn.assume_role_kwargs == {}

    @pytest.mark.parametrize(
        "assume_role_method", ['assume_role', 'assume_role_with_saml', 'assume_role_with_web_identity']
    )
    def test_get_assume_role_method(self, assume_role_method):
        mock_conn = mock_connection_factory(
            extra={"role_arn": MOCK_ROLE_ARN, "assume_role_method": assume_role_method}
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.assume_role_method == assume_role_method

    def test_default_assume_role_method(self):
        mock_conn = mock_connection_factory(
            extra={
                "role_arn": MOCK_ROLE_ARN,
            }
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn.assume_role_method == "assume_role"

    def test_unsupported_assume_role_method(self):
        mock_conn = mock_connection_factory(
            extra={"role_arn": MOCK_ROLE_ARN, "assume_role_method": "dummy_method"}
        )
        with pytest.raises(NotImplementedError, match="Found assume_role_method='dummy_method' in .* extra"):
            AwsConnectionWrapper(conn=mock_conn)

    @pytest.mark.parametrize("assume_role_kwargs", [None, {"DurationSeconds": 42}])
    def test_get_assume_role_kwargs(self, assume_role_kwargs):
        mock_conn_extra = {"role_arn": MOCK_ROLE_ARN}
        if assume_role_kwargs:
            mock_conn_extra["assume_role_kwargs"] = assume_role_kwargs
        mock_conn = mock_connection_factory(extra=mock_conn_extra)

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        expected = assume_role_kwargs or {}
        assert wrap_conn.assume_role_kwargs == expected

    @pytest.mark.parametrize("external_id_in_extra", [None, "mock-external-id-in-extra"])
    def test_get_assume_role_kwargs_external_id_in_kwargs(self, external_id_in_extra):
        mock_external_id_in_kwargs = "mock-external-id-in-kwargs"
        mock_conn_extra = {
            "role_arn": MOCK_ROLE_ARN,
            "assume_role_kwargs": {"ExternalId": mock_external_id_in_kwargs},
        }
        if external_id_in_extra:
            mock_conn_extra["external_id"] = external_id_in_extra
        mock_conn = mock_connection_factory(extra=mock_conn_extra)

        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert "ExternalId" in wrap_conn.assume_role_kwargs
        assert wrap_conn.assume_role_kwargs["ExternalId"] == mock_external_id_in_kwargs
        assert wrap_conn.assume_role_kwargs["ExternalId"] != external_id_in_extra

    def test_get_assume_role_kwargs_external_id_in_extra(self):
        mock_external_id_in_extra = "mock-external-id-in-extra"
        mock_conn_extra = {"role_arn": MOCK_ROLE_ARN, "external_id": mock_external_id_in_extra}
        mock_conn = mock_connection_factory(extra=mock_conn_extra)

        warning_message = "Please set 'ExternalId' in 'assume_role_kwargs' in .* extra."
        with pytest.warns(DeprecationWarning, match=warning_message):
            wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert "ExternalId" in wrap_conn.assume_role_kwargs
        assert wrap_conn.assume_role_kwargs["ExternalId"] == mock_external_id_in_extra

    @pytest.mark.parametrize(
        "orig_wrapper",
        [
            AwsConnectionWrapper(
                conn=mock_connection_factory(
                    login="mock-login",
                    password="mock-password",
                    extra={
                        "region_name": "mock-region",
                        "botocore_kwargs": {"user_agent": "Airflow Amazon Provider"},
                        "role_arn": MOCK_ROLE_ARN,
                        "aws_session_token": "mock-aws-session-token",
                    },
                ),
            ),
            AwsConnectionWrapper(conn=mock_connection_factory()),
            AwsConnectionWrapper(conn=None),
            AwsConnectionWrapper(
                conn=None,
                region_name="mock-region",
                botocore_config=Config(user_agent="Airflow Amazon Provider"),
            ),
        ],
    )
    @pytest.mark.parametrize("region_name", [None, "ca-central-1"])
    @pytest.mark.parametrize("botocore_config", [None, Config(region_name="ap-southeast-1")])
    def test_wrap_wrapper(self, orig_wrapper, region_name, botocore_config):
        wrap_kwargs = {}
        if region_name:
            wrap_kwargs["region_name"] = region_name
        if botocore_config:
            wrap_kwargs["botocore_config"] = botocore_config
        wrap_conn = AwsConnectionWrapper(conn=orig_wrapper, **wrap_kwargs)

        # Non init fields should be same in orig_wrapper and child wrapper
        wrap_non_init_fields = [f.name for f in fields(wrap_conn) if not f.init]
        for field in wrap_non_init_fields:
            assert getattr(wrap_conn, field) == getattr(
                orig_wrapper, field
            ), "Expected no changes in non-init values"

        # Test overwrite/inherit init fields
        assert wrap_conn.region_name == (region_name or orig_wrapper.region_name)
        assert wrap_conn.botocore_config == (botocore_config or orig_wrapper.botocore_config)

    def test_connection_host_raises_deprecation(self):
        mock_conn = mock_connection_factory(host="https://aws.com")
        expected_deprecation_message = (
            f"Host {mock_conn.host} specified in the connection is not used."
            " Please, set it on extra['endpoint_url'] instead"
        )
        with pytest.warns(DeprecationWarning) as record:
            AwsConnectionWrapper(conn=mock_conn)

            assert str(record[0].message) == expected_deprecation_message

    @pytest.mark.parametrize("conn_id", [None, "mock-conn-id"])
    @pytest.mark.parametrize("profile_name", [None, "mock-profile"])
    @pytest.mark.parametrize("role_arn", [None, MOCK_ROLE_ARN])
    def test_get_wrapper_from_metadata(self, conn_id, profile_name, role_arn):
        mock_conn = mock_connection_factory(
            conn_id=conn_id,
            extra={
                "role_arn": role_arn,
                "profile_name": profile_name,
            },
        )
        wrap_conn = AwsConnectionWrapper(conn=mock_conn)
        assert wrap_conn
        assert wrap_conn.conn_id == conn_id
        assert wrap_conn.role_arn == role_arn
        assert wrap_conn.profile_name == profile_name
