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

from base64 import b64encode
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.docker.credentials.ecr import EcrDockerCredentialHelper

NOTSET = type("NotSet", (), {})()
DEFAULT_ECR_AWS_ACCOUNT = "123456789098"


def mock_authorization_token(**kwargs):
    """Mock boto3 ECR client get_authorization_token method."""
    registry_ids = kwargs.get("registryIds", NOTSET)
    if registry_ids is NOTSET:
        registry_ids = [DEFAULT_ECR_AWS_ACCOUNT]

    if not isinstance(registry_ids, list):
        raise TypeError(f"registryIds expected list of strings, got {type(registry_ids).__name__}")
    else:
        if not all(isinstance(item, str) for item in registry_ids):
            raise TypeError(
                "registryIds expected list of strings, got "
                f"List[{', '.join(type(item).__name__ for item in registry_ids)}]"
            )
    auth_data = []
    for reg_id in registry_ids:
        auth_data.append(
            {
                "authorizationToken": b64encode(f"AWS:password_{reg_id}".encode()).decode("ascii"),
                "proxyEndpoint": f"https://{reg_id}.dkr.ecr.region.amazonaws.com",
                "expiresAt": mock.MagicMock(),
            }
        )

    return {"authorizationData": auth_data}


class TestEcrDockerCredentialHelper:
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")
    @pytest.mark.parametrize("aws_conn_id", [None, NOTSET, "aws_test_conn"])
    @pytest.mark.parametrize("region_name", [None, "eu-west-3"])
    @pytest.mark.parametrize("registry_ids", [None, "123456789098", ["123456789098", "012345678910"]])
    def test_get_ecr_credentials(self, mock_aws_hook_class, aws_conn_id, region_name, registry_ids):
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_client = mock.MagicMock()
        mock_client.get_authorization_token.side_effect = mock_authorization_token
        type(mock_aws_hook_instance).conn = mock.PropertyMock(return_value=mock_client)

        test_conn = Connection(
            conn_id='docker_without_anything',
            conn_type='docker',
        )
        ch_kwargs = {"region_name": region_name, "registry_ids": registry_ids}
        if aws_conn_id is not NOTSET:
            ch_kwargs["aws_conn_id"] = aws_conn_id

        ch = EcrDockerCredentialHelper(conn=test_conn, **ch_kwargs)
        creds = ch.get_credentials()
        mock_aws_hook_class.assert_called_once_with(
            aws_conn_id=(aws_conn_id if aws_conn_id is not NOTSET else "aws_default"),
            region_name=region_name,
            client_type="ecr",
        )

        expected_ids = [registry_ids] if isinstance(registry_ids, str) else registry_ids
        if registry_ids is not None:
            mock_client.get_authorization_token.assert_called_once_with(registryIds=expected_ids)
        else:
            # If Registry ID not specified than call should be without arguments
            mock_client.get_authorization_token.assert_called_once_with()

        # Should retrieve at least one credential
        assert len(creds) == 1 if not isinstance(registry_ids, list) else len(registry_ids)
        for ix, credential in enumerate(creds):
            assert credential.username == "AWS"
            assert credential.email is None
            assert credential.reauth

            if expected_ids is None:
                assert credential.password == f"password_{DEFAULT_ECR_AWS_ACCOUNT}"
                assert credential.registry == f"{DEFAULT_ECR_AWS_ACCOUNT}.dkr.ecr.region.amazonaws.com"
            else:
                assert credential.password == f"password_{expected_ids[ix]}"
                assert credential.registry == f"{expected_ids[ix]}.dkr.ecr.region.amazonaws.com"
