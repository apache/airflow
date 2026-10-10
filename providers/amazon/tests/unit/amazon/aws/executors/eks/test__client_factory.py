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

import inspect
import os
from base64 import b64encode
from pathlib import Path
from unittest import mock

import pytest
from kubernetes import client
from kubernetes_asyncio import client as async_client

from airflow.providers.amazon.aws.executors.eks import _client_factory
from airflow.providers.amazon.aws.executors.eks._client_factory import (
    _get_eks_async_kube_client,
    _get_eks_kube_client,
)
from airflow.providers.amazon.aws.utils.eks_get_token import TOKEN_EXPIRATION_MINUTES

from tests_common.test_utils.config import conf_vars

CLUSTER_NAME = "test-eks-cluster"
CLUSTER_ENDPOINT = "https://ABCDEF0123456789.gr7.us-east-1.eks.amazonaws.com"
CA_PEM = b"-----BEGIN CERTIFICATE-----\nfake-ca\n-----END CERTIFICATE-----\n"
TOKEN_REFRESH_SECONDS = TOKEN_EXPIRATION_MINUTES * 60


@pytest.fixture
def mock_aws():
    with (
        conf_vars({("aws_eks_executor", "cluster_name"): CLUSTER_NAME}),
        mock.patch.object(_client_factory, "EksHook") as eks_hook_cls,
        mock.patch.object(_client_factory, "StsHook") as sts_hook_cls,
        mock.patch.object(_client_factory, "fetch_access_token_for_cluster", autospec=True) as fetch_token,
    ):
        eks_hook = eks_hook_cls.return_value
        eks_hook.conn.describe_cluster.return_value = {
            "cluster": {
                "status": "ACTIVE",
                "endpoint": CLUSTER_ENDPOINT,
                "certificateAuthority": {"data": b64encode(CA_PEM).decode()},
            }
        }
        eks_hook.get_session.return_value.region_name = "us-east-1"
        sts_hook_cls.return_value.conn_client_meta.endpoint_url = "https://sts.us-east-1.amazonaws.com"
        fetch_token.return_value = "k8s-aws-v1.token-1"
        yield {"eks_hook_cls": eks_hook_cls, "eks_hook": eks_hook, "fetch_token": fetch_token}


class TestGetEksKubeClient:
    @pytest.mark.parametrize("status", ["ACTIVE", "UPDATING"])
    @conf_vars({("aws_eks_executor", "region_name"): "eu-west-1", ("aws_eks_executor", "conn_id"): "aws_eks"})
    def test_builds_client_from_described_cluster(self, mock_aws, status):
        mock_aws["eks_hook"].conn.describe_cluster.return_value["cluster"]["status"] = status

        core_v1 = _get_eks_kube_client()

        assert isinstance(core_v1, client.CoreV1Api)
        configuration = core_v1.api_client.configuration
        assert configuration.host == CLUSTER_ENDPOINT
        assert Path(configuration.ssl_ca_cert).read_bytes() == CA_PEM
        assert configuration.auth_settings()["BearerToken"]["value"] == "Bearer k8s-aws-v1.token-1"
        mock_aws["eks_hook_cls"].assert_called_once_with(aws_conn_id="aws_eks", region_name="eu-west-1")
        mock_aws["eks_hook"].conn.describe_cluster.assert_called_once_with(name=CLUSTER_NAME)
        os.unlink(configuration.ssl_ca_cert)

    @pytest.mark.parametrize(
        ("kubernetes_version", "token_key"),
        [("35.0.0", "authorization"), ("36.0.1", "BearerToken")],
    )
    def test_token_key_follows_kubernetes_version(self, kubernetes_version, token_key):
        assert _client_factory._get_sync_token_key(kubernetes_version) == token_key

    @pytest.mark.parametrize(
        ("elapsed", "expected_token", "fetch_count"),
        [
            pytest.param(TOKEN_REFRESH_SECONDS - 1, "k8s-aws-v1.token-1", 1, id="within-window"),
            pytest.param(TOKEN_REFRESH_SECONDS, "k8s-aws-v1.token-2", 2, id="after-deadline"),
        ],
    )
    @mock.patch.object(_client_factory, "time", autospec=True)
    def test_refresh_api_key_hook_re_mints_token_before_it_expires(
        self, mock_time, mock_aws, elapsed, expected_token, fetch_count
    ):
        mock_time.monotonic.return_value = 1000.0
        configuration = _get_eks_kube_client().api_client.configuration

        mock_aws["fetch_token"].return_value = "k8s-aws-v1.token-2"
        mock_time.monotonic.return_value = 1000.0 + elapsed
        assert configuration.auth_settings()["BearerToken"]["value"] == f"Bearer {expected_token}"

        assert mock_aws["fetch_token"].call_count == fetch_count
        args = mock_aws["fetch_token"].call_args
        assert args.args[0] == CLUSTER_NAME
        assert args.args[1].startswith("https://sts.us-east-1.amazonaws.com/?Action=GetCallerIdentity")
        assert args.kwargs["session"] is mock_aws["eks_hook"].get_session.return_value
        os.unlink(configuration.ssl_ca_cert)

    @pytest.mark.parametrize("status", ["CREATING", "DELETING", "FAILED"])
    def test_unusable_cluster_status_raises(self, mock_aws, status):
        mock_aws["eks_hook"].conn.describe_cluster.return_value["cluster"]["status"] = status

        with pytest.raises(
            ValueError,
            match=f"{CLUSTER_NAME} is {status}; .* ACTIVE or UPDATING. Wait for it to become ACTIVE",
        ):
            _get_eks_kube_client()

    @conf_vars({("aws_eks_executor", "cluster_name"): None})
    def test_missing_cluster_name_raises(self):
        with pytest.raises(
            ValueError,
            match=r"\[aws_eks_executor\] cluster_name is required.*AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME",
        ):
            _get_eks_kube_client()


@pytest.mark.asyncio
# kubernetes_asyncio loads the CA eagerly and the fake PEM is not loadable.
@mock.patch.object(_client_factory, "_write_cluster_ca_file", return_value=None)
@mock.patch.object(_client_factory, "time", autospec=True)
async def test_async_client_refreshes_bearer_token(mock_time, _, mock_aws):
    mock_time.monotonic.return_value = 1000.0
    core_v1 = _get_eks_async_kube_client()

    assert isinstance(core_v1, async_client.CoreV1Api)
    configuration = core_v1.api_client.configuration
    assert configuration.host == CLUSTER_ENDPOINT
    mock_aws["fetch_token"].return_value = "k8s-aws-v1.token-2"
    mock_time.monotonic.return_value = 1000.0 + TOKEN_REFRESH_SECONDS
    auth = configuration.auth_settings()
    # auth_settings() became a coroutine in kubernetes_asyncio 36.
    if inspect.isawaitable(auth):
        auth = await auth
    assert auth["BearerToken"]["value"] == "Bearer k8s-aws-v1.token-2"
    await core_v1.api_client.close()
