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
import signal
import tempfile
import threading
import time
from base64 import b64encode
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

import boto3
import pytest
from kubernetes import client
from kubernetes_asyncio import client as async_client

from airflow.providers.amazon.aws.executors.eks import _client_factory
from airflow.providers.amazon.aws.executors.eks._client_factory import (
    _get_eks_async_kube_client,
    _get_eks_kube_client,
)
from airflow.providers.amazon.aws.utils.eks_get_token import (
    TOKEN_EXPIRATION_MINUTES,
    fetch_access_token_for_cluster,
)

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

CLUSTER_NAME = "test-eks-cluster"
CLUSTER_ENDPOINT = "https://ABCDEF0123456789.gr7.us-east-1.eks.amazonaws.com"
CA_PEM = b"-----BEGIN CERTIFICATE-----\nfake-ca\n-----END CERTIFICATE-----\n"
TOKEN_REFRESH_SECONDS = TOKEN_EXPIRATION_MINUTES * 60


@pytest.fixture(autouse=True)
def ca_dir(tmp_path, monkeypatch):
    """Write CA files under tmp_path and start every test with no CA files written yet."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(_client_factory, "_ca_files", {})
    return tmp_path


@pytest.fixture
def mock_aws():
    with (
        conf_vars({("aws_eks_executor", "cluster_name"): CLUSTER_NAME}),
        mock.patch.object(_client_factory, "_EksExecutorHook") as eks_hook_cls,
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
        yield {
            "eks_hook_cls": eks_hook_cls,
            "eks_hook": eks_hook,
            "sts_hook_cls": sts_hook_cls,
            "fetch_token": fetch_token,
        }


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

    def test_concurrent_builds_never_change_sts_regional_endpoints_env_var(self, mock_aws, monkeypatch):
        monkeypatch.setenv("AWS_STS_REGIONAL_ENDPOINTS", "legacy")
        mock_aws["eks_hook"].get_session.side_effect = lambda: boto3.Session(
            aws_access_key_id="testing", aws_secret_access_key="testing", region_name="us-east-1"
        )
        mock_aws["fetch_token"].side_effect = fetch_access_token_for_cluster
        env_writes = []
        environ_cls = type(os.environ)
        for method in ("__setitem__", "__delitem__"):
            real = getattr(environ_cls, method)
            monkeypatch.setattr(
                environ_cls,
                method,
                lambda self, key, *args, _real=real: env_writes.append(key) or _real(self, key, *args),
            )
        seen = set()
        building = threading.Event()

        def watch():
            while building.is_set():
                seen.add(os.environ.get("AWS_STS_REGIONAL_ENDPOINTS"))
                # A bare spin loop holds the GIL against the builders and can push the test past its timeout.
                time.sleep(0.001)

        building.set()
        watcher = threading.Thread(target=watch)
        watcher.start()
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda _: _get_eks_kube_client(), range(16)))
        finally:
            building.clear()
            watcher.join()

        assert "AWS_STS_REGIONAL_ENDPOINTS" not in env_writes
        assert seen == {"legacy"}

    def test_builds_share_one_ca_file(self, mock_aws, ca_dir):
        paths = {_get_eks_kube_client().api_client.configuration.ssl_ca_cert for _ in range(5)}

        assert len(paths) == 1
        assert [str(path) for path in ca_dir.glob("airflow-eks-ca-*")] == list(paths)
        assert Path(paths.pop()).read_bytes() == CA_PEM

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

    @pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Multi-team requires Airflow 3.2+")
    def test_team_without_cluster_name_raises(self, mock_aws):
        """The global cluster_name set by mock_aws is not a fallback for the team."""
        with pytest.raises(
            ValueError,
            match=r"cluster_name is required.*AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CLUSTER_NAME",
        ):
            _get_eks_kube_client(team_name="team_a")


@pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Multi-team requires Airflow 3.2+")
@pytest.mark.parametrize("factory", [_get_eks_kube_client, _get_eks_async_kube_client], ids=["sync", "async"])
@pytest.mark.asyncio
# kubernetes_asyncio loads the CA eagerly and the fake PEM is not loadable.
@mock.patch.object(_client_factory, "_write_cluster_ca_file", return_value=None)
async def test_builds_client_for_the_teams_cluster(_, mock_aws, monkeypatch, factory):
    monkeypatch.setenv("AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CLUSTER_NAME", "team-a-cluster")
    monkeypatch.setenv("AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__REGION_NAME", "eu-west-1")
    monkeypatch.setenv("AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CONN_ID", "aws_team_a")

    core_v1 = factory(team_name="team_a")

    mock_aws["eks_hook_cls"].assert_called_once_with(aws_conn_id="aws_team_a", region_name="eu-west-1")
    mock_aws["eks_hook"].conn.describe_cluster.assert_called_once_with(name="team-a-cluster")
    assert mock_aws["fetch_token"].call_args.args[0] == "team-a-cluster"
    if inspect.iscoroutinefunction(core_v1.api_client.close):
        await core_v1.api_client.close()


@pytest.mark.parametrize("factory", [_get_eks_kube_client, _get_eks_async_kube_client], ids=["sync", "async"])
@pytest.mark.asyncio
@mock.patch.object(_client_factory, "_write_cluster_ca_file", autospec=True, return_value=None)
@mock.patch.object(_client_factory, "fetch_access_token_for_cluster", autospec=True, return_value="token")
@mock.patch.object(_client_factory, "StsHook", autospec=True)
@mock.patch("botocore.client.BaseClient._make_api_call", autospec=True)
async def test_describe_cluster_user_agent_names_the_executor(
    mock_api_call, mock_sts_hook, _fetch_token, _write_ca_file, monkeypatch, factory
):
    monkeypatch.setenv("AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME", CLUSTER_NAME)
    monkeypatch.setenv("AIRFLOW__AWS_EKS_EXECUTOR__REGION_NAME", "us-east-1")
    monkeypatch.setenv("AIRFLOW_CONN_AWS_DEFAULT", "aws://")
    mock_api_call.return_value = {
        "cluster": {
            "status": "ACTIVE",
            "endpoint": CLUSTER_ENDPOINT,
            "certificateAuthority": {"data": b64encode(CA_PEM).decode()},
        }
    }
    mock_sts_hook.return_value.conn_client_meta.endpoint_url = "https://sts.us-east-1.amazonaws.com"

    core_v1 = factory()

    eks_client, operation_name = mock_api_call.call_args.args[:2]
    assert operation_name == "DescribeCluster"
    assert "Caller/AwsEksExecutor" in eks_client.meta.config.user_agent_extra.split(" ")
    if inspect.iscoroutinefunction(core_v1.api_client.close):
        await core_v1.api_client.close()


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


class TestWriteClusterCaFile:
    CA_DATA = b64encode(CA_PEM).decode()

    def test_concurrent_writes_of_one_ca_leave_one_file(self, ca_dir):
        with ThreadPoolExecutor(max_workers=16) as pool:
            paths = set(pool.map(lambda _: _client_factory._write_cluster_ca_file(self.CA_DATA), range(64)))

        assert len(paths) == 1
        assert len(list(ca_dir.glob("airflow-eks-ca-*"))) == 1
        assert Path(paths.pop()).read_bytes() == CA_PEM

    def test_each_ca_gets_its_own_file_and_all_are_removed_at_exit(self, ca_dir):
        other_pem = CA_PEM.replace(b"fake-ca", b"other-ca")
        path = _client_factory._write_cluster_ca_file(self.CA_DATA)
        other_path = _client_factory._write_cluster_ca_file(b64encode(other_pem).decode())

        assert path != other_path
        assert Path(other_path).read_bytes() == other_pem

        _client_factory._remove_ca_files()

        assert list(ca_dir.glob("airflow-eks-ca-*")) == []

    @pytest.mark.skipif(not hasattr(os, "fork"), reason="needs os.fork")
    def test_forked_child_reuses_parents_ca_file_and_leaves_it_at_exit(self, ca_dir):
        path = _client_factory._write_cluster_ca_file(self.CA_DATA)

        # Fork while the lock is held, as another thread of the parent might hold it.
        with _client_factory._ca_files_lock:
            pid = os.fork()
            if pid == 0:
                exit_code = 1
                try:
                    # Die rather than hang on the inherited lock.
                    signal.signal(signal.SIGALRM, signal.SIG_DFL)
                    signal.alarm(10)
                    if _client_factory._write_cluster_ca_file(self.CA_DATA) == path:
                        # What the child's atexit hook would do.
                        _client_factory._remove_ca_files()
                        exit_code = 0
                finally:
                    os._exit(exit_code)
        _, status = os.waitpid(pid, 0)

        assert os.waitstatus_to_exitcode(status) == 0
        assert [str(p) for p in ca_dir.glob("airflow-eks-ca-*")] == [path]
