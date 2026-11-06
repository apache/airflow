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

import argparse
import importlib
import sys
from base64 import b64encode
from datetime import UTC, datetime
from unittest import mock

import pytest
from kubernetes.client import CoreV1Api
from kubernetes.client.rest import ApiException

from airflow.providers.amazon.aws.executors.eks import AwsEksExecutor, _client_factory
from airflow.providers.cncf.kubernetes.cli import kubernetes_command
from airflow.providers.cncf.kubernetes.executors.kubernetes_executor import KubernetesExecutor
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_2_PLUS

EKS_EXECUTOR_MODULE = "airflow.providers.amazon.aws.executors.eks.eks_executor"
CLUSTER_ENDPOINT = "https://ABCDEF0123456789.gr7.us-east-1.eks.amazonaws.com"
JOB_WATCHER = "airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils.KubernetesJobWatcher"
USER_CLIENT = "client-from-user-factory"
REQUIRES_MULTI_TEAM = pytest.mark.skipif(not AIRFLOW_V_3_2_PLUS, reason="Multi-team requires Airflow 3.2+")
TEAMS = [
    pytest.param(None, "AIRFLOW__", id="global"),
    pytest.param(
        "team_a",
        "AIRFLOW__TEAM_A___",
        id="team",
        marks=REQUIRES_MULTI_TEAM,
    ),
]


def _team_kwargs(team_name: str | None) -> dict:
    return {"team_name": team_name} if team_name else {}


def build_user_client():
    return USER_CLIENT


@pytest.fixture(autouse=True)
def eks_config():
    with conf_vars({("aws_eks_executor", "cluster_name"): "test-eks-cluster"}):
        yield


@pytest.fixture
def mock_aws():
    with (
        mock.patch.object(_client_factory, "_EksExecutorHook") as eks_hook_cls,
        mock.patch.object(_client_factory, "StsHook") as sts_hook_cls,
        mock.patch.object(_client_factory, "fetch_access_token_for_cluster", autospec=True) as fetch_token,
    ):
        eks_hook = eks_hook_cls.return_value
        eks_hook.conn.describe_cluster.return_value = {
            "cluster": {
                "status": "ACTIVE",
                "endpoint": CLUSTER_ENDPOINT,
                "certificateAuthority": {"data": b64encode(b"fake-ca").decode()},
            }
        }
        eks_hook.get_session.return_value.region_name = "us-east-1"
        sts_hook_cls.return_value.conn_client_meta.endpoint_url = "https://sts.us-east-1.amazonaws.com"
        fetch_token.return_value = "k8s-aws-v1.token"
        yield {"eks_hook_cls": eks_hook_cls, "eks_hook": eks_hook, "fetch_token": fetch_token}


def _start_and_end(executor: KubernetesExecutor) -> None:
    executor.job_id = 1
    try:
        executor.start()
    finally:
        executor.end()


class TestAwsEksExecutor:
    @pytest.mark.parametrize(("team_name", "env_prefix"), TEAMS)
    @conf_vars({("aws_eks_executor", "cluster_name"): None})
    def test_missing_cluster_name_raises(self, monkeypatch, team_name, env_prefix):
        if team_name:
            # Team config doesn't fall back to the global section.
            monkeypatch.setenv("AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME", "global-cluster")

        with pytest.raises(
            ValueError,
            match=rf"\[aws_eks_executor\] cluster_name.*{env_prefix}AWS_EKS_EXECUTOR__CLUSTER_NAME",
        ):
            AwsEksExecutor(**_team_kwargs(team_name))

    @pytest.mark.parametrize(
        ("team_name", "cluster_name", "conn_id", "region_name", "check_health"),
        [
            pytest.param(
                "team_a",
                "team-a-cluster",
                "aws_team_a",
                "eu-west-1",
                True,
                id="team",
                marks=REQUIRES_MULTI_TEAM,
            ),
            pytest.param(None, "global-cluster", "aws_global", "us-east-1", False, id="global"),
        ],
    )
    # kubernetes_asyncio loads the CA eagerly and the fake one is not loadable.
    @mock.patch.object(_client_factory, "_write_cluster_ca_file", return_value=None)
    @mock.patch.object(AwsEksExecutor, "check_health", autospec=True)
    @mock.patch(JOB_WATCHER, autospec=True)
    def test_team_config(
        self,
        mock_watcher,
        mock_check_health,
        _,
        mock_aws,
        monkeypatch,
        team_name,
        cluster_name,
        conn_id,
        region_name,
        check_health,
    ):
        """Each executor builds both its clients from its own [aws_eks_executor] values, and so does its watcher."""
        for key, value in {
            "AIRFLOW__KUBERNETES_EXECUTOR__ASYNC_POD_CREATION": "True",
            "AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__ASYNC_POD_CREATION": "True",
            "AIRFLOW__AWS_EKS_EXECUTOR__CLUSTER_NAME": "global-cluster",
            "AIRFLOW__AWS_EKS_EXECUTOR__REGION_NAME": "us-east-1",
            "AIRFLOW__AWS_EKS_EXECUTOR__CONN_ID": "aws_global",
            "AIRFLOW__AWS_EKS_EXECUTOR__CHECK_HEALTH_ON_STARTUP": "False",
            # The team leaves check_health_on_startup at its default rather than the global value.
            "AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CLUSTER_NAME": "team-a-cluster",
            "AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__REGION_NAME": "eu-west-1",
            "AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CONN_ID": "aws_team_a",
        }.items():
            monkeypatch.setenv(key, value)
        executor = AwsEksExecutor(**_team_kwargs(team_name))
        executor.job_id = 1
        executor.start()
        try:
            # Builds the async client, as the first batch of pods would.
            executor.kube_scheduler._run_pods_async([])
            async_client = executor.kube_scheduler._async_pod_client
        finally:
            executor.end()

        assert executor.kube_client.api_client.configuration.host == CLUSTER_ENDPOINT
        assert async_client.api_client.configuration.host == CLUSTER_ENDPOINT
        assert (
            mock_aws["eks_hook_cls"].call_args_list
            == [mock.call(aws_conn_id=conn_id, region_name=region_name)] * 2
        )
        assert mock_aws["eks_hook"].conn.describe_cluster.call_args_list == [mock.call(name=cluster_name)] * 2
        assert {call.args[0] for call in mock_aws["fetch_token"].call_args_list} == {cluster_name}
        assert mock_check_health.call_count == int(check_health)
        assert mock_watcher.call_args.kwargs["team_name"] == team_name
        assert mock_watcher.call_args.kwargs["client_factory"] == AwsEksExecutor.client_factory

    @pytest.mark.parametrize("user_factory", [False, True], ids=["default-client", "user-factory"])
    @pytest.mark.parametrize(("team_name", "env_prefix"), TEAMS)
    @mock.patch.object(AwsEksExecutor, "check_health", autospec=True)
    @mock.patch(JOB_WATCHER, autospec=True)
    @mock.patch("airflow.providers.cncf.kubernetes.kube_client.get_kube_client", autospec=True)
    def test_runs_alongside_kubernetes_executor(
        self,
        mock_get_kube_client,
        mock_watcher,
        mock_check_health,
        mock_aws,
        monkeypatch,
        team_name,
        env_prefix,
        user_factory,
    ):
        """A KubernetesExecutor in the same scope keeps its own client, including its own factory."""
        monkeypatch.setenv(f"{env_prefix}AWS_EKS_EXECUTOR__CLUSTER_NAME", "test-eks-cluster")
        if user_factory:
            monkeypatch.setenv(
                f"{env_prefix}KUBERNETES_EXECUTOR__CLIENT_FACTORY", f"{__name__}.build_user_client"
            )
        # The scheduler builds every executor before it starts any of them.
        eks_executor = AwsEksExecutor(**_team_kwargs(team_name))
        kubernetes_executor = KubernetesExecutor(**_team_kwargs(team_name))

        _start_and_end(eks_executor)
        _start_and_end(kubernetes_executor)

        assert eks_executor.kube_client.api_client.configuration.host == CLUSTER_ENDPOINT
        if user_factory:
            assert kubernetes_executor.kube_client == USER_CLIENT
        else:
            assert kubernetes_executor.kube_client is mock_get_kube_client.return_value
            mock_get_kube_client.assert_called_once_with(team_name=team_name)
        assert [call.kwargs["client_factory"] for call in mock_watcher.call_args_list] == [
            AwsEksExecutor.client_factory,
            None,
        ]

    @pytest.mark.parametrize("check_health", [True, False])
    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_checks_cluster_access_unless_disabled(self, mock_start, check_health):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock(spec=CoreV1Api)

        with conf_vars({("aws_eks_executor", "check_health_on_startup"): str(check_health)}):
            executor.start()

        mock_start.assert_called_once_with(executor)
        assert executor.kube_client.list_namespaced_pod.call_args_list == (
            [mock.call(executor.kube_config.kube_namespace, limit=1)] if check_health else []
        )

    @mock.patch.object(KubernetesExecutor, "start", autospec=True)
    def test_start_fails_when_cluster_rejects_the_executor(self, mock_start):
        executor = AwsEksExecutor()
        executor.kube_client = mock.MagicMock(spec=CoreV1Api)
        executor.kube_client.list_namespaced_pod.side_effect = ApiException(status=401, reason="Unauthorized")

        with pytest.raises(RuntimeError, match="401 Unauthorized"):
            executor.start()


class TestCncfKubernetesRequirement:
    def test_import_fails_without_cncf_kubernetes(self, monkeypatch):
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)
        monkeypatch.setitem(
            sys.modules, "airflow.providers.cncf.kubernetes.executors.kubernetes_executor", None
        )

        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=r"apache-airflow-providers-amazon\[cncf.kubernetes\]",
        ):
            importlib.import_module(EKS_EXECUTOR_MODULE)

    def test_import_fails_with_cncf_kubernetes_without_executor_client_factory(self, monkeypatch):
        monkeypatch.delattr(KubernetesExecutor, "client_factory")
        monkeypatch.delitem(sys.modules, EKS_EXECUTOR_MODULE)

        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=r"apache-airflow-providers-amazon\[cncf.kubernetes\]",
        ):
            importlib.import_module(EKS_EXECUTOR_MODULE)


@pytest.mark.db_test
class TestCleanupPods:
    @REQUIRES_MULTI_TEAM
    @pytest.mark.usefixtures("clean_executor_loader")
    @conf_vars(
        {
            ("core", "multi_team"): "True",
            ("core", "executor"): f"LocalExecutor;team_a={EKS_EXECUTOR_MODULE}.AwsEksExecutor",
        }
    )
    @mock.patch.object(kubernetes_command, "_delete_pod", autospec=True)
    @mock.patch.object(CoreV1Api, "list_namespaced_pod", autospec=True)
    def test_cleanup_pods_team_uses_the_teams_eks_cluster(
        self, mock_list_pods, mock_delete_pod, mock_aws, monkeypatch
    ):
        """``airflow kubernetes cleanup-pods --team`` connects through the team's AwsEksExecutor."""
        monkeypatch.setenv("AIRFLOW__TEAM_A___AWS_EKS_EXECUTOR__CLUSTER_NAME", "team-a-cluster")
        monkeypatch.setenv("AIRFLOW__TEAM_A___KUBERNETES_EXECUTOR__NAMESPACE", "team-a-namespace")
        pod = mock.MagicMock()
        pod.metadata.name = "finished-pod"
        pod.metadata.creation_timestamp = datetime(2021, 12, 20, tzinfo=UTC)
        pod.status.phase = "Succeeded"
        pod.status.reason = None
        mock_list_pods.return_value = mock.MagicMock(items=[pod], metadata=mock.MagicMock(_continue=None))

        kubernetes_command.cleanup_pods(
            argparse.Namespace(
                namespace=None,
                min_pending_minutes=30,
                min_completed_minutes=1,
                team="team_a",
                executor=None,
                allow_shared_namespace=False,
                verbose=False,
            )
        )

        mock_aws["eks_hook"].conn.describe_cluster.assert_called_once_with(name="team-a-cluster")
        kube_client = mock_list_pods.call_args.args[0]
        assert kube_client.api_client.configuration.host == CLUSTER_ENDPOINT
        assert mock_list_pods.call_args.kwargs["namespace"] == "team-a-namespace"
        mock_delete_pod.assert_called_once_with("finished-pod", "team-a-namespace", kube_client)
