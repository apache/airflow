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
"""Kubernetes client factories for the AwsEksExecutor."""

# Internal to AwsEksExecutor, not a public API. The executor writes the import paths of
# _get_eks_kube_client and _get_eks_async_kube_client into [kubernetes_executor] client_factory
# and async_client_factory, and cncf.kubernetes re-resolves them by that path in every process
# that builds a client, including the spawned pod watcher. Keep eks_executor's paths in step.

from __future__ import annotations

import os
import tempfile
from base64 import b64decode
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.utils.eks_get_token import fetch_access_token_for_cluster
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from kubernetes import client
    from kubernetes_asyncio import client as async_client

CONFIG_GROUP_NAME = "aws_eks_executor"
# UPDATING still serves the Kubernetes API, so only creating, deleting and failed clusters are refused.
_USABLE_CLUSTER_STATUSES = ("ACTIVE", "UPDATING")


def _get_eks_kube_client() -> client.CoreV1Api:
    """Build a Kubernetes client for the configured EKS cluster, in memory and without a kubeconfig."""
    from kubernetes import client

    configuration = client.Configuration()
    _configure_eks_auth(configuration)
    return client.CoreV1Api(client.ApiClient(configuration=configuration))


def _get_eks_async_kube_client() -> async_client.CoreV1Api:
    """Build the asynchronous Kubernetes client used when ``async_pod_creation`` is enabled."""
    from kubernetes_asyncio import client as async_client

    configuration = async_client.Configuration()
    _configure_eks_auth(configuration)
    return async_client.CoreV1Api(async_client.ApiClient(configuration=configuration))


def _configure_eks_auth(configuration: client.Configuration | async_client.Configuration) -> None:
    cluster_name = conf.get(CONFIG_GROUP_NAME, "cluster_name", fallback=None)
    if not cluster_name:
        raise ValueError(f"[{CONFIG_GROUP_NAME}] cluster_name is required to build an EKS client")
    region_name = conf.get(CONFIG_GROUP_NAME, "region_name", fallback=None)
    conn_id = conf.get(CONFIG_GROUP_NAME, "conn_id", fallback="aws_default")

    eks_hook = EksHook(aws_conn_id=conn_id, region_name=region_name)
    cluster = eks_hook.conn.describe_cluster(name=cluster_name)["cluster"]
    if cluster["status"] not in _USABLE_CLUSTER_STATUSES:
        raise ValueError(
            f"EKS cluster {cluster_name} is {cluster['status']}; the executor needs it to be ACTIVE"
        )
    session = eks_hook.get_session()

    # EKS only accepts tokens presigned against the regional STS endpoint; some regions
    # otherwise default to the global one. Same dance as EksHook.generate_config_file.
    os.environ["AWS_STS_REGIONAL_ENDPOINTS"] = "regional"
    try:
        sts_endpoint = StsHook(
            aws_conn_id=conn_id, region_name=session.region_name
        ).conn_client_meta.endpoint_url
    finally:
        del os.environ["AWS_STS_REGIONAL_ENDPOINTS"]
    sts_url = f"{sts_endpoint}/?Action=GetCallerIdentity&Version=2011-06-15"

    configuration.host = cluster["endpoint"]
    configuration.ssl_ca_cert = _write_cluster_ca_file(cluster["certificateAuthority"]["data"])
    # Key the bearer auth under both identifiers. kubernetes-client >= 36 looks the token up under
    # "BearerToken" and only aliases the api_key (not api_key_prefix) back to the legacy
    # "authorization" slot (see kubernetes-client/python#2595); older clients use "authorization".
    # Setting the prefix only under "authorization" makes the newer client emit the raw token with
    # no "Bearer " prefix, which the API server rejects with 401, so set both slots.
    for identifier in ("BearerToken", "authorization"):
        configuration.api_key_prefix[identifier] = "Bearer"

    # The EKS token is a presigned STS URL valid for ~15 minutes, but the scheduler holds
    # one client for its whole lifetime. refresh_api_key_hook runs on every authenticated
    # request (Configuration.get_api_key_with_prefix), and minting is a local SigV4 signing
    # with no network round-trip, so re-minting per request keeps the token fresh at
    # negligible cost. Botocore refreshes the session's own credentials when they rotate.
    def refresh_api_key(config: client.Configuration | async_client.Configuration) -> None:
        token = fetch_access_token_for_cluster(
            cluster_name, sts_url, region_name=session.region_name, session=session
        )
        config.api_key["BearerToken"] = token
        config.api_key["authorization"] = token

    configuration.refresh_api_key_hook = refresh_api_key
    refresh_api_key(configuration)


def _write_cluster_ca_file(base64_ca_data: str) -> str:
    # The kubernetes client only accepts the CA as a file path and urllib3 reads it lazily,
    # so the file must outlive this function; it is left for the OS to clean up.
    with tempfile.NamedTemporaryFile(
        mode="wb", prefix="airflow-eks-ca-", suffix=".pem", delete=False
    ) as ca_file:
        ca_file.write(b64decode(base64_ca_data))
    return ca_file.name
