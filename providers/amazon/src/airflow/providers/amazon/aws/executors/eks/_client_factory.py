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

# Internal to AwsEksExecutor, which declares these functions' import paths as its client_factory
# and async_client_factory.

from __future__ import annotations

import atexit
import logging
import os
import tempfile
import threading
import time
from base64 import b64decode
from collections.abc import Callable
from contextlib import suppress
from contextvars import ContextVar
from http import HTTPStatus

import kubernetes
from kubernetes import client
from kubernetes_asyncio import client as async_client
from packaging.version import Version

from airflow.providers.amazon.aws.executors.eks.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    AllEksConfigKeys,
    get_cluster_name_env_var,
)
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.hooks.sts import StsHook
from airflow.providers.amazon.aws.utils.eks_get_token import (
    TOKEN_EXPIRATION_MINUTES,
    fetch_access_token_for_cluster,
)
from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)

# UPDATING still serves the Kubernetes API, so every other status is refused.
_USABLE_CLUSTER_STATUSES = ("ACTIVE", "UPDATING")


def _get_sync_token_key(kubernetes_version: str) -> str:
    # kubernetes 36 moved the bearer token from the "authorization" api_key to "BearerToken".
    return "BearerToken" if Version(kubernetes_version).major >= 36 else "authorization"


_SYNC_TOKEN_KEY = _get_sync_token_key(kubernetes.__version__)


class _EksExecutorHook(EksHook):
    # Without this, the user agent reports KubernetesExecutor as the caller.
    def _get_caller(self, target_function_name: str = "execute") -> str:
        return "AwsEksExecutor"


_TOKEN_REJECTED = "The EKS cluster rejected the token (401); retrying once with a new token."

# The token the current request was sent with. Each thread and asyncio task has its own.
_sent_token: ContextVar[str | None] = ContextVar("_sent_token", default=None)


# A 401 means the cluster rejected the token before acting on the request, so the request, even a
# create, is safe to send again. The new token is signed with the credentials the connection
# resolves to now, which may have rotated since the rejected token was signed.
class _EksApiClient(client.ApiClient):
    def __init__(self, configuration: client.Configuration, expire_token: Callable[[], None]):
        super().__init__(configuration=configuration)
        self._expire_token = expire_token

    def call_api(self, *args, **kwargs):
        try:
            return super().call_api(*args, **kwargs)
        except client.ApiException as e:
            if e.status != HTTPStatus.UNAUTHORIZED:
                raise
        log.warning(_TOKEN_REJECTED)
        self._expire_token()
        return super().call_api(*args, **kwargs)


class _EksAsyncApiClient(async_client.ApiClient):
    def __init__(self, configuration: async_client.Configuration, expire_token: Callable[[], None]):
        super().__init__(configuration=configuration)
        self._expire_token = expire_token

    async def call_api(self, *args, **kwargs):
        try:
            return await super().call_api(*args, **kwargs)
        except async_client.ApiException as e:
            if e.status != HTTPStatus.UNAUTHORIZED:
                raise
        log.warning(_TOKEN_REJECTED)
        self._expire_token()
        return await super().call_api(*args, **kwargs)


def _get_eks_kube_client(team_name: str | None = None) -> client.CoreV1Api:
    """Build a Kubernetes client for the team's EKS cluster, in memory and without a kubeconfig."""
    configuration = client.Configuration()
    expire_token = _configure_eks_auth(configuration, _SYNC_TOKEN_KEY, team_name)
    return client.CoreV1Api(_EksApiClient(configuration, expire_token))


def _get_eks_async_kube_client(team_name: str | None = None) -> async_client.CoreV1Api:
    """Build the asynchronous Kubernetes client used when ``async_pod_creation`` is enabled."""
    configuration = async_client.Configuration()
    # kubernetes_asyncio has always used "BearerToken".
    expire_token = _configure_eks_auth(configuration, "BearerToken", team_name)
    return async_client.CoreV1Api(_EksAsyncApiClient(configuration, expire_token))


def _configure_eks_auth(
    configuration: client.Configuration | async_client.Configuration,
    token_key: str,
    team_name: str | None,
) -> Callable[[], None]:
    """Set up EKS token auth, and return a function that makes the next request get a new token."""
    # The factories run where there is no executor, such as the pod watcher process, so they look
    # up the team's config themselves, the same way ExecutorConf does. Airflow releases without
    # multi-team don't accept team_name.
    team_kwargs = {"team_name": team_name} if team_name else {}
    cluster_name = conf.get(CONFIG_GROUP_NAME, AllEksConfigKeys.CLUSTER_NAME, fallback=None, **team_kwargs)
    if not cluster_name:
        raise ValueError(
            f"[{CONFIG_GROUP_NAME}] cluster_name is required to build an EKS client. "
            f"Set it in airflow.cfg or with {get_cluster_name_env_var(team_name)}."
        )
    region_name = conf.get(CONFIG_GROUP_NAME, AllEksConfigKeys.REGION_NAME, fallback=None, **team_kwargs)
    conn_id = conf.get(
        CONFIG_GROUP_NAME,
        AllEksConfigKeys.AWS_CONN_ID,
        fallback=CONFIG_DEFAULTS[AllEksConfigKeys.AWS_CONN_ID],
        **team_kwargs,
    )

    eks_hook = _EksExecutorHook(aws_conn_id=conn_id, region_name=region_name)
    cluster = eks_hook.conn.describe_cluster(name=cluster_name)["cluster"]
    if cluster["status"] not in _USABLE_CLUSTER_STATUSES:
        raise ValueError(
            f"EKS cluster {cluster_name} is {cluster['status']}; the executor needs it to be ACTIVE or "
            f"UPDATING. Wait for it to become ACTIVE, or point [{CONFIG_GROUP_NAME}] cluster_name at a "
            "usable cluster."
        )
    session = eks_hook.get_session()

    # EKS needs the regional STS endpoint, which botocore uses by default since 1.40.
    sts_endpoint = StsHook(aws_conn_id=conn_id, region_name=session.region_name).conn_client_meta.endpoint_url
    sts_url = f"{sts_endpoint}/?Action=GetCallerIdentity&Version=2011-06-15"

    configuration.host = cluster["endpoint"]
    configuration.ssl_ca_cert = _write_cluster_ca_file(cluster["certificateAuthority"]["data"])
    configuration.api_key_prefix[token_key] = "Bearer"

    # EKS tokens expire after about 15 minutes, so get a new one shortly before the current one expires.
    # The first call always fetches a token because token is None. The call at the end of this function
    # makes that happen now, so the first request already has a token.
    token: str | None = None
    refresh_at = 0.0
    token_lock = threading.Lock()

    def refresh_api_key(config: client.Configuration | async_client.Configuration) -> None:
        nonlocal token, refresh_at
        with token_lock:
            if token is None or time.monotonic() >= refresh_at:
                # Sign with the credentials the connection resolves to now: only a new session picks up
                # static keys rotated outside the process, such as in a rewritten credentials file.
                token = fetch_access_token_for_cluster(
                    cluster_name, sts_url, region_name=session.region_name, session=eks_hook.get_session()
                )
                refresh_at = time.monotonic() + TOKEN_EXPIRATION_MINUTES * 60
            config.api_key[token_key] = token
            _sent_token.set(token)

    def expire_token() -> None:
        nonlocal token, eks_hook
        with token_lock:
            # Requests rejected with the same token, such as concurrent pod creations, share one new token.
            if token == _sent_token.get():
                # The hook caches the connection, so read it again in case the keys stored in it rotated.
                eks_hook = _EksExecutorHook(aws_conn_id=conn_id, region_name=region_name)
                token = None

    configuration.refresh_api_key_hook = refresh_api_key
    refresh_api_key(configuration)
    return expire_token


# The CA file written for each cluster CA, and the process that wrote it.
_ca_files: dict[str, tuple[str, int]] = {}
_ca_files_lock = threading.Lock()


def _write_cluster_ca_file(base64_ca_data: str) -> str:
    # The kubernetes client reads the CA file lazily, so it must outlive the client. A client is
    # built for every log read, so write each CA once per process rather than once per client.
    with _ca_files_lock:
        if base64_ca_data not in _ca_files:
            with tempfile.NamedTemporaryFile(
                mode="wb", prefix="airflow-eks-ca-", suffix=".pem", delete=False
            ) as ca_file:
                ca_file.write(b64decode(base64_ca_data))
            _ca_files[base64_ca_data] = (ca_file.name, os.getpid())
        return _ca_files[base64_ca_data][0]


def _remove_ca_files() -> None:
    # A forked child reuses its parent's files, so each process only removes the ones it wrote.
    for path, pid in _ca_files.values():
        if pid == os.getpid():
            with suppress(OSError):
                os.remove(path)


def _reset_ca_files_lock() -> None:
    # Another thread of the parent may have held the lock when it forked.
    global _ca_files_lock
    _ca_files_lock = threading.Lock()


atexit.register(_remove_ca_files)
os.register_at_fork(after_in_child=_reset_ca_files_lock)
