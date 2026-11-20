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
"""Client for kubernetes communication."""

from __future__ import annotations

import inspect
import logging
import socket
import warnings
from collections.abc import Callable
from typing import Any

import urllib3.util
from urllib3.connection import HTTPConnection

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.compat.sdk import conf

log = logging.getLogger(__name__)

try:
    from kubernetes import client, config
    from kubernetes.client import Configuration
    from kubernetes.client.rest import ApiException
    from kubernetes_asyncio import client as async_client, config as async_config

    from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
        API_TIMEOUT,
        API_TIMEOUT_OFFSET_SERVER_SIDE,
    )

    has_kubernetes = True

    def _get_default_configuration() -> Configuration:
        if hasattr(Configuration, "get_default_copy"):
            return Configuration.get_default_copy()
        return Configuration()

    def _disable_verify_ssl() -> None:
        configuration = _get_default_configuration()
        configuration.verify_ssl = False
        Configuration.set_default(configuration)

    def _get_request_timeout(timeout_seconds: int | None) -> float:
        """Get the client-side request timeout."""
        if timeout_seconds is not None and timeout_seconds > API_TIMEOUT - API_TIMEOUT_OFFSET_SERVER_SIDE:
            return timeout_seconds + API_TIMEOUT_OFFSET_SERVER_SIDE
        return API_TIMEOUT

    class _TimeoutK8sApiClient(client.ApiClient):
        """
        Wrapper around kubernetes sync ApiClient to set default timeout.

        When *disable_verify_ssl* is True the TLS certificate check is turned off, and when
        *enable_tcp_keepalive* is True TCP keepalive is enabled, on the *configuration*
        that is passed (or on a fresh default copy) so that callers do not need to repeat
        this logic at every call-site.
        """

        def __init__(
            self,
            configuration: client.Configuration | None = None,
            *,
            disable_verify_ssl: bool = False,
            enable_tcp_keepalive: bool = False,
        ) -> None:
            if disable_verify_ssl or enable_tcp_keepalive:
                if configuration is None:
                    configuration = client.Configuration.get_default_copy()
                if disable_verify_ssl:
                    configuration.verify_ssl = False
                if enable_tcp_keepalive:
                    configure_tcp_keepalive(configuration)
            super().__init__(configuration=configuration)

        def call_api(self, *args: Any, **kwargs: Any) -> Any:
            timeout_seconds = kwargs.get("timeout_seconds")  # get server-side timeout
            # Use setdefault's intent but handle explicit None: generated kubernetes client methods always
            # pass _request_timeout=None explicitly, making setdefault a no-op and leaving an infinite
            # timeout. On a half-open TCP connection (e.g. after a 429 or LB-side 500), this causes the
            # call to hang indefinitely with no exception, preventing any retry or recovery.
            if kwargs.get("_request_timeout") is None:
                kwargs["_request_timeout"] = _get_request_timeout(timeout_seconds)
            return super().call_api(*args, **kwargs)

    class _TimeoutAsyncK8sApiClient(async_client.ApiClient):
        """Wrapper around kubernetes async ApiClient to set default timeout."""

        def __init__(
            self,
            configuration: async_client.Configuration | None = None,
        ) -> None:
            super().__init__(configuration=configuration)

        async def call_api(self, *args: Any, **kwargs: Any) -> Any:
            timeout_seconds = kwargs.get("timeout_seconds")  # server-side timeout
            # Use setdefault's intent but handle explicit None: generated kubernetes_asyncio client methods
            # always pass _request_timeout=None explicitly, making setdefault a no-op and leaving an
            # infinite timeout. On a half-open TCP connection (e.g. after a 429 or LB-side 500), this
            # causes the trigger's await to hang indefinitely with no exception, preventing any retry or
            # recovery.
            if kwargs.get("_request_timeout") is None:
                kwargs["_request_timeout"] = _get_request_timeout(timeout_seconds)
            return await super().call_api(*args, **kwargs)

except ImportError as e:
    # We need an exception class to be able to use it in ``except`` elsewhere
    # in the code base
    ApiException = BaseException
    has_kubernetes = False
    _import_err = e


def configure_tcp_keepalive(configuration: Configuration) -> None:
    """
    Enable TCP keepalive on the Kubernetes client ``configuration``.

    This prevents connections from hanging indefinitely when an idle connection is timed out
    by services like cloud load balancers or firewalls. The options are set through
    ``Configuration.socket_options``, which the client passes on to its urllib3 pool manager.

    See https://github.com/apache/airflow/pull/11406 for the original discussion.
    """
    tcp_keep_idle = conf.getint("kubernetes_executor", "tcp_keep_idle")
    tcp_keep_intvl = conf.getint("kubernetes_executor", "tcp_keep_intvl")
    tcp_keep_cnt = conf.getint("kubernetes_executor", "tcp_keep_cnt")

    keepalive_options: list[tuple[int, int, int | bytes]] = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]

    if hasattr(socket, "TCP_KEEPIDLE"):
        keepalive_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcp_keep_idle))
    else:
        log.debug("Unable to set TCP_KEEPIDLE on this platform")

    if hasattr(socket, "TCP_KEEPINTVL"):
        keepalive_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcp_keep_intvl))
    else:
        log.debug("Unable to set TCP_KEEPINTVL on this platform")

    if hasattr(socket, "TCP_KEEPCNT"):
        keepalive_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, tcp_keep_cnt))
    else:
        log.debug("Unable to set TCP_KEEPCNT on this platform")

    base_options = configuration.socket_options
    if base_options is None:
        base_options = HTTPConnection.default_socket_options
    overridden = {(level, optname) for level, optname, _ in keepalive_options}
    configuration.socket_options = [
        option for option in base_options if (option[0], option[1]) not in overridden
    ] + keepalive_options


def _enable_tcp_keepalive(configuration: Configuration | None = None) -> None:
    """
    Enable TCP keepalive mechanism.

    .. deprecated::
        Use :func:`configure_tcp_keepalive` instead.
    """
    warnings.warn(
        "_enable_tcp_keepalive is deprecated, use configure_tcp_keepalive(configuration) instead.",
        AirflowProviderDeprecationWarning,
        stacklevel=2,
    )
    set_default = configuration is None
    if configuration is None:
        configuration = _get_default_configuration()
    configure_tcp_keepalive(configuration)
    if set_default:
        Configuration.set_default(configuration)


def get_kube_client(
    in_cluster: bool | None = None,
    cluster_context: str | None = None,
    config_file: str | None = None,
    team_name: str | None = None,
) -> client.CoreV1Api:
    """
    Retrieve Kubernetes client.

    :param in_cluster: whether we are in cluster
    :param cluster_context: context of the cluster
    :param config_file: configuration file
    :param team_name: team whose ``[kubernetes_executor]`` connection settings to use. Like all team
        config, they don't fall back to the un-prefixed section.
    :return: kubernetes client
    """
    team_kwargs = _team_kwargs(team_name)
    if in_cluster is None:
        in_cluster = conf.getboolean("kubernetes_executor", "in_cluster", **team_kwargs)
    if not has_kubernetes:
        raise _import_err

    configuration = _get_default_configuration()

    # Keepalive stays a global setting, so it is not read from the team's config.
    if conf.getboolean("kubernetes_executor", "enable_tcp_keepalive"):
        configure_tcp_keepalive(configuration)
    api_client_retry_configuration = conf.getjson(
        "kubernetes_executor", "api_client_retry_configuration", fallback={}, **team_kwargs
    )

    verify_ssl = conf.getboolean("kubernetes_executor", "verify_ssl", **team_kwargs)
    if team_name:
        # The default configuration is shared by the whole process, so a team's client sets its
        # own value rather than changing the default or inheriting another executor's change.
        configuration.verify_ssl = verify_ssl
    elif not verify_ssl:
        _disable_verify_ssl()

    if isinstance(api_client_retry_configuration, dict):
        configuration.retries = urllib3.util.Retry(**api_client_retry_configuration)
    else:
        raise ValueError("api_client_retry_configuration should be a dictionary")

    if in_cluster:
        config.load_incluster_config(client_configuration=configuration)
    else:
        if cluster_context is None:
            cluster_context = conf.get("kubernetes_executor", "cluster_context", fallback=None, **team_kwargs)
        if config_file is None:
            config_file = conf.get("kubernetes_executor", "config_file", fallback=None, **team_kwargs)
        config.load_kube_config(
            config_file=config_file, context=cluster_context, client_configuration=configuration
        )

    if not verify_ssl:
        configuration.verify_ssl = False

    ssl_ca_cert = conf.get("kubernetes_executor", "ssl_ca_cert", **team_kwargs)
    if ssl_ca_cert:
        configuration.ssl_ca_cert = ssl_ca_cert

    api_client = client.ApiClient(configuration=configuration)
    return client.CoreV1Api(api_client)


async def get_async_kube_client(
    in_cluster: bool | None = None,
    cluster_context: str | None = None,
    config_file: str | None = None,
    team_name: str | None = None,
) -> async_client.CoreV1Api:
    """
    Retrieve an asynchronous Kubernetes client.

    Mirrors :func:`get_kube_client` but builds a ``kubernetes_asyncio`` client so that
    pod-creation API calls can be issued concurrently by the KubernetesExecutor. Reads the
    same configuration keys (``in_cluster``, ``cluster_context``, ``config_file``,
    ``verify_ssl``, ``ssl_ca_cert``).

    :param in_cluster: whether we are in cluster
    :param cluster_context: context of the cluster
    :param config_file: configuration file
    :param team_name: team whose connection settings to use, as in :func:`get_kube_client`
    :return: asynchronous kubernetes client
    """
    if not has_kubernetes:
        raise _import_err
    team_kwargs = _team_kwargs(team_name)
    if in_cluster is None:
        in_cluster = conf.getboolean("kubernetes_executor", "in_cluster", **team_kwargs)

    configuration = async_client.Configuration()
    if not conf.getboolean("kubernetes_executor", "verify_ssl", **team_kwargs):
        configuration.verify_ssl = False

    if in_cluster:
        async_config.load_incluster_config(client_configuration=configuration)
    else:
        if cluster_context is None:
            cluster_context = conf.get("kubernetes_executor", "cluster_context", fallback=None, **team_kwargs)
        if config_file is None:
            config_file = conf.get("kubernetes_executor", "config_file", fallback=None, **team_kwargs)
        await async_config.load_kube_config(
            config_file=config_file, context=cluster_context, client_configuration=configuration
        )

    ssl_ca_cert = conf.get("kubernetes_executor", "ssl_ca_cert", **team_kwargs)
    if ssl_ca_cert:
        configuration.ssl_ca_cert = ssl_ca_cert

    return async_client.CoreV1Api(_TimeoutAsyncK8sApiClient(configuration))


def _team_kwargs(team_name: str | None) -> dict[str, str]:
    """Only pass ``team_name`` when set, since older Airflow releases have no team-aware lookup."""
    return {"team_name": team_name} if team_name else {}


def _call_client_factory(factory: Callable[..., Any], team_name: str | None) -> Any:
    """Pass ``team_name`` only to a factory that takes it, since factories used to take no arguments."""
    parameters = inspect.signature(factory).parameters.values()
    if any(p.name == "team_name" or p.kind is p.VAR_KEYWORD for p in parameters):
        return factory(team_name=team_name)
    return factory()


def _get_executor_kube_client(
    team_name: str | None = None, executor_client_factory: str | None = None
) -> client.CoreV1Api:
    """
    Retrieve the Kubernetes client for the KubernetesExecutor.

    Uses the executor's own factory when it has one, then the ``client_factory`` setting, and
    :func:`get_kube_client` otherwise.

    :param team_name: team the executor is running for, so a team can point its executor at its
        own cluster. Like all team config, the team's settings don't fall back to the un-prefixed
        section. A factory that takes ``team_name`` is told the team.
    :param executor_client_factory: import path of the executor's own factory, see
        ``KubernetesExecutor.client_factory``
    :return: kubernetes client
    """
    # Factories are import paths, since the pod watcher runs in its own process and resolves its
    # factory there itself.
    if executor_client_factory:
        return _call_client_factory(import_string(executor_client_factory), team_name)
    if client_factory := conf.getimport(
        "kubernetes_executor", "client_factory", fallback=None, **_team_kwargs(team_name)
    ):
        return _call_client_factory(client_factory, team_name)
    return get_kube_client(team_name=team_name)


async def _get_executor_async_kube_client(
    team_name: str | None = None, executor_client_factory: str | None = None
) -> async_client.CoreV1Api:
    """
    Retrieve the asynchronous Kubernetes client for the KubernetesExecutor.

    Uses the executor's own factory when it has one, then the ``async_client_factory`` setting,
    and :func:`get_async_kube_client` otherwise.

    :param team_name: team the executor is running for, used the same way as in
        :func:`_get_executor_kube_client`
    :param executor_client_factory: import path of the executor's own factory, see
        ``KubernetesExecutor.async_client_factory``
    :return: asynchronous kubernetes client
    """
    if executor_client_factory:
        return _call_client_factory(import_string(executor_client_factory), team_name)
    if async_client_factory := conf.getimport(
        "kubernetes_executor", "async_client_factory", fallback=None, **_team_kwargs(team_name)
    ):
        return _call_client_factory(async_client_factory, team_name)
    return await get_async_kube_client(team_name=team_name)
