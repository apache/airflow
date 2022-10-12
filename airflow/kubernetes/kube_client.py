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
"""Client for kubernetes communication"""
from __future__ import annotations

import logging

from airflow.configuration import conf

log = logging.getLogger(__name__)

try:
    from kubernetes import client, config
    from kubernetes.client import Configuration
    from kubernetes.client.rest import ApiException

    has_kubernetes = True

    def _disable_verify_ssl() -> None:
        if hasattr(Configuration, 'get_default_copy'):
            configuration = Configuration.get_default_copy()
        else:
            configuration = Configuration()
        configuration.verify_ssl = False
        Configuration.set_default(configuration)

except ImportError as e:
    # We need an exception class to be able to use it in ``except`` elsewhere
    # in the code base
    ApiException = BaseException
    has_kubernetes = False
    _import_err = e


def _enable_tcp_keepalive() -> None:
    """
    This function enables TCP keepalive mechanism. This prevents urllib3 connection
    to hang indefinitely when idle connection is time-outed on services like cloud
    load balancers or firewalls.

    See https://github.com/apache/airflow/pull/11406 for detailed explanation.
    Please ping @michalmisiewicz or @dimberman in the PR if you want to modify this function.
    """
    import socket

    from urllib3.connection import HTTPConnection, HTTPSConnection

    tcp_keep_idle = conf.getint('kubernetes', 'tcp_keep_idle')
    tcp_keep_intvl = conf.getint('kubernetes', 'tcp_keep_intvl')
    tcp_keep_cnt = conf.getint('kubernetes', 'tcp_keep_cnt')

    socket_options = [(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)]

    if hasattr(socket, "TCP_KEEPIDLE"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, tcp_keep_idle))
    else:
        log.debug("Unable to set TCP_KEEPIDLE on this platform")

    if hasattr(socket, "TCP_KEEPINTVL"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, tcp_keep_intvl))
    else:
        log.debug("Unable to set TCP_KEEPINTVL on this platform")

    if hasattr(socket, "TCP_KEEPCNT"):
        socket_options.append((socket.IPPROTO_TCP, socket.TCP_KEEPCNT, tcp_keep_cnt))
    else:
        log.debug("Unable to set TCP_KEEPCNT on this platform")

    HTTPSConnection.default_socket_options = HTTPSConnection.default_socket_options + socket_options
    HTTPConnection.default_socket_options = HTTPConnection.default_socket_options + socket_options


def get_kube_client(
    in_cluster: bool = conf.getboolean('kubernetes', 'in_cluster'),
    cluster_context: str | None = None,
    config_file: str | None = None,
) -> client.CoreV1Api:
    """
    Retrieves Kubernetes client

    :param in_cluster: whether we are in cluster
    :param cluster_context: context of the cluster
    :param config_file: configuration file
    :return kubernetes client
    :rtype client.CoreV1Api
    """
    if not has_kubernetes:
        raise _import_err

    if conf.getboolean('kubernetes', 'enable_tcp_keepalive'):
        _enable_tcp_keepalive()

    if in_cluster:
        config.load_incluster_config()
    else:
        if cluster_context is None:
            cluster_context = conf.get('kubernetes', 'cluster_context', fallback=None)
        if config_file is None:
            config_file = conf.get('kubernetes', 'config_file', fallback=None)
        config.load_kube_config(config_file=config_file, context=cluster_context)

    if not conf.getboolean('kubernetes', 'verify_ssl'):
        _disable_verify_ssl()

    return client.CoreV1Api()
