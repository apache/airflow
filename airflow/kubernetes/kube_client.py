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

from typing import Optional

from airflow.configuration import conf
from six import PY2

try:
    from kubernetes import config, client
    from kubernetes.client.rest import ApiException  # pylint: disable=unused-import
    from kubernetes.client.api_client import ApiClient
    from kubernetes.client import Configuration
    from airflow.kubernetes.refresh_config import (  # pylint: disable=ungrouped-imports
        load_kube_config,
        RefreshConfiguration,
    )
    has_kubernetes = True

    def _get_kube_config(in_cluster,  # type: bool
                         cluster_context,  # type: Optional[str]
                         config_file,  # type: Optional[str]
                         ):  # type: (...) -> Optional[Configuration]
        if in_cluster:
            # load_incluster_config set default configuration with config populated by k8s
            config.load_incluster_config()
            cfg = None
        else:
            # this block can be replaced with just config.load_kube_config once
            # refresh_config module is replaced with upstream fix
            cfg = RefreshConfiguration()
            load_kube_config(
                client_configuration=cfg, config_file=config_file, context=cluster_context)

        if PY2:
            # For connect_get_namespaced_pod_exec
            configuration = Configuration()
            configuration.assert_hostname = False
            Configuration.set_default(configuration)
        return cfg

    def _get_client_with_patched_configuration(cfg):  # type (Optional[Configuration]) -> client.CoreV1Api:
        '''
        This is a workaround for supporting api token refresh in k8s client.

        The function can be replace with `return client.CoreV1Api()` once the
        upstream client supports token refresh.
        '''
        if cfg:
            return client.CoreV1Api(api_client=ApiClient(configuration=cfg))
        else:
            return client.CoreV1Api()

except ImportError as e:
    # We need an exception class to be able to use it in ``except`` elsewhere
    # in the code base
    ApiException = BaseException
    has_kubernetes = False
    _import_err = e


def get_kube_client(in_cluster=conf.getboolean('kubernetes', 'in_cluster'),  # type: bool
                    cluster_context=None,  # type: Optional[str]
                    config_file=None,  # type: Optional[str]
                    ):
    """
    Retrieves Kubernetes client

    :param in_cluster: whether we are in cluster
    :type in_cluster: bool
    :param cluster_context: context of the cluster
    :type cluster_context: str
    :param config_file: configuration file
    :type config_file: str
    :return kubernetes client
    :rtype client.CoreV1Api
    """

    if not has_kubernetes:
        raise _import_err

    if not in_cluster:
        if cluster_context is None:
            cluster_context = conf.get('kubernetes', 'cluster_context', fallback=None)
        if config_file is None:
            config_file = conf.get('kubernetes', 'config_file', fallback=None)

    client_conf = _get_kube_config(in_cluster, cluster_context, config_file)
    return _get_client_with_patched_configuration(client_conf)
