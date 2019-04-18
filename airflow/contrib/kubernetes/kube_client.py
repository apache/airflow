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
from airflow.configuration import conf
from six import PY2

try:
    from kubernetes import config, client
    from kubernetes.client.rest import ApiException
    has_kubernetes = True
except ImportError as e:
    # We need an exception class to be able to use it in ``except`` elsewhere
    # in the code base
    ApiException = BaseException
    has_kubernetes = False
    _import_err = e


def _load_kube_config(in_cluster, cluster_context, config_file):
    if not has_kubernetes:
        raise _import_err
    if in_cluster:
        config.load_incluster_config()
    else:
        config.load_kube_config(config_file=config_file, context=cluster_context)
    if PY2:
        # For connect_get_namespaced_pod_exec
        from kubernetes.client import Configuration
        configuration = Configuration()
        configuration.assert_hostname = False
        Configuration.set_default(configuration)
    return client.CoreV1Api()


def get_kube_client(in_cluster=conf.getboolean('kubernetes', 'in_cluster'),
                    cluster_context=None,
                    config_file=None):
    if not in_cluster:
        if cluster_context is None:
            cluster_context = conf.get('kubernetes', 'cluster_context', fallback=None)
        if config_file is None:
            config_file = conf.get('kubernetes', 'config_file', fallback=None)
    return _load_kube_config(in_cluster, cluster_context, config_file)
