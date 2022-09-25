#
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
from typing import TYPE_CHECKING, Optional, Sequence

from kubernetes.client import CoreV1Api

from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class FlinkKubernetesOperator(BaseOperator):
    """
    Creates flinkDeployment object in kubernetes cluster:

    .. seealso::
        For more detail about Flink Deployment Object have a look at the reference:
        https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/custom-resource/reference/#flinkdeployment

    :param application_file: Defines Kubernetes 'custom_resource_definition' of 'flinkDeployment' as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace to put flinkDeployment
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the to Kubernetes cluster.
    :param api_group: kubernetes api group of flinkDeployment
    :param api_version: kubernetes api version of flinkDeployment
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    """

    template_fields: Sequence[str] = ('application_file', 'namespace')
    template_ext: Sequence[str] = ('.yaml', '.yml', '.json')
    ui_color = '#f4a460'

    def __init__(
        self,
        *,
        application_file: str,
        namespace: Optional[str] = None,
        kubernetes_conn_id: str = 'kubernetes_default',
        api_group: str = 'flink.apache.org',
        api_version: str = 'v1beta1',
        in_cluster: Optional[bool] = None,
        cluster_context: Optional[str] = None,
        config_file: Optional[str] = None,
        plural: str = "flinkdeployments",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = plural
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        self._patch_deprecated_k8s_settings(hook)
        return hook

    def _patch_deprecated_k8s_settings(self, hook: KubernetesHook):
        """
        Here we read config from core Airflow config [kubernetes] section.
        In a future release we will stop looking at this section and require users
        to use Airflow connections to configure KPO.

        When we find values there that we need to apply on the hook, we patch special
        hook attributes here.
        """
        # default for enable_tcp_keepalive is True; patch if False
        if conf.getboolean('kubernetes', 'enable_tcp_keepalive') is False:
            hook._deprecated_core_disable_tcp_keepalive = True

        # default verify_ssl is True; patch if False.
        if conf.getboolean('kubernetes', 'verify_ssl') is False:
            hook._deprecated_core_disable_verify_ssl = True

        # default for in_cluster is True; patch if False and no KPO param.
        conf_in_cluster = conf.getboolean('kubernetes', 'in_cluster')
        if self.in_cluster is None and conf_in_cluster is False:
            hook._deprecated_core_in_cluster = conf_in_cluster

        # there's no default for cluster context; if we get something (and no KPO param) patch it.
        conf_cluster_context = conf.get('kubernetes', 'cluster_context', fallback=None)
        if not self.cluster_context and conf_cluster_context:
            hook._deprecated_core_cluster_context = conf_cluster_context

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    def execute(self, context: 'Context'):
        # self.hook.core_v1_client.CustomObjectsApi(api_client=self.api_client)

        self.log.info(
            "Creating flinkApplication with Context: %s and op_context: %s", self.cluster_context, context
        )

        self.log.info("All pods: %s", self.client.list_namespace())
        self.hook.custom_object_client.list_cluster_custom_object(
            group=self.api_group, version=self.api_version, plural=self.plural
        )
        self.log.info("body=self.application_file: %s", self.application_file)
        self.log.info("All pods: %s", self.client.list_pod_for_all_namespaces())
        response = self.hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response
