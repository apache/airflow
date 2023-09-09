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
from __future__ import annotations

import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from kubernetes.client import ApiException
from kubernetes.watch import Watch

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook, _load_body_to_dict

if TYPE_CHECKING:
    from kubernetes.client.models import CoreV1EventList

    from airflow.utils.context import Context


class SparkKubernetesOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster.

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_file: Defines Kubernetes 'custom_resource_definition' of 'sparkApplication' as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace to put sparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the to Kubernetes cluster.
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    :param watch: whether to watch the job status and logs or not
    """

    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        application_file: str,
        namespace: str | None = None,
        kubernetes_conn_id: str = "kubernetes_default",
        api_group: str = "sparkoperator.k8s.io",
        api_version: str = "v1beta2",
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        config_file: str | None = None,
        watch: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.application_file = application_file
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.watch = watch

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    def _get_namespace_event_stream(self, namespace, query_kwargs=None):
        try:
            return Watch().stream(
                self.hook.core_v1_client.list_namespaced_event,
                namespace=namespace,
                watch=True,
                **(query_kwargs or {}),
            )
        except ApiException as e:
            if e.status == 410:  # Resource version is too old
                events: CoreV1EventList = self.hook.core_v1_client.list_namespaced_event(
                    namespace=namespace, watch=False
                )
                resource_version = events.metadata.resource_version
                query_kwargs["resource_version"] = resource_version
                return self._get_namespace_event_stream(namespace, query_kwargs)
            else:
                raise

    def execute(self, context: Context):
        body = _load_body_to_dict(self.application_file)
        name = body["metadata"]["name"]
        namespace = self.namespace or self.hook.get_namespace()

        response = None
        is_job_created = False
        if self.watch:
            try:
                namespace_event_stream = self._get_namespace_event_stream(
                    namespace=namespace,
                    query_kwargs={
                        "field_selector": f"involvedObject.kind=SparkApplication,involvedObject.name={name}"
                    },
                )

                response = self.hook.create_custom_object(
                    group=self.api_group,
                    version=self.api_version,
                    plural=self.plural,
                    body=body,
                    namespace=namespace,
                )
                is_job_created = True
                for event in namespace_event_stream:
                    obj = event["object"]
                    if event["object"].last_timestamp >= datetime.datetime.strptime(
                        response["metadata"]["creationTimestamp"], "%Y-%m-%dT%H:%M:%S%z"
                    ):
                        self.log.info(obj.message)
                        if obj.reason == "SparkDriverRunning":
                            pod_log_stream = Watch().stream(
                                self.hook.core_v1_client.read_namespaced_pod_log,
                                name=f"{name}-driver",
                                namespace=namespace,
                                timestamps=True,
                            )
                            for line in pod_log_stream:
                                self.log.info(line)
                        elif obj.reason in [
                            "SparkApplicationSubmissionFailed",
                            "SparkApplicationFailed",
                            "SparkApplicationDeleted",
                        ]:
                            is_job_created = False
                            raise AirflowException(obj.message)
                        elif obj.reason == "SparkApplicationCompleted":
                            break
                        else:
                            continue
            except Exception:
                if is_job_created:
                    self.on_kill()
                raise

        else:
            response = self.hook.create_custom_object(
                group=self.api_group,
                version=self.api_version,
                plural=self.plural,
                body=body,
                namespace=namespace,
            )

        return response

    def on_kill(self) -> None:
        body = _load_body_to_dict(self.application_file)
        name = body["metadata"]["name"]
        namespace = self.namespace or self.hook.get_namespace()
        self.hook.delete_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            namespace=namespace,
            name=name,
        )
