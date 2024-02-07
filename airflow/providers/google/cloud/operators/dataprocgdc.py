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
#
"""This module contains Dataproc GDC operators using KRM APIs."""

from __future__ import annotations

import datetime
from functools import cached_property
from typing import Sequence, TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import _load_body_to_dict
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from kubernetes.client import CoreV1Api, ApiException
from kubernetes.watch import Watch


if TYPE_CHECKING:
    from kubernetes.client.models import CoreV1EventList
    from airflow.utils.context import Context


class DataprocGDCSubmitSparkJobKrmOperator(BaseOperator):
    """
    Creates sparkApplication object in dpgdc cluster using KRM APIs.

    :param application_file: Defines gdc 'custom_resource_definition' of
    'sparkApplication'
        path to a '.yaml' file, '.json' file, YAML string or python dictionary.
    :param namespace: kubernetes namespace to put sparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id
    <howto/connection:kubernetes>` for the GDC cluster.
    :param api_group: DPGDC api group
    :param api_version: DPGDC api version
    :param watch: whether to watch the job status and logs or not
    """

    template_fields: Sequence[str] = "application_file"
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        application_file: str | dict,
        kubernetes_conn_id: str = "myk8s",
        config_file: str | None = None,
        cluster_context: str | None = None,
        namespace: str = "default",
        api_group: str = "dataprocgdc.cloud.google.com",
        api_version: str = "v1alpha1",
        in_cluster: bool = False,
        watch: bool = True,
        **kwargs,
    ) -> None:
        """
        Args:
          application_file:
          kubernetes_conn_id:
          config_file:
          cluster_context:
          namespace:
          api_group:
          api_version:
          in_cluster:
          watch:
          **kwargs:
        """
        super().__init__(**kwargs)
        self.application_file = application_file
        self.kubernetes_conn_id = kubernetes_conn_id
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.namespace = namespace
        self.api_group = api_group
        self.api_version = api_version
        self.in_cluster = in_cluster
        self.watch = watch
        self.plural = "sparkapplications"

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

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
        if isinstance(self.application_file, str):
            body = _load_body_to_dict(self.application_file)
        else:
            body = self.application_file
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
                    }
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
        if isinstance(self.application_file, str):
            body = _load_body_to_dict(self.application_file)
        else:
            body = self.application_file
        name = body["metadata"]["name"]
        namespace = self.namespace or self.hook.get_namespace()
        self.hook.delete_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            namespace=namespace,
            name=name)


class DataprocGdcCreateAppEnvironmentKrmOperator(BaseOperator):
    """
    Creates Application Environment object in dpgdc cluster using KRM APIs.

    :param application_file: Defines gdc 'custom_resource_definition' of
    'sparkApplication' path to a '.yaml' file, '.json' file, YAML string or python dictionary.
    :param namespace: kubernetes namespace to put sparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id
    <howto/connection:kubernetes>` for the GDC cluster.
    :param api_group: DPGDC api group
    :param api_version: DPGDC api version
    """

    template_fields: Sequence[str] = ("application_file")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
        self,
        *,
        application_file: str | dict,
        kubernetes_conn_id: str = "myk8s",
        config_file: str | None = None,
        cluster_context: str | None = None,
        namespace: str = "default",
        api_group: str = "dataprocgdc.cloud.google.com",
        api_version: str = "v1alpha1",
        in_cluster: bool = False,
        **kwargs,
    ) -> None:
        """
        Args:
          application_file:
          kubernetes_conn_id:
          config_file:
          cluster_context:
          namespace:
          api_group:
          api_version:
          in_cluster:
          **kwargs:
        """
        super().__init__(**kwargs)
        self.application_file = application_file
        self.kubernetes_conn_id = kubernetes_conn_id
        self.config_file = config_file
        self.cluster_context = cluster_context
        self.namespace = namespace
        self.api_group = api_group
        self.api_version = api_version
        self.in_cluster = in_cluster
        self.plural = "applicationenvironments"

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context
        )
        return hook

    @cached_property
    def client(self) -> CoreV1Api:
        return self.hook.core_v1_client

    def execute(self, context: Context):
        if isinstance(self.application_file, str):
            body = _load_body_to_dict(self.application_file)
        else:
            body = self.application_file
        name = body["metadata"]["name"]
        namespace = self.namespace or self.hook.get_namespace()

        response = self.hook.create_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=body,
            namespace=namespace,
        )

        return response

# TODO Implement DataprocGDC operator using OnePlatform APIs.
