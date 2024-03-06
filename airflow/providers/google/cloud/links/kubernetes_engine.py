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

import json
from typing import TYPE_CHECKING

from google.cloud.container_v1.types import Cluster

from airflow.providers.google.cloud.links.base import BaseGoogleLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

KUBERNETES_BASE_LINK = "/kubernetes"
KUBERNETES_CLUSTER_LINK = (
    KUBERNETES_BASE_LINK + "/clusters/details/{location}/{cluster_name}/details?project={project_id}"
)
KUBERNETES_POD_LINK = (
    KUBERNETES_BASE_LINK
    + "/pod/{location}/{cluster_name}/{namespace}/{pod_name}/details?project={project_id}"
)
KUBERNETES_JOB_LINK = (
    KUBERNETES_BASE_LINK
    + "/job/{location}/{cluster_name}/{namespace}/{job_name}/details?project={project_id}"
)
KUBERNETES_WORKLOADS_LINK = (
    KUBERNETES_BASE_LINK + '/workload/overview?project={project_id}&pageState=("savedViews":'
    '("c":%5B"gke%2F{location}%2F{cluster_name}"%5D,"n":%5B"{namespace}"%5D))'
)


class KubernetesEngineClusterLink(BaseGoogleLink):
    """Helper class for constructing Kubernetes Engine Cluster Link."""

    name = "Kubernetes Cluster"
    key = "kubernetes_cluster_conf"
    format_str = KUBERNETES_CLUSTER_LINK

    @staticmethod
    def persist(context: Context, task_instance, cluster: dict | Cluster | None):
        if isinstance(cluster, dict):
            cluster = Cluster.from_json(json.dumps(cluster))

        task_instance.xcom_push(
            context=context,
            key=KubernetesEngineClusterLink.key,
            value={
                "location": task_instance.location,
                "cluster_name": cluster.name,  # type: ignore
                "project_id": task_instance.project_id,
            },
        )


class KubernetesEnginePodLink(BaseGoogleLink):
    """Helper class for constructing Kubernetes Engine Pod Link."""

    name = "Kubernetes Pod"
    key = "kubernetes_pod_conf"
    format_str = KUBERNETES_POD_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=KubernetesEnginePodLink.key,
            value={
                "location": task_instance.location,
                "cluster_name": task_instance.cluster_name,
                "namespace": task_instance.pod.metadata.namespace,
                "pod_name": task_instance.pod.metadata.name,
                "project_id": task_instance.project_id,
            },
        )


class KubernetesEngineJobLink(BaseGoogleLink):
    """Helper class for constructing Kubernetes Engine Job Link."""

    name = "Kubernetes Job"
    key = "kubernetes_job_conf"
    format_str = KUBERNETES_JOB_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=KubernetesEngineJobLink.key,
            value={
                "location": task_instance.location,
                "cluster_name": task_instance.cluster_name,
                "namespace": task_instance.job.metadata.namespace,
                "job_name": task_instance.job.metadata.name,
                "project_id": task_instance.project_id,
            },
        )


class KubernetesEngineWorkloadsLink(BaseGoogleLink):
    """Helper class for constructing Kubernetes Engine Workloads Link."""

    name = "Kubernetes Workloads"
    key = "kubernetes_workloads_conf"
    format_str = KUBERNETES_WORKLOADS_LINK

    @staticmethod
    def persist(
        context: Context,
        task_instance,
    ):
        task_instance.xcom_push(
            context=context,
            key=KubernetesEngineWorkloadsLink.key,
            value={
                "location": task_instance.location,
                "cluster_name": task_instance.cluster_name,
                "namespace": task_instance.namespace,
                "project_id": task_instance.project_id,
            },
        )
