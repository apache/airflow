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

from airflow.providers.google.cloud.links.kubernetes_engine import (
    KubernetesEngineClusterLink,
    KubernetesEngineJobLink,
    KubernetesEnginePodLink,
    KubernetesEngineWorkloadsLink,
)

EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_NAME = "Kubernetes Cluster"
EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_KEY = "kubernetes_cluster_conf"
EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_FORMAT_STR = (
    "/kubernetes/clusters/details/{location}/{cluster_name}/details?project={project_id}"
)
EXPECTED_KUBERNETES_ENGINE_POD_LINK_NAME = "Kubernetes Pod"
EXPECTED_KUBERNETES_ENGINE_POD_LINK_KEY = "kubernetes_pod_conf"
EXPECTED_KUBERNETES_ENGINE_POD_LINK_FORMAT_STR = (
    "/kubernetes/pod/{location}/{cluster_name}/{namespace}/{pod_name}/details?project={project_id}"
)
EXPECTED_KUBERNETES_ENGINE_JOB_LINK_NAME = "Kubernetes Job"
EXPECTED_KUBERNETES_ENGINE_JOB_LINK_KEY = "kubernetes_job_conf"
EXPECTED_KUBERNETES_ENGINE_JOB_LINK_FORMAT_STR = (
    "/kubernetes/job/{location}/{cluster_name}/{namespace}/{job_name}/details?project={project_id}"
)
EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_NAME = "Kubernetes Workloads"
EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_KEY = "kubernetes_workloads_conf"
EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_FORMAT_STR = '/kubernetes/workload/overview?project={project_id}&pageState=("savedViews":("c":%5B"gke%2F{location}%2F{cluster_name}"%5D,"n":%5B"{namespace}"%5D))'


class TestKubernetesEngineClusterLink:
    def test_class_attributes(self):
        assert KubernetesEngineClusterLink.name == EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_NAME
        assert KubernetesEngineClusterLink.key == EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_KEY
        assert KubernetesEngineClusterLink.format_str == EXPECTED_KUBERNETES_ENGINE_CLUSTER_LINK_FORMAT_STR


class TestKubernetesEnginePodLink:
    def test_class_attributes(self):
        assert KubernetesEnginePodLink.name == EXPECTED_KUBERNETES_ENGINE_POD_LINK_NAME
        assert KubernetesEnginePodLink.key == EXPECTED_KUBERNETES_ENGINE_POD_LINK_KEY
        assert KubernetesEnginePodLink.format_str == EXPECTED_KUBERNETES_ENGINE_POD_LINK_FORMAT_STR


class TestKubernetesEngineJobLink:
    def test_class_attributes(self):
        assert KubernetesEngineJobLink.name == EXPECTED_KUBERNETES_ENGINE_JOB_LINK_NAME
        assert KubernetesEngineJobLink.key == EXPECTED_KUBERNETES_ENGINE_JOB_LINK_KEY
        assert KubernetesEngineJobLink.format_str == EXPECTED_KUBERNETES_ENGINE_JOB_LINK_FORMAT_STR


class TestKubernetesEngineWorkloadsLink:
    def test_class_attributes(self):
        assert KubernetesEngineWorkloadsLink.name == EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_NAME
        assert KubernetesEngineWorkloadsLink.key == EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_KEY
        assert (
            KubernetesEngineWorkloadsLink.format_str == EXPECTED_KUBERNETES_ENGINE_WORKLOADS_LINK_FORMAT_STR
        )
