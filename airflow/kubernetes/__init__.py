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

from airflow.utils.deprecation_tools import add_deprecated_classes

__deprecated_classes: dict[str, dict[str, str]] = {
    "kubernetes_helper_functions": {
        "add_pod_suffix": "airflow.providers.cncf.kubernetes.kubernetes_helper_functions.add_pod_suffix.",
        "annotations_for_logging_task_metadata": "airflow.providers.cncf.kubernetes."
        "kubernetes_helper_functions."
        "annotations_for_logging_task_metadata.",
        "annotations_to_key": "airflow.providers.cncf.kubernetes."
        "kubernetes_helper_functions.annotations_to_key",
        "create_pod_id": "airflow.providers.cncf.kubernetes.kubernetes_helper_functions.create_pod_id",
        "get_logs_task_metadata": "airflow.providers.cncf.kubernetes."
        "kubernetes_helper_functions.get_logs_task_metadata",
        "rand_str": "airflow.providers.cncf.kubernetes.kubernetes_helper_functions.rand_str",
    },
    "pod": {
        "Port": "airflow.providers.cncf.kubernetes.backcompat.pod.Port",
        "Resources": "airflow.providers.cncf.kubernetes.backcompat.pod.Resources",
    },
    "pod_launcher": {
        "PodLauncher": "airflow.providers.cncf.kubernetes.pod_launcher.PodLauncher",
        "PodStatus": "airflow.providers.cncf.kubernetes.pod_launcher.PodStatus",
    },
    "pod_launcher_deprecated": {
        "PodLauncher": "airflow.providers.cncf.kubernetes.pod_launcher_deprecated.PodLauncher",
        "PodStatus": "airflow.providers.cncf.kubernetes.pod_launcher_deprecated.PodStatus",
        # imports of imports from other kubernetes modules (in case they are imported from here)
        "get_kube_client": "airflow.providers.cncf.kubernetes.kube_client.get_kube_client",
        "PodDefaults": "airflow.providers.cncf.kubernetes.pod_generator_deprecated.PodDefaults",
    },
    "pod_runtime_info_env": {
        "PodRuntimeInfoEnv": "airflow.providers.cncf.kubernetes.backcompat."
        "pod_runtime_info_env.PodRuntimeInfoEnv",
    },
    "volume": {
        "Volume": "airflow.providers.cncf.kubernetes.backcompat.volume.Volume",
    },
    "volume_mount": {
        "VolumeMount": "airflow.providers.cncf.kubernetes.backcompat.volume_mount.VolumeMount",
    },
    # the below classes are not served from provider but from internal pre_7_4_0_compatibility package
    "k8s_model": {
        "K8SModel": "airflow.kubernetes.pre_7_4_0_compatibility.k8s_model.K8SModel",
        "append_to_pod": "airflow.kubernetes.pre_7_4_0_compatibility.k8s_model.append_to_pod",
    },
    "kube_client": {
        "_disable_verify_ssl": "airflow.kubernetes.pre_7_4_0_compatibility.kube_client._disable_verify_ssl",
        "_enable_tcp_keepalive": "airflow.kubernetes.pre_7_4_0_compatibility.kube_client."
        "_enable_tcp_keepalive",
        "get_kube_client": "airflow.kubernetes.pre_7_4_0_compatibility.kube_client.get_kube_client",
    },
    "pod_generator": {
        "datetime_to_label_safe_datestring": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator"
        ".datetime_to_label_safe_datestring",
        "extend_object_field": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator."
        "extend_object_field",
        "label_safe_datestring_to_datetime": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator."
        "label_safe_datestring_to_datetime",
        "make_safe_label_value": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator."
        "make_safe_label_value",
        "merge_objects": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator.merge_objects",
        "PodGenerator": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator.PodGenerator",
        # imports of imports from other kubernetes modules (in case they are imported from here)
        "PodGeneratorDeprecated": "airflow.kubernetes.pre_7_4_0_compatibility."
        "pod_generator_deprecated.PodGenerator",
        "PodDefaults": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator_deprecated.PodDefaults",
        # those two are inlined in kubernetes.pre_7_4_0_compatibility.pod_generator even if
        # originally they were imported in airflow.kubernetes.pod_generator
        "add_pod_suffix": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator.add_pod_suffix",
        "rand_str": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator.rand_str",
    },
    "pod_generator_deprecated": {
        "make_safe_label_value": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator_deprecated."
        "make_safe_label_value",
        "PodDefaults": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator_deprecated.PodDefaults",
        "PodGenerator": "airflow.kubernetes.pre_7_4_0_compatibility.pod_generator_deprecated.PodGenerator",
    },
    "secret": {
        "Secret": "airflow.kubernetes.pre_7_4_0_compatibility.secret.Secret",
        # imports of imports from other kubernetes modules (in case they are imported from here)
        "K8SModel": "airflow.kubernetes.pre_7_4_0_compatibility.k8s_model.K8SModel",
    },
}

__override_deprecated_names: dict[str, dict[str, str]] = {
    "pod": {
        "Port": "kubernetes.client.models.V1ContainerPort",
        "Resources": "kubernetes.client.models.V1ResourceRequirements",
    },
    "pod_runtime_info_env": {
        "PodRuntimeInfoEnv": "kubernetes.client.models.V1EnvVar",
    },
    "volume": {
        "Volume": "kubernetes.client.models.V1Volume",
    },
    "volume_mount": {
        "VolumeMount": "kubernetes.client.models.V1VolumeMount",
    },
    "k8s_model": {
        "K8SModel": "airflow.airflow.providers.cncf.kubernetes.k8s_model.K8SModel",
        "append_to_pod": "airflow.airflow.providers.cncf.kubernetes.k8s_model.append_to_pod",
    },
    "kube_client": {
        "_disable_verify_ssl": "airflow.kubernetes.airflow.providers.cncf.kubernetes."
        "kube_client._disable_verify_ssl",
        "_enable_tcp_keepalive": "airflow.kubernetes.airflow.providers.cncf.kubernetes.kube_client."
        "_enable_tcp_keepalive",
        "get_kube_client": "airflow.kubernetes.airflow.providers.cncf.kubernetes.kube_client.get_kube_client",
    },
    "pod_generator": {
        "datetime_to_label_safe_datestring": "airflow.providers.cncf.kubernetes.pod_generator"
        ".datetime_to_label_safe_datestring",
        "extend_object_field": "airflow.kubernetes.airflow.providers.cncf.kubernetes.pod_generator."
        "extend_object_field",
        "label_safe_datestring_to_datetime": "airflow.providers.cncf.kubernetes.pod_generator."
        "label_safe_datestring_to_datetime",
        "make_safe_label_value": "airflow.providers.cncf.kubernetes.pod_generator.make_safe_label_value",
        "merge_objects": "airflow.providers.cncf.kubernetes.pod_generator.merge_objects",
        "PodGenerator": "airflow.providers.cncf.kubernetes.pod_generator.PodGenerator",
    },
    "pod_generator_deprecated": {
        "make_safe_label_value": "airflow.providers.cncf.kubernetes.pod_generator_deprecated."
        "make_safe_label_value",
        "PodDefaults": "airflow.providers.cncf.kubernetes.pod_generator_deprecated.PodDefaults",
        "PodGenerator": "airflow.providers.cncf.kubernetes.pod_generator_deprecated.PodGenerator",
    },
    "secret": {
        "Secret": "airflow.providers.cncf.kubernetes.secret.Secret",
    },
}
add_deprecated_classes(
    __deprecated_classes,
    __name__,
    __override_deprecated_names,
    "The `cncf.kubernetes` provider must be >= 7.4.0 for that.",
)
