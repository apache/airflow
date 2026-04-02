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

from typing import TYPE_CHECKING

from airflow.exceptions import AirflowConfigException
from airflow.providers.common.compat.sdk import conf
from airflow.settings import AIRFLOW_HOME

if TYPE_CHECKING:
    from airflow.executors.base_executor import ExecutorConf


class KubeConfig:
    """
    Configuration for Kubernetes.

    :param executor_conf: Optional team-aware configuration object. If not provided,
        falls back to the global configuration for backwards compatibility. This parameter
        supports the multi-team feature introduced in AIP-67.
    """

    core_section = "core"
    kubernetes_section = "kubernetes_executor"
    logging_section = "logging"

    def __init__(self, executor_conf: ExecutorConf | None = None):
        # Use the provided executor_conf for team-aware configuration, or fall back to global conf
        # for backwards compatibility with older versions of Airflow.
        self._conf = executor_conf if executor_conf is not None else conf

        configuration_dict = conf.as_dict(display_sensitive=True)
        self.core_configuration = configuration_dict[self.core_section]
        self.airflow_home = AIRFLOW_HOME
        self.dags_folder = self._conf.get(self.core_section, "dags_folder")
        self.parallelism = self._conf.getint(self.core_section, "parallelism")
        self.pod_template_file = self._conf.get(self.kubernetes_section, "pod_template_file", fallback="")

        self.delete_worker_pods = self._conf.getboolean(self.kubernetes_section, "delete_worker_pods")
        self.delete_worker_pods_on_failure = self._conf.getboolean(
            self.kubernetes_section, "delete_worker_pods_on_failure"
        )
        self.worker_pod_pending_fatal_container_state_reasons: list[str] = []
        fatal_reasons = self._conf.get(
            self.kubernetes_section, "worker_pod_pending_fatal_container_state_reasons", fallback=""
        )
        if fatal_reasons:
            self.worker_pod_pending_fatal_container_state_reasons = [
                r.strip() for r in fatal_reasons.split(",")
            ]

        self.worker_pods_creation_batch_size = self._conf.getint(
            self.kubernetes_section, "worker_pods_creation_batch_size"
        )
        self.worker_container_repository = self._conf.get(
            self.kubernetes_section, "worker_container_repository"
        )
        self.worker_container_tag = self._conf.get(self.kubernetes_section, "worker_container_tag")

        if self.worker_container_repository and self.worker_container_tag:
            self.kube_image = f"{self.worker_container_repository}:{self.worker_container_tag}"
        else:
            # Ignore needed because ExecutorConf.get() returns str | None (no overloads),
            # so mypy infers kube_image as str from the f-string branch and rejects None here.
            # This is a pre-existing type inconsistency: kube_image can be None at runtime,
            # but PodGenerator.construct_pod() declares kube_image: str. Passing None is
            # intentional — the K8s client omits None fields, whereas "" would serialize
            # as 'image': '' in the pod spec.
            self.kube_image = None  # type: ignore[assignment]

        # The Kubernetes Namespace in which the Scheduler and Webserver reside. Note
        # that if your
        # cluster has RBAC enabled, your scheduler may need service account permissions to
        # create, watch, get, and delete pods in this namespace.
        self.kube_namespace = self._conf.get(self.kubernetes_section, "namespace")
        self.multi_namespace_mode = self._conf.getboolean(self.kubernetes_section, "multi_namespace_mode")
        multi_ns_list = self._conf.get(
            self.kubernetes_section, "multi_namespace_mode_namespace_list", fallback=""
        )
        if self.multi_namespace_mode and multi_ns_list:
            self.multi_namespace_mode_namespace_list: list[str] | None = multi_ns_list.split(",")
        else:
            self.multi_namespace_mode_namespace_list = None
        # The Kubernetes Namespace in which pods will be created by the executor. Note
        # that if your
        # cluster has RBAC enabled, your workers may need service account permissions to
        # interact with cluster components.
        self.executor_namespace: str = (
            self._conf.get(self.kubernetes_section, "namespace", fallback="default") or "default"
        )
        self.kube_client_request_args = self._conf.getjson(
            self.kubernetes_section, "kube_client_request_args", fallback={}
        )
        if not isinstance(self.kube_client_request_args, dict):
            raise AirflowConfigException(
                f"[{self.kubernetes_section}] 'kube_client_request_args' expected a JSON dict, got "
                + type(self.kube_client_request_args).__name__
            )
        if self.kube_client_request_args:
            if "_request_timeout" in self.kube_client_request_args and isinstance(
                self.kube_client_request_args["_request_timeout"], list
            ):
                self.kube_client_request_args["_request_timeout"] = tuple(
                    self.kube_client_request_args["_request_timeout"]
                )
        self.delete_option_kwargs = self._conf.getjson(
            self.kubernetes_section, "delete_option_kwargs", fallback={}
        )
        if not isinstance(self.delete_option_kwargs, dict):
            raise AirflowConfigException(
                f"[{self.kubernetes_section}] 'delete_option_kwargs' expected a JSON dict, got "
                + type(self.delete_option_kwargs).__name__
            )
