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
"""Executes a Kubernetes Job."""
from __future__ import annotations

import logging
import os
import re
from functools import cached_property
from typing import TYPE_CHECKING

from kubernetes.client import models as k8s
from kubernetes.client.api_client import ApiClient

from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.backcompat.backwards_compat_converters import (
    convert_affinity,
    convert_env_vars,
    convert_image_pull_secrets,
    convert_port,
    convert_toleration,
    convert_volume,
    convert_volume_mount,
)
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.utils.k8s_yaml_manager import (
    reconcile_jobs,
)
from airflow.utils import yaml
from airflow.utils.helpers import validate_key

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


class KubernetesJobOperator(BaseOperator):
    """
    Executes a Kubernetes Job


    """

    def __init__(
        self,
        *,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        job_namespace: str | None = None,
        pod_namespace: str | None = None,
        image: str | None = None,
        job_name: str | None = None,
        pod_name: str | None = None,
        cmds: list[str] | None = None,
        arguments: list[str] | None = None,
        ports: list[k8s.V1ContainerPort] | None = None,
        volume_mounts: list[k8s.V1VolumeMount] | None = None,
        volumes: list[k8s.V1Volume] | None = None,
        env_vars: list[k8s.V1EnvVar] | dict[str, str] | None = None,
        env_from: list[k8s.V1EnvFromSource] | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        job_labels: dict | None = None,
        pod_labels: dict | None = None,
        image_pull_policy: str | None = None,
        job_annotations: dict | None = None,
        pod_annotations: dict | None = None,
        container_resources: k8s.V1ResourceRequirements | None = None,
        affinity: k8s.V1Affinity | None = None,
        config_file: str | None = None,
        node_selector: dict | None = None,
        image_pull_secrets: list[k8s.V1LocalObjectReference] | None = None,
        service_account_name: str | None = None,
        hostnetwork: bool = False,
        host_aliases: list[k8s.V1HostAlias] | None = None,
        tolerations: list[k8s.V1Toleration] | None = None,
        security_context: k8s.V1PodSecurityContext | dict | None = None,
        container_security_context: k8s.V1SecurityContext | dict | None = None,
        dnspolicy: str | None = None,
        dns_config: k8s.V1PodDNSConfig | None = None,
        hostname: str | None = None,
        subdomain: str | None = None,
        schedulername: str | None = None,
        init_containers: list[k8s.V1Container] | None = None,
        priority_class_name: str | None = None,
        base_container_name: str | None = None,
        termination_message_policy: str = "File",
        job_active_deadline_seconds: int | None = None,
        pod_active_deadline_seconds: int | None = None,
        job_template_file: str | None = None,
        full_job_spec: k8s.V1Job | None = None,
        backoff_limit: int | None = None,
        completion_mode: str | None = None,
        completions: int | None = None,
        manual_selector: bool | None = None,
        parallelism: int | None = None,
        selector: k8s.V1LabelSelector | None = None,
        suspend: bool | None = None,
        ttl_seconds_after_finished: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.kubernetes_conn_id = kubernetes_conn_id
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.config_file = config_file
        self.job_template_file = job_template_file
        self.full_job_spec = full_job_spec
        self.job_request_obj: k8s.V1Job | None = None
        self.job: k8s.V1Job | None = None
        self.image = image
        self.job_namespace = job_namespace
        self.pod_namespace = pod_namespace
        self.cmds = cmds or []
        self.arguments = arguments or []
        self.job_labels = job_labels or {}
        self.pod_labels = pod_labels or {}
        self.env_vars = convert_env_vars(env_vars) if env_vars else []
        self.env_from = env_from or []
        self.ports = [convert_port(p) for p in ports] if ports else []
        self.volume_mounts = [convert_volume_mount(v) for v in volume_mounts] if volume_mounts else []
        self.volumes = [convert_volume(volume) for volume in volumes] if volumes else []
        self.image_pull_policy = image_pull_policy
        self.node_selector = node_selector or {}
        self.job_annotations = job_annotations or {}
        self.pod_annotations = pod_annotations or {}
        self.affinity = convert_affinity(affinity) if affinity else {}
        self.container_resources = container_resources
        self.config_file = config_file
        self.image_pull_secrets = convert_image_pull_secrets(image_pull_secrets) if image_pull_secrets else []
        self.service_account_name = service_account_name
        self.hostnetwork = hostnetwork
        self.host_aliases = host_aliases
        self.tolerations = (
            [convert_toleration(toleration) for toleration in tolerations] if tolerations else []
        )
        self.security_context = security_context or {}
        self.container_security_context = container_security_context
        self.dnspolicy = dnspolicy
        self.dns_config = dns_config
        self.hostname = hostname
        self.subdomain = subdomain
        self.schedulername = schedulername
        self.init_containers = init_containers or []
        self.priority_class_name = priority_class_name
        self.job_name = self._set_name(job_name)
        self.pod_name = self._set_name(pod_name)
        self.pod_request_obj: k8s.V1Pod | None = None
        self.pod: k8s.V1Pod | None = None
        self.base_container_name = base_container_name or "base"
        self.remote_pod: k8s.V1Pod | None = None
        self.termination_message_policy = termination_message_policy
        self.job_active_deadline_seconds = job_active_deadline_seconds
        self.pod_active_deadline_seconds = pod_active_deadline_seconds
        self.backoff_limit = backoff_limit
        self.completion_mode = completion_mode
        self.completions = completions
        self.manual_selector = manual_selector
        self.parallelism = parallelism
        self.selector = selector
        self.suspend = suspend
        self.ttl_seconds_after_finished = ttl_seconds_after_finished

    @cached_property
    def _incluster_namespace(self):
        from pathlib import Path

        path = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
        return path.exists() and path.read_text() or None

    @cached_property
    def hook(self) -> KubernetesHook:
        hook = KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )
        return hook

    def create_job(self, job_request_obj: k8s.V1Job) -> k8s.V1Job:
        self.log.debug("Starting job:\n%s", yaml.safe_dump(job_request_obj.to_dict()))
        self.hook.create_job(job=job_request_obj)

        return job_request_obj

    def execute(self, context: Context):
        self.job_request_obj = self.build_job_request_obj(context)
        self.job = self.create_job(  # must set `self.job` for `on_kill`
            job_request_obj=self.job_request_obj
        )

    @staticmethod
    def deserialize_job_template_file(path: str) -> k8s.V1Job:
        """
        Generate a Job from a file.

        Unfortunately we need access to the private method
        ``_ApiClient__deserialize_model`` from the kubernetes client.
        This issue is tracked here: https://github.com/kubernetes-client/python/issues/977.

        :param path: Path to the file
        :return: a kubernetes.client.models.V1Job
        """
        if os.path.exists(path):
            with open(path) as stream:
                job = yaml.safe_load(stream)
        else:
            job = None
            log.warning("Template file %s does not exist", path)

        api_client = ApiClient()
        return api_client._ApiClient__deserialize_model(job, k8s.V1Job)

    @staticmethod
    def _set_name(name: str | None) -> str | None:
        if name is not None:
            validate_key(name, max_length=220)
            return re.sub(r"[^a-z0-9-]+", "-", name.lower())
        return None

    def build_job_request_obj(self, context: Context | None = None) -> k8s.V1Job:
        """
        Return V1Job object based on job template file, full job spec, and other operator parameters.

        The V1Job attributes are derived (in order of precedence) from operator params, full job spec, job
        template file.
        """
        self.log.debug("Creating job for KubernetesJobOperator task %s", self.task_id)
        if self.job_template_file:
            self.log.debug("Job template file found, will parse for base job")
            job_template = self.deserialize_job_template_file(self.job_template_file)
            if self.full_job_spec:
                # looks one more time in the future
                job_template = reconcile_jobs(job_template, self.full_job_spec)
        elif self.full_job_spec:
            job_template = self.full_job_spec
        else:
            job_template = k8s.V1Job(metadata=k8s.V1ObjectMeta())

        pod_template = k8s.V1PodTemplateSpec(
            metadata=k8s.V1ObjectMeta(
                namespace=self.pod_namespace,
                labels=self.pod_labels,
                name=self.pod_name,
                annotations=self.pod_annotations,
            ),
            spec=k8s.V1PodSpec(
                node_selector=self.node_selector,
                affinity=self.affinity,
                tolerations=self.tolerations,
                init_containers=self.init_containers,
                host_aliases=self.host_aliases,
                containers=[
                    k8s.V1Container(
                        image=self.image,
                        name=self.base_container_name,
                        command=self.cmds,
                        ports=self.ports,
                        image_pull_policy=self.image_pull_policy,
                        resources=self.container_resources,
                        volume_mounts=self.volume_mounts,
                        args=self.arguments,
                        env=self.env_vars,
                        env_from=self.env_from,
                        security_context=self.container_security_context,
                        termination_message_policy=self.termination_message_policy,
                    )
                ],
                image_pull_secrets=self.image_pull_secrets,
                service_account_name=self.service_account_name,
                host_network=self.hostnetwork,
                hostname=self.hostname,
                subdomain=self.subdomain,
                security_context=self.security_context,
                dns_policy=self.dnspolicy,
                dns_config=self.dns_config,
                scheduler_name=self.schedulername,
                restart_policy="Never",
                priority_class_name=self.priority_class_name,
                volumes=self.volumes,
                active_deadline_seconds=self.pod_active_deadline_seconds,
            ),
        )

        job = k8s.V1Job(
            api_version="v1",
            kind="Job",
            metadata=k8s.V1ObjectMeta(
                namespace=self.job_namespace,
                labels=self.job_labels,
                name=self.job_name,
                annotations=self.job_annotations,
            ),
            spec=k8s.V1JobSpec(
                active_deadline_seconds=self.job_active_deadline_seconds,
                backoff_limit=self.backoff_limit,
                completion_mode=self.completion_mode,
                completions=self.completions,
                manual_selector=self.manual_selector,
                parallelism=self.parallelism,
                selector=self.selector,
                suspend=self.suspend,
                template=pod_template,
                ttl_seconds_after_finished=self.ttl_seconds_after_finished,
            ),
        )

        job = reconcile_jobs(job_template, job)

        # if not pod.metadata.name:
        #     pod.metadata.name = _create_pod_id(
        #         task_id=self.task_id, unique=self.random_name_suffix, max_length=80
        #     )
        # elif self.random_name_suffix:
        #     # user has supplied pod name, we're just adding suffix
        #     pod.metadata.name = _add_pod_suffix(pod_name=pod.metadata.name)

        if not job.metadata.namespace:
            hook_namespace = self.hook.get_namespace()
            job_namespace = self.job_namespace or hook_namespace or self._incluster_namespace or "default"
            job.metadata.namespace = job_namespace

        # for secret in self.secrets:
        #     self.log.debug("Adding secret to task %s", self.task_id)
        #     pod = secret.attach_to_pod(pod)
        # if self.do_xcom_push:
        #     self.log.debug("Adding xcom sidecar to task %s", self.task_id)
        #     pod = xcom_sidecar.add_xcom_sidecar(
        #         pod,
        #         sidecar_container_image=self.hook.get_xcom_sidecar_container_image(),
        #         sidecar_container_resources=self.hook.get_xcom_sidecar_container_resources(),
        #     )

        # labels = self._get_ti_pod_labels(context)
        # self.log.info("Building pod %s with labels: %s", pod.metadata.name, labels)

        # # Merge Pod Identifying labels with labels passed to operator
        # pod.metadata.labels.update(labels)
        # # Add Airflow Version to the label
        # # And a label to identify that pod is launched by KubernetesPodOperator
        # pod.metadata.labels.update(
        #     {
        #         "airflow_version": airflow_version.replace("+", "-"),
        #         "airflow_kpo_in_cluster": str(self.hook.is_in_cluster),
        #     }
        # )
        # pod_mutation_hook(pod)
        return job
