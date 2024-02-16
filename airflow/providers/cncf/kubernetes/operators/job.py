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

import copy
import logging
import os
from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from kubernetes.client import BatchV1Api, models as k8s
from kubernetes.client.api_client import ApiClient

from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    add_unique_suffix,
    create_unique_id,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator, merge_objects
from airflow.utils import yaml

if TYPE_CHECKING:
    from airflow.utils.context import Context

log = logging.getLogger(__name__)


class KubernetesJobOperator(KubernetesPodOperator):
    """
    Executes a Kubernetes Job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesJobOperator`

    .. note::
        If you use `Google Kubernetes Engine <https://cloud.google.com/kubernetes-engine/>`__
        and Airflow is not running in the same cluster, consider using
        :class:`~airflow.providers.google.cloud.operators.kubernetes_engine.GKEStartJobOperator`, which
        simplifies the authorization process.

    :param job_template_file: path to job template file (templated)
    :param full_job_spec: The complete JodSpec
    :param backoff_limit: Specifies the number of retries before marking this job failed. Defaults to 6
    :param completion_mode: CompletionMode specifies how Pod completions are tracked. It can be `NonIndexed` (default) or `Indexed`.
    :param completions: Specifies the desired number of successfully finished pods the job should be run with.
    :param manual_selector: manualSelector controls generation of pod labels and pod selectors.
    :param parallelism: Specifies the maximum desired number of pods the job should run at any given time.
    :param selector: The selector of this V1JobSpec.
    :param suspend: Suspend specifies whether the Job controller should create Pods or not.
    :param ttl_seconds_after_finished: ttlSecondsAfterFinished limits the lifetime of a Job that has finished execution (either Complete or Failed).
    """

    template_fields: Sequence[str] = tuple({"job_template_file"} | set(KubernetesPodOperator.template_fields))

    def __init__(
        self,
        *,
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
        self.job_template_file = job_template_file
        self.full_job_spec = full_job_spec
        self.job_request_obj: k8s.V1Job | None = None
        self.job: k8s.V1Job | None = None
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

    @cached_property
    def client(self) -> BatchV1Api:
        return self.hook.batch_v1_client

    def create_job(self, job_request_obj: k8s.V1Job) -> k8s.V1Job:
        self.log.debug("Starting job:\n%s", yaml.safe_dump(job_request_obj.to_dict()))
        self.hook.create_job(job=job_request_obj)

        return job_request_obj

    def execute(self, context: Context):
        self.job_request_obj = self.build_job_request_obj(context)
        self.job = self.create_job(  # must set `self.job` for `on_kill`
            job_request_obj=self.job_request_obj
        )

        ti = context["ti"]
        ti.xcom_push(key="job_name", value=self.job.metadata.name)
        ti.xcom_push(key="job_namespace", value=self.job.metadata.namespace)

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

    def on_kill(self) -> None:
        if self.job:
            job = self.job
            kwargs = {
                "name": job.metadata.name,
                "namespace": job.metadata.namespace,
            }
            if self.termination_grace_period is not None:
                kwargs.update(grace_period_seconds=self.termination_grace_period)
            self.client.delete_namespaced_job(**kwargs)

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
                job_template = self.reconcile_jobs(job_template, self.full_job_spec)
        elif self.full_job_spec:
            job_template = self.full_job_spec
        else:
            job_template = k8s.V1Job(metadata=k8s.V1ObjectMeta())

        pod_template = super().build_pod_request_obj(context)
        pod_template_spec = k8s.V1PodTemplateSpec(
            metadata=pod_template.metadata,
            spec=pod_template.spec,
        )

        job = k8s.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=k8s.V1ObjectMeta(
                namespace=self.namespace,
                labels=self.labels,
                name=self.name,
                annotations=self.annotations,
            ),
            spec=k8s.V1JobSpec(
                active_deadline_seconds=self.active_deadline_seconds,
                backoff_limit=self.backoff_limit,
                completion_mode=self.completion_mode,
                completions=self.completions,
                manual_selector=self.manual_selector,
                parallelism=self.parallelism,
                selector=self.selector,
                suspend=self.suspend,
                template=pod_template_spec,
                ttl_seconds_after_finished=self.ttl_seconds_after_finished,
            ),
        )

        job = self.reconcile_jobs(job_template, job)

        if not job.metadata.name:
            job.metadata.name = create_unique_id(
                task_id=self.task_id, unique=self.random_name_suffix, max_length=80
            )
        elif self.random_name_suffix:
            # user has supplied job name, we're just adding suffix
            job.metadata.name = add_unique_suffix(name=job.metadata.name)

        job.metadata.name = f"job-{job.metadata.name}"

        if not job.metadata.namespace:
            hook_namespace = self.hook.get_namespace()
            job_namespace = self.namespace or hook_namespace or self._incluster_namespace or "default"
            job.metadata.namespace = job_namespace

        self.log.info("Building job %s ", job.metadata.name)

        return job

    @staticmethod
    def reconcile_jobs(base_job: k8s.V1Job, client_job: k8s.V1Job | None) -> k8s.V1Job:
        """
        Merge Kubernetes Job objects.

        :param base_job: has the base attributes which are overwritten if they exist
            in the client job and remain if they do not exist in the client_job
        :param client_job: the job that the client wants to create.
        :return: the merged jobs

        This can't be done recursively as certain fields are overwritten and some are concatenated.
        """
        if client_job is None:
            return base_job

        client_job_cp = copy.deepcopy(client_job)
        client_job_cp.spec = KubernetesJobOperator.reconcile_job_specs(base_job.spec, client_job_cp.spec)
        client_job_cp.metadata = PodGenerator.reconcile_metadata(base_job.metadata, client_job_cp.metadata)
        client_job_cp = merge_objects(base_job, client_job_cp)

        return client_job_cp

    @staticmethod
    def reconcile_job_specs(
        base_spec: k8s.V1JobSpec | None, client_spec: k8s.V1JobSpec | None
    ) -> k8s.V1JobSpec | None:
        """
        Merge Kubernetes JobSpec objects.

        :param base_spec: has the base attributes which are overwritten if they exist
            in the client_spec and remain if they do not exist in the client_spec
        :param client_spec: the spec that the client wants to create.
        :return: the merged specs
        """
        if base_spec and not client_spec:
            return base_spec
        if not base_spec and client_spec:
            return client_spec
        elif client_spec and base_spec:
            client_spec.template.spec = PodGenerator.reconcile_specs(
                base_spec.template.spec, client_spec.template.spec
            )
            client_spec.template.metadata = PodGenerator.reconcile_metadata(
                base_spec.template.metadata, client_spec.template.metadata
            )
            return merge_objects(base_spec, client_spec)

        return None
