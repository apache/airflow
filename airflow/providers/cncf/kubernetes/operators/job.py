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
from kubernetes.client.rest import ApiException

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cncf.kubernetes.hooks.kubernetes import KubernetesHook
from airflow.providers.cncf.kubernetes.kubernetes_helper_functions import (
    add_unique_suffix,
    create_unique_id,
)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator, merge_objects
from airflow.providers.cncf.kubernetes.triggers.job import KubernetesJobTrigger
from airflow.utils import yaml
from airflow.utils.context import Context

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
    :param wait_until_job_complete: Whether to wait until started job finished execution (either Complete or
        Failed). Default is False.
    :param job_poll_interval: Interval in seconds between polling the job status. Default is 10.
        Used if the parameter `wait_until_job_complete` set True.
    :param deferrable: Run operator in the deferrable mode. Note that the parameter
        `wait_until_job_complete` must be set True.
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
        wait_until_job_complete: bool = False,
        job_poll_interval: float = 10,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
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
        self.wait_until_job_complete = wait_until_job_complete
        self.job_poll_interval = job_poll_interval
        self.deferrable = deferrable

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
        if self.deferrable and not self.wait_until_job_complete:
            self.log.warning(
                "Deferrable mode is available only with parameter `wait_until_job_complete=True`. "
                "Please, set it up."
            )
        self.job_request_obj = self.build_job_request_obj(context)
        self.job = self.create_job(  # must set `self.job` for `on_kill`
            job_request_obj=self.job_request_obj
        )

        ti = context["ti"]
        ti.xcom_push(key="job_name", value=self.job.metadata.name)
        ti.xcom_push(key="job_namespace", value=self.job.metadata.namespace)

        if self.wait_until_job_complete and self.deferrable:
            self.execute_deferrable()
            return

        if self.wait_until_job_complete:
            self.job = self.hook.wait_until_job_complete(
                job_name=self.job.metadata.name,
                namespace=self.job.metadata.namespace,
                job_poll_interval=self.job_poll_interval,
            )

        ti.xcom_push(key="job", value=self.job.to_dict())
        if self.wait_until_job_complete:
            if error_message := self.hook.is_job_failed(job=self.job):
                raise AirflowException(
                    f"Kubernetes job '{self.job.metadata.name}' is failed with error '{error_message}'"
                )

    def execute_deferrable(self):
        self.defer(
            trigger=KubernetesJobTrigger(
                job_name=self.job.metadata.name,  # type: ignore[union-attr]
                job_namespace=self.job.metadata.namespace,  # type: ignore[union-attr]
                kubernetes_conn_id=self.kubernetes_conn_id,
                cluster_context=self.cluster_context,
                config_file=self.config_file,
                in_cluster=self.in_cluster,
                poll_interval=self.job_poll_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Context, event: dict, **kwargs):
        ti = context["ti"]
        ti.xcom_push(key="job", value=event["job"])
        if event["status"] == "error":
            raise AirflowException(event["message"])

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
                "job": self.hook.batch_v1_client.api_client.sanitize_for_serialization(self.job),
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


class KubernetesDeleteJobOperator(BaseOperator):
    """
    Delete a Kubernetes Job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesDeleteJobOperator`

    :param name: name of the Job.
    :param namespace: the namespace to run within kubernetes.
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used. (templated)
    :param delete_on_status: Condition for performing delete operation depending on the job status. Values:
        ``None`` - delete the job regardless of its status, "Complete" - delete only successfully completed
        jobs, "Failed" - delete only failed jobs. (default: ``None``)
    :param wait_for_completion: Whether to wait for the job to complete. (default: ``False``)
    :param poll_interval: Interval in seconds between polling the job status. Used when the `delete_on_status`
        parameter is set. (default: 10.0)
    """

    template_fields: Sequence[str] = (
        "config_file",
        "name",
        "namespace",
        "cluster_context",
    )

    def __init__(
        self,
        *,
        name: str,
        namespace: str,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        delete_on_status: str | None = None,
        wait_for_completion: bool = False,
        poll_interval: float = 10.0,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context
        self.delete_on_status = delete_on_status
        self.wait_for_completion = wait_for_completion
        self.poll_interval = poll_interval

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    @cached_property
    def client(self) -> BatchV1Api:
        return self.hook.batch_v1_client

    def execute(self, context: Context):
        try:
            if self.delete_on_status not in ("Complete", "Failed", None):
                raise AirflowException(
                    "The `delete_on_status` parameter must be one of 'Complete', 'Failed' or None. "
                    "The current value is %s",
                    str(self.delete_on_status),
                )

            if self.wait_for_completion:
                job = self.hook.wait_until_job_complete(
                    job_name=self.name, namespace=self.namespace, job_poll_interval=self.poll_interval
                )
            else:
                job = self.hook.get_job_status(job_name=self.name, namespace=self.namespace)

            if (
                self.delete_on_status is None
                or (self.delete_on_status == "Complete" and self.hook.is_job_successful(job=job))
                or (self.delete_on_status == "Failed" and self.hook.is_job_failed(job=job))
            ):
                self.log.info("Deleting kubernetes Job: %s", self.name)
                self.client.delete_namespaced_job(name=self.name, namespace=self.namespace)
                self.log.info("Kubernetes job was deleted.")
            else:
                self.log.info(
                    "Deletion of the job %s was skipped due to settings of on_status=%s",
                    self.name,
                    self.delete_on_status,
                )
        except ApiException as e:
            if e.status == 404:
                self.log.info("The Kubernetes job %s does not exist.", self.name)
            else:
                raise e


class KubernetesPatchJobOperator(BaseOperator):
    """
    Update a Kubernetes Job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:KubernetesPatchJobOperator`

    :param name: name of the Job
    :param namespace: the namespace to run within kubernetes
    :param body: Job json object with parameters for update
        https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.25/#job-v1-batch
        e.g. ``{"spec": {"suspend": True}}``
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the Kubernetes cluster.
    :param config_file: The path to the Kubernetes config file. (templated)
        If not specified, default value is ``~/.kube/config``
    :param in_cluster: run kubernetes client with in_cluster configuration.
    :param cluster_context: context that points to kubernetes cluster.
        Ignored when in_cluster is True. If None, current-context is used. (templated)
    """

    template_fields: Sequence[str] = (
        "config_file",
        "name",
        "namespace",
        "body",
        "cluster_context",
    )

    def __init__(
        self,
        *,
        name: str,
        namespace: str,
        body: object,
        kubernetes_conn_id: str | None = KubernetesHook.default_conn_name,
        config_file: str | None = None,
        in_cluster: bool | None = None,
        cluster_context: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.namespace = namespace
        self.body = body
        self.kubernetes_conn_id = kubernetes_conn_id
        self.config_file = config_file
        self.in_cluster = in_cluster
        self.cluster_context = cluster_context

    @cached_property
    def hook(self) -> KubernetesHook:
        return KubernetesHook(
            conn_id=self.kubernetes_conn_id,
            in_cluster=self.in_cluster,
            config_file=self.config_file,
            cluster_context=self.cluster_context,
        )

    def execute(self, context: Context) -> dict:
        self.log.info("Updating existing Job: %s", self.name)
        job_object = self.hook.patch_namespaced_job(
            job_name=self.name, namespace=self.namespace, body=self.body
        )
        self.log.info("Job was updated.")
        return k8s.V1Job.to_dict(job_object)
